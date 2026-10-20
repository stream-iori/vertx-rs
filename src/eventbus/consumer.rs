use super::envelope::ErasedEnvelope;
use super::inner::{EventBusInner, SubscriberId};
use crate::{Context, Envelope, Result, VertxError};
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// A pull consumer. Drop unregisters; scope cancellation stops receiving even
/// when messages remain buffered. Handler tasks are tracked by the owner scope.
pub struct Consumer<T> {
    rx: mpsc::Receiver<ErasedEnvelope>,
    address: String,
    id: SubscriberId,
    bus: Arc<EventBusInner>,
    owner: Option<Context>,
    cancel: CancellationToken,
    _marker: PhantomData<T>,
}
impl<T: Send + 'static> Consumer<T> {
    pub(crate) fn new(
        rx: mpsc::Receiver<ErasedEnvelope>,
        address: String,
        id: SubscriberId,
        bus: Arc<EventBusInner>,
        owner: Option<Context>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            rx,
            address,
            id,
            bus,
            owner,
            cancel,
            _marker: PhantomData,
        }
    }
    pub async fn recv(&mut self) -> Option<Envelope<T>> {
        let env = tokio::select! { biased;
            _ = self.cancel.cancelled() => return None,
            r = self.rx.recv() => r?,
        };
        env.trace.dequeue();
        env.typed(&self.address).ok()
    }
    pub fn address(&self) -> &str {
        &self.address
    }
    pub(crate) fn subscriber_id(&self) -> SubscriberId {
        self.id
    }
    fn handler_context(&self) -> Result<Context> {
        let ctx = self.owner.clone().ok_or(VertxError::ContextMismatch)?;
        if !Context::current().is_some_and(|c| ctx.same(&c)) {
            return Err(VertxError::ContextMismatch);
        }
        ctx.ensure_alive()?;
        Ok(ctx)
    }
    /// Attach a non-Send handler on its owning EventLoop.
    ///
    /// ```
    /// # use async_trait::async_trait;
    /// # use vertx_rs::{Context, Result, Verticle};
    /// # struct Example;
    /// # #[async_trait(?Send)] impl Verticle for Example {
    /// # async fn start(&mut self, ctx: Context) -> Result<()> {
    /// let mut count = 0u64;
    /// ctx.event_bus().consumer::<()>("count")?.handler(move |env| {
    ///     count += 1;
    ///     async move { env.reply(count) }
    /// })?;
    /// # Ok(()) } }
    /// ```
    pub fn handler<F, Fut>(mut self, mut handler: F) -> Result<()>
    where
        F: FnMut(Envelope<T>) -> Fut + 'static,
        Fut: Future<Output = Result<()>> + 'static,
    {
        let ctx = self.handler_context()?;
        ctx.check_local()?;
        let err_ctx = ctx.clone();
        ctx.spawn_local(async move {
            while let Some(env) = self.recv().await {
                // Catch each message separately so one panic does not kill the consumer.
                let trace = env.trace.clone();
                let handler_span = trace.handler_span(&err_ctx);
                let result = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
                    crate::task::TracedFuture::new(
                        async { handler(env).await },
                        trace.dispatch.clone(),
                        handler_span.clone(),
                    ),
                ))
                .await
                .unwrap_or_else(|e| Err(crate::error::panic_error(e)));
                if let Err(e) = result {
                    tracing::dispatcher::with_default(&trace.dispatch, || {
                        handler_span.in_scope(|| err_ctx.report_error(&e))
                    });
                }
            }
            Ok(())
        })?;
        Ok(())
    }
    /// Worker counterpart; the closure and its future must be Send.
    ///
    /// ```
    /// use async_trait::async_trait;
    /// use vertx_rs::{Context, Result, WorkerVerticle};
    /// struct Worker;
    /// #[async_trait]
    /// impl WorkerVerticle for Worker {
    ///     async fn start(&mut self, ctx: Context) -> Result<()> {
    ///         ctx.event_bus().consumer::<u32>("worker")?.handler_worker(|env| async move {
    ///             let answer = *env.body() * 2;
    ///             env.reply(answer)
    ///         })
    ///     }
    /// }
    /// ```
    pub fn handler_worker<F, Fut>(mut self, mut handler: F) -> Result<()>
    where
        F: FnMut(Envelope<T>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let ctx = self.handler_context()?;
        if !ctx.is_worker() {
            return Err(VertxError::ContextMismatch);
        }
        let err_ctx = ctx.clone();
        ctx.spawn(async move {
            while let Some(env) = self.recv().await {
                let trace = env.trace.clone();
                let handler_span = trace.handler_span(&err_ctx);
                let result = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
                    crate::task::TracedFuture::new(
                        async { handler(env).await },
                        trace.dispatch.clone(),
                        handler_span.clone(),
                    ),
                ))
                .await
                .unwrap_or_else(|e| Err(crate::error::panic_error(e)));
                if let Err(e) = result {
                    tracing::dispatcher::with_default(&trace.dispatch, || {
                        handler_span.in_scope(|| err_ctx.report_error(&e))
                    });
                }
            }
            Ok(())
        })?;
        Ok(())
    }
}
impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        self.bus.unregister(&self.address, self.id);
        if let Some(ctx) = &self.owner {
            ctx.scope
                .state
                .lock()
                .unwrap()
                .subscribers
                .retain(|(_, id)| *id != self.id);
        }
    }
}
