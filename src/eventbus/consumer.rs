use super::envelope::ErasedEnvelope;
use super::inner::{EventBusInner, QueueState, SubscriberId};
use crate::{Context, Envelope, Result, VertxError};
use futures::{stream::FusedStream, Stream, StreamExt};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use tokio::sync::mpsc;

/// A pull consumer. Drop unregisters; scope cancellation stops receiving even
/// when messages remain buffered. Handler tasks are tracked by the owner scope.
pub struct Consumer<T> {
    rx: mpsc::Receiver<ErasedEnvelope>,
    address: String,
    id: SubscriberId,
    bus: Arc<EventBusInner>,
    owner: Option<Context>,
    queue: Arc<QueueState>,
    cancellation: Pin<Box<tokio_util::sync::WaitForCancellationFutureOwned>>,
    terminal: bool,
    _marker: PhantomData<T>,
}
impl<T: Send + 'static> Consumer<T> {
    pub(crate) fn new(
        rx: mpsc::Receiver<ErasedEnvelope>,
        address: String,
        id: SubscriberId,
        bus: Arc<EventBusInner>,
        owner: Option<Context>,
        queue: Arc<QueueState>,
    ) -> Self {
        Self {
            rx,
            address,
            id,
            bus,
            owner,
            cancellation: Box::pin(queue.cancel.clone().cancelled_owned()),
            terminal: false,
            queue,
            _marker: PhantomData,
        }
    }
    /// Pull one message. EOF is None; cancellation and conversion failures are
    /// errors, delivered once. After a terminal result the stream stays ended.
    pub async fn recv(&mut self) -> Result<Option<Envelope<T>>> {
        self.next().await.transpose()
    }
    async fn next_for_handler(&mut self, ctx: &Context) -> Option<Envelope<T>> {
        match self.recv().await {
            Ok(message) => message,
            Err(error) => {
                if !ctx.is_expected_cancellation(&error) {
                    ctx.report_error(&error);
                }
                None
            }
        }
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
            while let Some(env) = self.next_for_handler(&err_ctx).await {
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
            while let Some(env) = self.next_for_handler(&err_ctx).await {
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
// T is only a type marker; no pinned T lives in this struct. The cancellation
// future remains pinned in its Box even when the Consumer moves.
impl<T> Unpin for Consumer<T> {}
impl<T> Consumer<T> {
    fn unregister(&self) {
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
    fn terminate(&mut self) {
        self.terminal = true;
        self.rx.close();
        self.unregister();
    }
}
impl<T: Send + 'static> Stream for Consumer<T> {
    type Item = Result<Envelope<T>>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.terminal {
            return Poll::Ready(None);
        }
        if this.cancellation.as_mut().poll(cx).is_ready() {
            let error = this
                .owner
                .as_ref()
                .map(|ctx| VertxError::DeploymentTerminated(ctx.scope.deployment))
                .unwrap_or(VertxError::VertxClosed);
            this.terminate();
            return Poll::Ready(Some(Err(error)));
        }
        match this.rx.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                this.terminate();
                Poll::Ready(None)
            }
            Poll::Ready(Some(envelope)) => {
                this.queue.dequeued();
                envelope.trace.dequeue();
                let result = envelope.typed(&this.address);
                if result.is_err() {
                    this.terminate();
                }
                Poll::Ready(Some(result))
            }
        }
    }
}
impl<T: Send + 'static> FusedStream for Consumer<T> {
    fn is_terminated(&self) -> bool {
        self.terminal
    }
}
impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        self.unregister();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn stream_eof_is_fused_and_does_not_unregister_other_consumers() {
        let vx = crate::Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(1)
            .build();
        let mut consumer = vx.event_bus().consumer::<()>("eof").unwrap();
        let mut other = vx.event_bus().consumer::<()>("eof").unwrap();
        consumer.rx.close();
        assert!(consumer.next().await.is_none());
        assert!(consumer.next().await.is_none());
        assert!(consumer.is_terminated());
        vx.event_bus().send("eof", ()).await.unwrap();
        assert!(other.recv().await.unwrap().is_some());
        vx.close().await.unwrap();
    }
}
