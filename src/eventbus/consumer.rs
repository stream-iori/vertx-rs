use super::envelope::{ErasedEnvelope, MessageTrace};
use super::queue::QueueState;
use super::route::{Registration, SubscriptionRegistration};
use crate::error::panic_error;
use crate::task::TracedFuture;
use crate::{Context, Envelope, Result, VertxError};
use futures::stream::FusedStream;
use futures::{FutureExt, Stream, StreamExt};
use std::future::Future;
use std::marker::PhantomData;
use std::ops::AsyncFnMut;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use tokio::sync::mpsc;

/// A pull consumer. Drop unregisters; scope cancellation stops receiving even
/// when messages remain buffered. Handler tasks are tracked by the owner scope.
pub struct Consumer<T> {
    rx: mpsc::Receiver<ErasedEnvelope>,
    registration: SubscriptionRegistration,
    owner: Option<Context>,
    queue: Arc<QueueState>,
    cancellation: Pin<Box<tokio_util::sync::WaitForCancellationFutureOwned>>,
    terminal: bool,
    _marker: PhantomData<T>,
}
impl<T: Send + 'static> Consumer<T> {
    pub(super) fn new(registration: Registration, owner: Option<Context>) -> Self {
        let Registration {
            rx,
            subscription,
            state: queue,
        } = registration;
        Self {
            rx,
            registration: subscription,
            owner,
            cancellation: queue.cancellation(),
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
    /// Returns the address this consumer was registered for.
    pub fn address(&self) -> &str {
        self.registration.address()
    }
    fn handler_context(&self) -> Result<Context> {
        let ctx = self.owner.clone().ok_or(VertxError::ContextMismatch)?;
        if !Context::current().is_some_and(|c| ctx.same(&c)) {
            return Err(VertxError::ContextMismatch);
        }
        ctx.ensure_accepting_work()?;
        Ok(ctx)
    }
    /// Attach an async handler on its owning EventLoop. Messages are processed
    /// one at a time, so the handler can borrow its captured state across await.
    ///
    /// ```
    /// # use vertx_rs::{Context, Result, Verticle};
    /// # struct Example;
    /// # impl Verticle for Example {
    /// # async fn start(&mut self, ctx: Context) -> Result<()> {
    /// let mut count = 0u64;
    /// ctx.event_bus().consumer::<()>("count")?.handler(async move |env| {
    ///     let count = &mut count;
    ///     tokio::task::yield_now().await;
    ///     *count += 1;
    ///     env.reply(*count)
    /// })?;
    /// # Ok(()) } }
    /// ```
    pub fn handler(
        mut self,
        mut handler: impl AsyncFnMut(Envelope<T>) -> Result<()> + 'static,
    ) -> Result<()> {
        let ctx = self.handler_context()?;
        ctx.ensure_current_event_loop_context()?;
        ctx.clone().spawn_local(async move {
            while let Some(env) = self.next_for_handler(&ctx).await {
                dispatch_message(&ctx, env.trace.clone(), async { handler(env).await }).await;
            }
            Ok(())
        })?;
        Ok(())
    }
    /// Worker counterpart; the closure and its future must be Send.
    ///
    /// ```
    /// use vertx_rs::{Context, Result, WorkerVerticle};
    /// struct Worker;
    /// impl WorkerVerticle for Worker {
    ///     async fn start(&mut self, ctx: Context) -> Result<()> {
    ///         ctx.event_bus().consumer::<u32>("worker")?.handler_worker(|env| async move {
    ///             let answer = *env.body() * 2;
    ///             env.reply(answer)
    ///         })
    ///     }
    /// }
    /// ```
    ///
    /// ```compile_fail,E0277
    /// use vertx_rs::{Consumer, Result};
    ///
    /// fn register(consumer: Consumer<()>) -> Result<()> {
    ///     consumer.handler_worker(|env| async move {
    ///         let value = std::rc::Rc::new(1u32);
    ///         tokio::task::yield_now().await;
    ///         env.reply(*value)
    ///     })
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
        ctx.clone().spawn(async move {
            while let Some(env) = self.next_for_handler(&ctx).await {
                // Defer the closure call to the traced poll as it can panic.
                dispatch_message(&ctx, env.trace.clone(), async { handler(env).await }).await;
            }
            Ok(())
        })?;
        Ok(())
    }
}

async fn dispatch_message(
    ctx: &Context,
    trace: MessageTrace,
    future: impl Future<Output = Result<()>>,
) {
    let span = trace.handler_span(ctx);
    let result = AssertUnwindSafe(TracedFuture::new(
        future,
        trace.dispatch.clone(),
        span.clone(),
    ))
    .catch_unwind()
    .await
    .unwrap_or_else(|error| Err(panic_error(error)));
    if let Err(error) = result {
        tracing::dispatcher::with_default(&trace.dispatch, || {
            span.in_scope(|| ctx.report_error(&error))
        });
    }
}
// T is only a type marker; no pinned T lives in this struct. The cancellation
// future remains pinned in its Box even when the Consumer moves.
impl<T> Unpin for Consumer<T> {}
impl<T> Consumer<T> {
    fn unregister(&self) {
        self.registration.unregister();
        if let Some(ctx) = &self.owner {
            ctx.scope().forget_resource(self.registration.id);
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
                .map(|ctx| VertxError::DeploymentTerminated(ctx.scope().deployment))
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
                let result = envelope.typed(this.registration.address());
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
    #[rstest::rstest]
    #[case::empty(0)]
    #[case::buffered(2)]
    #[tokio::test]
    async fn stream_eof_is_fused_and_does_not_unregister_other_consumers(#[case] buffered: usize) {
        let vx = crate::Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(1)
            .build()
            .unwrap();
        let mut consumer = vx.event_bus().consumer::<()>("eof").unwrap();
        for _ in 0..buffered {
            vx.event_bus().send("eof", ()).await.unwrap();
        }
        let mut other = vx.event_bus().consumer::<()>("eof").unwrap();
        consumer.rx.close();
        for _ in 0..buffered {
            assert!(consumer.next().await.unwrap().is_ok());
        }
        assert!(consumer.next().await.is_none());
        assert!(consumer.next().await.is_none());
        assert!(consumer.is_terminated());
        vx.event_bus().send("eof", ()).await.unwrap();
        assert!(other.recv().await.unwrap().is_some());
        vx.close().await.unwrap();
    }
}
