use crate::{Result, VertxError};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

/// Process-unique identifier, shared by the handle, logs and diagnostic records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TaskId(pub(crate) u64);
impl TaskId {
    pub(crate) fn next() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}
impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "task#{}", self.0)
    }
}

/// Executors expose cancellation metadata without exposing their scheduler types.
pub(crate) trait TaskControl: Send + Sync {
    fn abort(&self);
    fn is_finished(&self) -> bool;
}
impl TaskControl for tokio::task::AbortHandle {
    fn abort(&self) {
        self.abort();
    }
    fn is_finished(&self) -> bool {
        self.is_finished()
    }
}
/// Dropping the handle detaches the waiter; it does not cancel the task.
pub struct TaskHandle<T> {
    id: TaskId,
    backend: TaskBackend<T>,
    abort: TaskAbortHandle,
}
enum TaskBackend<T> {
    Tokio(tokio::task::JoinHandle<Result<T>>),
    Channel(oneshot::Receiver<Result<T>>),
}
/// Cancellation takes effect on the owning executor after an active poll returns.
#[derive(Clone)]
pub struct TaskAbortHandle {
    control: Arc<dyn TaskControl>,
}
impl TaskAbortHandle {
    pub fn abort(&self) {
        self.control.abort();
    }
    pub fn is_finished(&self) -> bool {
        self.control.is_finished()
    }
}
impl<T> TaskHandle<T> {
    pub(crate) fn from_channel(
        receiver: oneshot::Receiver<Result<T>>,
        control: Arc<dyn TaskControl>,
    ) -> Self {
        Self {
            id: TaskId::next(),
            backend: TaskBackend::Channel(receiver),
            abort: TaskAbortHandle { control },
        }
    }
    pub(crate) fn from_tokio(handle: tokio::task::JoinHandle<Result<T>>) -> Self {
        let control = Arc::new(handle.abort_handle());
        Self {
            id: TaskId::next(),
            backend: TaskBackend::Tokio(handle),
            abort: TaskAbortHandle { control },
        }
    }
    pub(crate) fn with_id(mut self, id: TaskId) -> Self {
        self.id = id;
        self
    }
    pub fn id(&self) -> TaskId {
        self.id
    }
    pub fn abort(&self) {
        self.abort.abort();
    }
    pub fn abort_handle(&self) -> TaskAbortHandle {
        self.abort.clone()
    }
    pub fn is_finished(&self) -> bool {
        self.abort.is_finished()
    }
}
impl<T> Future for TaskHandle<T> {
    type Output = Result<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.get_mut().backend {
            TaskBackend::Tokio(handle) => Pin::new(handle)
                .poll(cx)
                .map(|r| r.unwrap_or_else(|e| Err(e.into()))),
            TaskBackend::Channel(receiver) => Pin::new(receiver).poll(cx).map(|r| {
                r.unwrap_or_else(|_| Err(VertxError::Join("task result channel closed".into())))
            }),
        }
    }
}

/// tracing's WithSubscriber restores Dispatch for poll, but not for Drop.
/// This wrapper covers both without holding an entered guard across await.
pub(crate) struct TracedFuture<F> {
    future: Option<Pin<Box<F>>>,
    dispatch: tracing::Dispatch,
    span: tracing::Span,
}
impl<F> TracedFuture<F> {
    pub fn new(future: F, dispatch: tracing::Dispatch, span: tracing::Span) -> Self {
        Self {
            future: Some(Box::pin(future)),
            dispatch,
            span,
        }
    }
}
impl<F: Future> Future for TracedFuture<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let _dispatch = tracing::dispatcher::set_default(&this.dispatch);
        let _span = this.span.enter();
        this.future
            .as_mut()
            .expect("future exists until Drop")
            .as_mut()
            .poll(cx)
    }
}
impl<F> Drop for TracedFuture<F> {
    fn drop(&mut self) {
        let _dispatch = tracing::dispatcher::set_default(&self.dispatch);
        let _span = self.span.enter();
        drop(self.future.take());
    }
}
