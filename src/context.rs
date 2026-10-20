use crate::deployment::{DeploymentScope, Phase};
use crate::error::panic_error;
use crate::eventloop::{EventLoopId, EventLoopPool};
use crate::vertx::{Vertx, VertxInner};
use crate::worker_pool::WorkerPool;
use crate::TaskHandle;
use crate::{DeploymentId, EventBus, Result, ThreadingModel, VertxError};
use futures::FutureExt;
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, Weak};
use std::task::{Context as TaskContext, Poll};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ThreadIdentity {
    EventLoop(u64, EventLoopId),
    Worker(u64),
}
thread_local! {
    static CURRENT: RefCell<Option<Context>> = const { RefCell::new(None) };
    static THREAD: Cell<Option<ThreadIdentity>> = const { Cell::new(None) };
}
pub(crate) fn set_thread(identity: Option<ThreadIdentity>) {
    THREAD.with(|t| t.set(identity));
}
pub(crate) fn thread_identity() -> Option<ThreadIdentity> {
    THREAD.with(Cell::get)
}
pub(crate) type ExceptionHandler = Arc<dyn Fn(&VertxError) + Send + Sync>;
pub(crate) struct ContextShared {
    pub event_bus: EventBus,
    pub event_loops: Arc<EventLoopPool>,
    pub workers: Arc<WorkerPool>,
    pub vertx: Weak<VertxInner>,
    pub exception_handler: RwLock<Option<ExceptionHandler>>,
    pub cancel: CancellationToken,
    pub blocking: TaskTracker,
    pub shutdown_grace: std::time::Duration,
}
struct ContextData {
    id: u64,
    driver: tokio::runtime::Handle,
    execution: std::sync::Mutex<()>,
    ordered: Arc<Mutex<()>>,
    exception_handler: RwLock<Option<ExceptionHandler>>,
}
#[derive(Clone, Copy)]
enum Kind {
    EventLoop(EventLoopId),
    Worker,
}
/// Logical execution context. Its kind is independent of the physical thread
/// used by `execute_blocking`. Framework tasks restore it for each poll.
#[derive(Clone)]
pub struct Context {
    kind: Kind,
    pub(crate) inner: Arc<ContextShared>,
    pub(crate) scope: Arc<DeploymentScope>,
    data: Arc<ContextData>,
    stopping: bool,
}
static NEXT_CONTEXT: AtomicU64 = AtomicU64::new(1);
impl Context {
    fn new(kind: Kind, inner: Arc<ContextShared>, scope: Arc<DeploymentScope>) -> Self {
        let driver = inner.event_loops.driver(match kind {
            Kind::EventLoop(id) => id,
            Kind::Worker => inner.event_loops.pick(),
        });
        Self {
            kind,
            inner,
            scope,
            data: Arc::new(ContextData {
                id: NEXT_CONTEXT.fetch_add(1, Ordering::Relaxed),
                driver,
                execution: std::sync::Mutex::new(()),
                ordered: Arc::new(Mutex::new(())),
                exception_handler: RwLock::new(None),
            }),
            stopping: false,
        }
    }
    pub(crate) fn new_event_loop(
        id: EventLoopId,
        inner: Arc<ContextShared>,
        scope: Arc<DeploymentScope>,
    ) -> Self {
        Self::new(Kind::EventLoop(id), inner, scope)
    }
    pub(crate) fn new_worker(inner: Arc<ContextShared>, scope: Arc<DeploymentScope>) -> Self {
        Self::new(Kind::Worker, inner, scope)
    }
    pub(crate) fn for_stop(&self) -> Self {
        let mut c = self.clone();
        c.stopping = true;
        c
    }
    pub fn is_event_loop(&self) -> bool {
        matches!(self.kind, Kind::EventLoop(_))
    }
    pub fn is_worker(&self) -> bool {
        matches!(self.kind, Kind::Worker)
    }
    pub fn threading_model(&self) -> ThreadingModel {
        if self.is_event_loop() {
            ThreadingModel::EventLoop
        } else {
            ThreadingModel::Worker
        }
    }
    pub fn event_loop_id(&self) -> Option<EventLoopId> {
        match self.kind {
            Kind::EventLoop(id) => Some(id),
            _ => None,
        }
    }
    pub fn deployment_id(&self) -> Option<DeploymentId> {
        Some(self.scope.deployment)
    }
    pub fn owner(&self) -> Option<Vertx> {
        self.inner.vertx.upgrade().map(Vertx::from_inner)
    }
    pub fn current() -> Option<Context> {
        CURRENT.with(|c| c.borrow().clone())
    }
    pub fn is_on_event_loop_thread() -> bool {
        matches!(thread_identity(), Some(ThreadIdentity::EventLoop(..)))
    }
    pub fn is_on_worker_thread() -> bool {
        matches!(thread_identity(), Some(ThreadIdentity::Worker(_)))
    }
    pub fn is_on_vertx_thread() -> bool {
        thread_identity().is_some()
    }
    pub fn is_alive(&self) -> bool {
        !self.inner.cancel.is_cancelled() && self.scope.is_alive()
    }
    pub fn cancellation_token(&self) -> CancellationToken {
        self.scope.cancel.clone()
    }
    pub fn event_bus(&self) -> EventBus {
        self.inner.event_bus.bound(self.clone())
    }
    pub(crate) fn same(&self, other: &Context) -> bool {
        Arc::ptr_eq(&self.data, &other.data)
    }
    pub(crate) fn ensure_alive(&self) -> Result<()> {
        if self.inner.cancel.is_cancelled() {
            return Err(VertxError::VertxClosed);
        }
        drop(self.scope.admit()?);
        Ok(())
    }
    pub(crate) fn check_local(&self) -> Result<()> {
        self.ensure_alive()?;
        if let Kind::EventLoop(id) = self.kind {
            if self.inner.event_loops.check_thread(id)
                && Self::current().is_some_and(|c| self.same(&c))
            {
                return Ok(());
            }
        }
        Err(VertxError::ContextMismatch)
    }
    pub fn set_exception_handler<F>(&self, f: F)
    where
        F: Fn(&VertxError) + Send + Sync + 'static,
    {
        *self.data.exception_handler.write().unwrap() = Some(Arc::new(f));
    }
    pub fn clear_exception_handler(&self) {
        self.data.exception_handler.write().unwrap().take();
    }
    pub(crate) fn report_error(&self, err: &VertxError) {
        let handler = self
            .data
            .exception_handler
            .read()
            .unwrap()
            .clone()
            .or_else(|| self.inner.exception_handler.read().unwrap().clone());
        if let Some(h) = handler {
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| h(err))).is_err() {
                tracing::error!("exception handler panicked");
            }
        } else {
            tracing::error!(deployment = %self.scope.deployment, context = self.data.id, error = %err, "unhandled error");
        }
    }
    fn enter(&self) -> ContextGuard {
        let prev = CURRENT.with(|c| c.replace(Some(self.clone())));
        ContextGuard {
            prev,
            _not_send: PhantomData,
        }
    }
    pub(crate) fn wrap<F: Future>(&self, future: F) -> ContextFuture<F> {
        ContextFuture {
            ctx: self.clone(),
            future: Some(Box::pin(future)),
        }
    }
    pub(crate) fn scope_future<F: Future>(&self, future: F) -> impl Future<Output = F::Output> {
        let ctx = self.clone();
        let mut future = Box::pin(future);
        futures::future::poll_fn(move |cx| {
            let _context = ctx.enter();
            future.as_mut().poll(cx)
        })
    }
    fn managed<F>(&self, future: F) -> impl Future<Output = Result<()>>
    where
        F: Future<Output = Result<()>>,
    {
        let ctx = self.clone();
        async move {
            if ctx.scope.cancel.is_cancelled() {
                return Err(VertxError::DeploymentTerminated(ctx.scope.deployment));
            }
            let r = std::panic::AssertUnwindSafe(future)
                .catch_unwind()
                .await
                .unwrap_or_else(|e| Err(panic_error(e)));
            if let Err(e) = &r {
                ctx.report_error(e);
            }
            r
        }
    }
    /// Spawn a managed non-Send task. Must be called in this exact Context.
    /// Dropping the returned TaskHandle detaches the caller, not the scope.
    pub fn spawn_local<F>(&self, future: F) -> Result<TaskHandle<()>>
    where
        F: Future<Output = Result<()>> + 'static,
    {
        self.check_local()?;
        let mut state = self.scope.admit()?;
        let future = Tracked::new(self.wrap(self.managed(future)), self.scope.task_token());
        let handle = TaskHandle::from_tokio(tokio::task::spawn_local(future));
        state.tasks.retain(|h| !h.is_finished());
        state.tasks.push(handle.abort_handle());
        Ok(handle)
    }
    /// Dispatch a Send future to this Context. EventLoop targets remain pinned.
    pub fn spawn<F>(&self, future: F) -> Result<TaskHandle<()>>
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        self.ensure_alive()?;
        let mut state = self.scope.admit()?;
        let task = Tracked::new(self.wrap(self.managed(future)), self.scope.task_token());
        let handle = match self.kind {
            Kind::EventLoop(id) => {
                TaskHandle::from_tokio(self.inner.event_loops.spawn_pinned(id, move || task)?)
            }
            Kind::Worker => {
                self.inner
                    .workers
                    .spawn(self.data.id, self.data.driver.clone(), task)?
            }
        };
        state.tasks.retain(|h| !h.is_finished());
        state.tasks.push(handle.abort_handle());
        Ok(handle)
    }
    /// Queue a Future on this Context. Execution never starts inline.
    pub fn run_on_context<F>(&self, future: F) -> Result<TaskHandle<()>>
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        self.spawn(future)
    }
    pub(crate) fn worker_lane(&self) -> u64 {
        self.data.id
    }
    pub(crate) fn worker_driver(&self) -> tokio::runtime::Handle {
        self.data.driver.clone()
    }
    pub fn request_undeploy(&self) -> Result<()> {
        self.ensure_alive()?;
        self.scope.request_stop();
        Ok(())
    }
    pub async fn execute_blocking<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.execute_blocking_with(f, true).await
    }
    pub async fn execute_blocking_with<F, R>(&self, f: F, ordered: bool) -> Result<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        // Reserve tracking before the first await, including queued work.
        let token = {
            let state = self.scope.state.lock().unwrap();
            let allowed = matches!(state.phase, Phase::Starting | Phase::Running)
                && !self.inner.cancel.is_cancelled()
                || self.stopping
                    && state.phase == Phase::Stopping
                    && Self::current().is_some_and(|c| c.stopping && c.same(self));
            if !allowed {
                return Err(VertxError::DeploymentTerminated(self.scope.deployment));
            }
            self.scope.blocking.token()
        };
        let global_token = self.inner.blocking.token();
        let ordered_lock = if ordered {
            Some(tokio::select! { biased;
                _ = self.scope.cancel.cancelled(), if !self.stopping => return Err(VertxError::DeploymentTerminated(self.scope.deployment)),
                g = self.data.ordered.clone().lock_owned() => g,
            })
        } else {
            None
        };
        let ctx = self.clone();
        let handle = self.inner.workers.spawn_blocking(move || {
            let (_token, _global, _ordered) = (token, global_token, ordered_lock);
            let _context = ctx.enter();
            // Move captures into a local so cancellation drops them before the
            // Context guard and tracking tokens, rather than with the FnOnce environment.
            let operation = f;
            if !ctx.stopping && ctx.scope.cancel.is_cancelled() {
                drop(operation);
                return Err(VertxError::DeploymentTerminated(ctx.scope.deployment));
            }
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation)).map_err(panic_error)
        })?;
        handle
            .await
            .map_err(|_| VertxError::Join("blocking result channel closed".into()))?
    }
    pub fn timer(&self, delay: std::time::Duration) -> Result<crate::Timer> {
        let _state = self.scope.admit()?;
        self.inner.event_bus.ensure_open()?;
        Ok(crate::Timer::new(delay, self.scope.cancel.clone()))
    }
    pub fn periodic(
        &self,
        init: std::time::Duration,
        period: std::time::Duration,
    ) -> Result<crate::Periodic> {
        let _state = self.scope.admit()?;
        self.inner.event_bus.ensure_open()?;
        crate::Periodic::new(init, period, self.scope.cancel.clone())
    }
}
struct ContextGuard {
    prev: Option<Context>,
    _not_send: PhantomData<Rc<()>>,
}
impl Drop for ContextGuard {
    fn drop(&mut self) {
        CURRENT.with(|c| *c.borrow_mut() = self.prev.take());
    }
}
pub(crate) struct ContextFuture<F> {
    ctx: Context,
    future: Option<Pin<Box<F>>>,
}
impl<F: Future> Future for ContextFuture<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.ctx.is_event_loop() {
            assert!(
                this.ctx
                    .inner
                    .event_loops
                    .check_thread(this.ctx.event_loop_id().unwrap()),
                "EventLoop affinity violated"
            );
        }
        let _execution = this
            .ctx
            .data
            .execution
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let _context = this.ctx.enter();
        tracing::trace!(deployment = %this.ctx.scope.deployment, context = this.ctx.data.id,
            thread = ?std::thread::current().id(), event_loop = ?this.ctx.event_loop_id(), "context poll");
        this.future
            .as_mut()
            .expect("future already dropped")
            .as_mut()
            .poll(cx)
    }
}
impl<F> Drop for ContextFuture<F> {
    fn drop(&mut self) {
        let _execution = self
            .ctx
            .data
            .execution
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let _context = self.ctx.enter();
        // Drop non-Send user state on its owning executor with its Context.
        drop(self.future.take());
    }
}

// Tracking ends only after user future destructors finish, including aborts
// before the first poll. The completion token must be dropped last.
struct Tracked<F> {
    future: Option<Pin<Box<F>>>,
    token: Option<tokio_util::task::task_tracker::TaskTrackerToken>,
}
impl<F> Tracked<F> {
    fn new(future: F, token: tokio_util::task::task_tracker::TaskTrackerToken) -> Self {
        Self {
            future: Some(Box::pin(future)),
            token: Some(token),
        }
    }
}
impl<F: Future> Future for Tracked<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        self.get_mut().future.as_mut().unwrap().as_mut().poll(cx)
    }
}
impl<F> Drop for Tracked<F> {
    fn drop(&mut self) {
        drop(self.future.take());
        drop(self.token.take());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn context_restored_when_send_future_moves_between_os_threads() {
        let vx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(2)
            .build();
        let scope = Arc::new(DeploymentScope::new(
            DeploymentId::next(),
            vx.inner.shared.cancel.child_token(),
        ));
        let ctx = Context::new_worker(vx.inner.shared.clone(), scope);
        let id = ctx.deployment_id();
        let mut polls = 0;
        let mut future = Box::pin(ctx.wrap(futures::future::poll_fn(move |_| {
            assert_eq!(Context::current().unwrap().deployment_id(), id);
            polls += 1;
            if polls == 2 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })));
        let mut threads = Vec::new();
        for n in 0..2 {
            future = std::thread::spawn(move || {
                assert!(Context::current().is_none());
                let waker = futures::task::noop_waker();
                let mut cx = TaskContext::from_waker(&waker);
                assert_eq!(future.as_mut().poll(&mut cx).is_ready(), n == 1);
                assert!(Context::current().is_none());
                (future, std::thread::current().id())
            })
            .join()
            .map(|(f, t)| {
                threads.push(t);
                f
            })
            .unwrap();
        }
        assert_ne!(threads[0], threads[1]);
        drop(future);
        assert!(Context::current().is_none());
        vx.close().await.unwrap();
    }
    #[tokio::test]
    async fn guard_restores_nested_context_and_panic() {
        let vx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(2)
            .build();
        let make = || {
            Context::new_worker(
                vx.inner.shared.clone(),
                Arc::new(DeploymentScope::new(
                    DeploymentId::next(),
                    CancellationToken::new(),
                )),
            )
        };
        let (a, b) = (make(), make());
        {
            let _a = a.enter();
            assert!(Context::current().unwrap().same(&a));
            let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _b = b.enter();
                panic!("guard test");
            }));
            assert!(r.is_err());
            assert!(Context::current().unwrap().same(&a));
        }
        assert!(Context::current().is_none());
        vx.close().await.unwrap();
    }
    #[tokio::test]
    async fn registration_and_stop_share_one_admission_transaction() {
        let vx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(2)
            .build();
        for _ in 0..50 {
            let scope = Arc::new(DeploymentScope::new(
                DeploymentId::next(),
                vx.inner.shared.cancel.child_token(),
            ));
            let ctx = Context::new_worker(vx.inner.shared.clone(), scope.clone());
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let c = ctx.clone();
            let b = barrier.clone();
            let register = std::thread::spawn(move || {
                b.wait();
                c.event_bus().consumer::<()>("race")
            });
            let c = ctx.clone();
            let b = barrier.clone();
            let stop = std::thread::spawn(move || {
                b.wait();
                c.scope.request_stop();
                c.inner.event_bus.cleanup_scope(&c.scope);
            });
            let result = register.join().unwrap();
            stop.join().unwrap();
            match result {
                Ok(mut consumer) => assert!(consumer.recv().await.is_none()),
                Err(crate::VertxError::DeploymentTerminated(_)) => {}
                _ => panic!("unexpected registration outcome"),
            }
            assert!(matches!(
                vx.event_bus().try_send("race", ()),
                Err(crate::VertxError::NoConsumer(_))
            ));
            assert!(scope.state.lock().unwrap().subscribers.is_empty());
        }
        vx.close().await.unwrap();
    }
}
