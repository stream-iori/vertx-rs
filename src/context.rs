mod blocking;
mod future;

use crate::TaskHandle;
use crate::deployment::DeploymentScope;
use crate::diagnostics::Diagnostics;
use crate::eventloop::{EventLoopId, EventLoopPool};
use crate::vertx::{Vertx, VertxInner};
use crate::worker_pool::WorkerPool;
use crate::{DeploymentId, EventBus, Result, ThreadingModel, VertxError};
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, Weak};
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
    pub diagnostics: Arc<Diagnostics>,
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
/// Stable process-unique logical execution identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContextId(pub(crate) u64);
impl std::fmt::Display for ContextId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "context#{}", self.0)
    }
}
static NEXT_CONTEXT: AtomicU64 = AtomicU64::new(1);
impl Context {
    pub fn id(&self) -> ContextId {
        ContextId(self.data.id)
    }
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
    /// Reports the logical kind, even inside a blocking closure running on a Worker.
    /// Use [`Self::is_on_event_loop_thread`] to inspect the physical thread.
    pub fn is_event_loop(&self) -> bool {
        matches!(self.kind, Kind::EventLoop(_))
    }
    /// Reports the logical kind, independent of the current physical thread.
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
    /// Returns the logical Context for this synchronous poll or blocking operation.
    /// Outside managed execution, including raw Tokio tasks, returns `None`.
    pub fn current() -> Option<Context> {
        CURRENT.with(|c| c.borrow().clone())
    }
    /// Reports the physical thread kind independently of the logical Context.
    pub fn is_on_event_loop_thread() -> bool {
        matches!(thread_identity(), Some(ThreadIdentity::EventLoop(..)))
    }
    /// Reports whether this OS thread belongs to a framework Worker pool.
    pub fn is_on_worker_thread() -> bool {
        matches!(thread_identity(), Some(ThreadIdentity::Worker(_)))
    }
    pub fn is_on_vertx_thread() -> bool {
        thread_identity().is_some()
    }
    pub fn is_alive(&self) -> bool {
        self.ensure_alive().is_ok()
    }
    /// Returns the deployment cancellation token. Cancelling it requests shutdown;
    /// it cannot interrupt a running synchronous poll or blocking closure.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.scope.cancel.clone()
    }
    /// Returns a bus whose consumers are bound to this Context and deployment.
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
        if let Kind::EventLoop(id) = self.kind
            && self.inner.event_loops.check_thread(id)
            && Self::current().is_some_and(|c| self.same(&c))
        {
            return Ok(());
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
        let local = self.data.exception_handler.read().unwrap().clone();
        let handler = local.or_else(|| self.inner.exception_handler.read().unwrap().clone());
        let instance_id = self.inner.diagnostics.instance;
        let deployment_id = self.scope.deployment;
        let context_id = self.id();
        if self.inner.diagnostics.options.enabled {
            tracing::error!(
                instance_id, deployment_id = %deployment_id, context_id = %context_id,
                error_kind = err.kind(), error = %err, "framework operation failed"
            );
        }
        if let Some(h) = handler
            && std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| h(err))).is_err()
        {
            tracing::error!(
                instance_id, deployment_id = %deployment_id, context_id = %context_id,
                error_kind = "panic", "exception handler panicked"
            );
        }
    }
    fn enter(&self) -> ContextGuard {
        let prev = CURRENT.with(|c| c.replace(Some(self.clone())));
        ContextGuard {
            prev,
            _not_send: PhantomData,
        }
    }
    pub(crate) fn is_expected_cancellation(&self, error: &VertxError) -> bool {
        matches!(error, VertxError::TaskCancelled)
            || matches!(error, VertxError::DeploymentTerminated(id) if *id == self.scope.deployment && self.scope.cancel.is_cancelled())
            || matches!(error, VertxError::VertxClosed if self.inner.cancel.is_cancelled())
    }
    /// Spawn a managed non-Send task. Must be called in this exact Context.
    /// Dropping the returned TaskHandle detaches the caller, not the scope.
    ///
    /// Requires this exact live Context and its assigned EventLoop; otherwise
    /// returns a lifecycle error or `ContextMismatch`. Abort is executor-side;
    /// completion is published only after user state destruction.
    pub fn spawn_local<F>(&self, future: F) -> Result<TaskHandle<()>>
    where
        F: Future<Output = Result<()>> + 'static,
    {
        self.check_local()?;
        let mut admission = self.scope.admit()?;
        let (future, id) = self.track_future(future);
        let handle = TaskHandle::from_tokio(tokio::task::spawn_local(future)).with_id(id);
        admission.track_task(handle.abort_handle());
        Ok(handle)
    }
    /// Dispatch a Send future to this Context. EventLoop targets remain pinned.
    ///
    /// May be called from outside the Context. Rejects closed scopes/instances.
    /// Dropping the handle detaches the waiter; abort requests executor-side
    /// cancellation and cannot interrupt synchronous code.
    pub fn spawn<F>(&self, future: F) -> Result<TaskHandle<()>>
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        self.ensure_alive()?;
        let mut admission = self.scope.admit()?;
        let (task, id) = self.track_future(future);
        let handle = match self.kind {
            Kind::EventLoop(id) => self.inner.event_loops.spawn_pinned(id, move || task)?,
            Kind::Worker => {
                self.inner
                    .workers
                    .spawn(self.worker_lane(), self.worker_driver(), task)?
            }
        }
        .with_id(id);
        admission.track_task(handle.abort_handle());
        Ok(handle)
    }
    /// Queue a Future on this Context. Execution never starts inline.
    ///
    /// Uses the same admission and cancellation rules as [`Self::spawn`].
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
    /// Requests this deployment to stop without waiting for its own tasks.
    /// Use this inside the deployment instead of awaiting its own undeploy.
    /// A scope that no longer admits work returns `DeploymentTerminated`.
    pub fn request_undeploy(&self) -> Result<()> {
        self.ensure_alive()?;
        self.scope.request_stop();
        Ok(())
    }
    /// Creates a timer starting now, cancelled with this deployment.
    /// Rejects stopped scopes or closed instances. Polling needs a Tokio time driver;
    /// an unrepresentable deadline may panic.
    pub fn timer(&self, delay: std::time::Duration) -> Result<crate::Timer> {
        let _state = self.scope.admit()?;
        self.inner.event_bus.ensure_open()?;
        Ok(crate::Timer::new(delay, self.scope.cancel.clone()))
    }
    /// Creates a Skip-policy periodic timer bound to this deployment.
    /// Rejects a zero period, stopped scopes or closed instances. Ticking needs a
    /// Tokio time driver; an unrepresentable initial deadline may panic.
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

#[cfg(test)]
mod tests;
