mod blocking;
mod exception_handler;
mod future;

pub(crate) use exception_handler::ExceptionSlot;
pub(crate) use future::TaskResult;

use crate::deployment::DeploymentScope;
use crate::diagnostics::Diagnostics;
use crate::eventloop::EventLoopId;
use crate::tcp::TcpListeners;
use crate::vertx::{Executors, InstanceLifetime, InstanceServices, Vertx, VertxInner};
use crate::{DeploymentId, EventBus, Result, ThreadingModel, VertxError};
use crate::{TaskHandle, TaskId};
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::marker::PhantomData;
use std::mem::DropGuard;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ThreadIdentity {
    EventLoop(u64, EventLoopId),
    Worker(u64),
}
thread_local! {
    static CURRENT: RefCell<Option<Context>> = const { RefCell::new(None) };
    static THREAD: Cell<Option<ThreadIdentity>> = const { Cell::new(None) };
}
pub(crate) fn enter_thread(identity: ThreadIdentity) -> impl Drop {
    let previous = THREAD.replace(Some(identity));
    DropGuard::new((previous, PhantomData::<Rc<()>>), |(previous, _)| {
        THREAD.set(previous);
    })
}
pub(crate) fn thread_identity() -> Option<ThreadIdentity> {
    THREAD.with(Cell::get)
}
struct ContextData {
    id: ContextId,
    target: Target,
    executors: Arc<Executors>,
    services: Arc<InstanceServices>,
    lifetime: Arc<InstanceLifetime>,
    scope: Arc<DeploymentScope>,
    owner: Weak<VertxInner>,
    ordered: Arc<Mutex<()>>,
    exception_handler: ExceptionSlot,
}
enum Target {
    EventLoop(EventLoopId),
    Worker {
        driver: tokio::runtime::Handle,
        execution: std::sync::Mutex<()>,
    },
}
/// Logical execution context. Its kind is independent of the physical thread
/// used by `execute_blocking`. Framework tasks restore it for each poll.
#[derive(Clone)]
pub struct Context {
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
    /// Returns the process-unique identity shared by this Context's clones.
    pub fn id(&self) -> ContextId {
        self.data.id
    }
    fn new(target: Target, owner: &Arc<VertxInner>, scope: Arc<DeploymentScope>) -> Self {
        Self {
            data: Arc::new(ContextData {
                id: ContextId(NEXT_CONTEXT.fetch_add(1, Ordering::Relaxed)),
                target,
                executors: owner.executors.clone(),
                services: owner.services.clone(),
                lifetime: owner.lifetime.clone(),
                scope,
                owner: Arc::downgrade(owner),
                ordered: Arc::new(Mutex::new(())),
                exception_handler: ExceptionSlot::default(),
            }),
            stopping: false,
        }
    }
    pub(crate) fn new_event_loop(
        id: EventLoopId,
        owner: &Arc<VertxInner>,
        scope: Arc<DeploymentScope>,
    ) -> Self {
        Self::new(Target::EventLoop(id), owner, scope)
    }
    pub(crate) fn new_worker(owner: &Arc<VertxInner>, scope: Arc<DeploymentScope>) -> Self {
        let loops = &owner.executors.event_loops;
        Self::new(
            Target::Worker {
                driver: loops.driver(loops.pick()),
                execution: std::sync::Mutex::new(()),
            },
            owner,
            scope,
        )
    }
    pub(crate) fn scope(&self) -> &Arc<DeploymentScope> {
        &self.data.scope
    }
    pub(crate) fn diagnostics(&self) -> &Arc<Diagnostics> {
        &self.data.services.diagnostics
    }
    pub(crate) fn tcp_listeners(&self) -> &TcpListeners {
        &self.data.services.tcp
    }
    pub(crate) fn instance_event_bus(&self) -> &EventBus {
        &self.data.services.event_bus
    }
    pub(crate) fn shutdown_grace(&self) -> std::time::Duration {
        self.data.lifetime.shutdown_grace
    }
    pub(crate) fn instance_closed(&self) -> bool {
        self.data.lifetime.cancel.is_cancelled()
    }
    pub(crate) fn belongs_to(&self, owner: &Arc<VertxInner>) -> bool {
        std::ptr::eq(self.data.owner.as_ptr(), Arc::as_ptr(owner))
    }
    pub(crate) fn for_stop(&self) -> Self {
        let mut c = self.clone();
        c.stopping = true;
        c
    }
    /// Reports the logical kind, even inside a blocking closure running on a Worker.
    /// Use [`Self::is_on_event_loop_thread`] to inspect the physical thread.
    pub fn is_event_loop(&self) -> bool {
        matches!(self.data.target, Target::EventLoop(_))
    }
    /// Reports the logical kind, independent of the current physical thread.
    pub fn is_worker(&self) -> bool {
        matches!(self.data.target, Target::Worker { .. })
    }
    /// Returns the logical execution model assigned at deployment.
    pub fn threading_model(&self) -> ThreadingModel {
        if self.is_event_loop() {
            ThreadingModel::EventLoop
        } else {
            ThreadingModel::Worker
        }
    }
    /// Returns the assigned EventLoop for ordinary verticles.
    pub fn event_loop_id(&self) -> Option<EventLoopId> {
        match self.data.target {
            Target::EventLoop(id) => Some(id),
            Target::Worker { .. } => None,
        }
    }
    /// Returns the owning deployment's ID, which persists after shutdown.
    pub fn deployment_id(&self) -> DeploymentId {
        self.scope().deployment
    }
    /// Returns the owning instance if a Vertx handle or its close coordinator is alive.
    pub fn owner(&self) -> Option<Vertx> {
        self.data.owner.upgrade().map(Vertx::from_inner)
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
    /// Reports whether the current OS thread belongs to any Vertx executor.
    pub fn is_on_vertx_thread() -> bool {
        thread_identity().is_some()
    }
    /// Reports whether this deployment currently admits ordinary work.
    pub fn is_alive(&self) -> bool {
        self.ensure_alive().is_ok()
    }
    /// Returns the deployment cancellation token. Cancelling it requests shutdown;
    /// it cannot interrupt a running synchronous poll or blocking closure.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.scope().cancel.clone()
    }
    /// Returns a bus whose consumers are bound to this Context and deployment.
    pub fn event_bus(&self) -> EventBus {
        self.instance_event_bus().bound(self.clone())
    }
    pub(crate) fn same(&self, other: &Context) -> bool {
        Arc::ptr_eq(&self.data, &other.data)
    }
    pub(crate) fn ensure_alive(&self) -> Result<()> {
        if self.instance_closed() {
            return Err(VertxError::VertxClosed);
        }
        drop(self.scope().admit()?);
        Ok(())
    }
    pub(crate) fn check_local(&self) -> Result<()> {
        self.ensure_alive()?;
        if let Some(id) = self.event_loop_id()
            && self.data.executors.event_loops.check_thread(id)
            && Self::current().is_some_and(|c| self.same(&c))
        {
            return Ok(());
        }
        Err(VertxError::ContextMismatch)
    }
    /// Replaces this Context's hook for managed task and lifecycle failures.
    /// The hook may change its own registration. A stopped Context discards it.
    pub fn set_exception_handler<F>(&self, f: F)
    where
        F: Fn(&VertxError) + Send + Sync + 'static,
    {
        self.data.exception_handler.set(f);
    }
    /// Restores fallback to the owning instance's exception hook.
    pub fn clear_exception_handler(&self) {
        self.data.exception_handler.clear();
    }
    pub(crate) fn seal_exception_handler(&self) -> Result<()> {
        self.data.exception_handler.seal()
    }
    pub(crate) fn report_error(&self, err: &VertxError) {
        let handler = self
            .data
            .exception_handler
            .snapshot()
            .or_else(|| self.data.services.exception_handler.snapshot());
        let diagnostics = self.diagnostics();
        let instance_id = diagnostics.instance;
        let deployment_id = self.scope().deployment;
        let context_id = self.id();
        if diagnostics.options.enabled {
            tracing::error!(
                instance_id, deployment_id = %deployment_id, context_id = %context_id,
                error_kind = err.kind(), error = %err, "framework operation failed"
            );
        }
        if let Some(handler) = handler {
            if let Err(payload) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(err)))
            {
                let error = crate::error::panic_error(payload);
                tracing::error!(
                    instance_id, deployment_id = %deployment_id, context_id = %context_id,
                    error_kind = error.kind(), %error, "exception handler panicked"
                );
            }
            if let Err(error) = exception_handler::drop_handler(Some(handler)) {
                tracing::error!(
                    instance_id, deployment_id = %deployment_id, context_id = %context_id,
                    error_kind = error.kind(), %error, "exception handler destructor panicked"
                );
            }
        }
    }
    pub(crate) fn enter(&self) -> ContextGuard {
        let prev = CURRENT.with(|c| c.replace(Some(self.clone())));
        ContextGuard {
            prev,
            _not_send: PhantomData,
        }
    }
    pub(crate) fn is_expected_cancellation(&self, error: &VertxError) -> bool {
        matches!(error, VertxError::TaskCancelled)
            || matches!(error, VertxError::DeploymentTerminated(id) if *id == self.scope().deployment && self.scope().cancel.is_cancelled())
            || matches!(error, VertxError::VertxClosed if self.instance_closed())
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
        self.spawn_local_with_id(TaskId::next(), future)
    }

    pub(crate) fn spawn_local_with_id<F>(&self, id: TaskId, future: F) -> Result<TaskHandle<()>>
    where
        F: Future<Output = Result<()>> + 'static,
    {
        self.check_local()?;
        let registration = self.scope().register_task(id)?;
        let frame = self.frame(id);
        let task = self.clone().track_future(frame, registration, future);
        let handle = TaskHandle::from_tokio(id, tokio::task::spawn_local(task));
        self.scope().attach_task(id, handle.abort_handle());
        Ok(handle)
    }
    /// Queue a `Send` future on this Context, including from an external thread.
    ///
    /// EventLoop targets remain pinned and the future never runs inline.
    /// Rejects closed scopes and instances.
    /// Dropping the handle detaches the waiter; abort requests executor-side
    /// cancellation and cannot interrupt synchronous code.
    ///
    /// ```
    /// use vertx_rs::{Context, Result, TaskHandle};
    ///
    /// fn dispatch(ctx: Context) -> std::thread::JoinHandle<Result<TaskHandle<()>>> {
    ///     std::thread::spawn(move || ctx.spawn(async { Ok(()) }))
    /// }
    /// ```
    pub fn spawn<F>(&self, future: F) -> Result<TaskHandle<()>>
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        self.ensure_alive()?;
        let id = TaskId::next();
        let registration = self.scope().register_task(id)?;
        let frame = self.frame(id);
        let ctx = self.clone();
        let loops = &self.data.executors.event_loops;
        let handle = match self.data.target {
            Target::EventLoop(loop_id)
                if loops.check_thread(loop_id) && Self::current().is_some() =>
            {
                let task = ctx.track_future(frame, registration, future);
                TaskHandle::from_tokio(id, tokio::task::spawn_local(task))
            }
            Target::EventLoop(loop_id) => loops.spawn_pinned(loop_id, id, move || {
                ctx.track_future(frame, registration, future)
            })?,
            Target::Worker { .. } => {
                let task = ctx.track_future(frame, registration, future);
                self.spawn_worker(id, task)?
            }
        };
        self.scope().attach_task(id, handle.abort_handle());
        Ok(handle)
    }
    pub(crate) fn spawn_worker<F>(&self, id: TaskId, task: F) -> Result<TaskHandle<()>>
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        let Target::Worker { driver, .. } = &self.data.target else {
            unreachable!("worker task requires a Worker Context");
        };
        self.data
            .executors
            .workers
            .spawn(id, self.data.id.0, driver.clone(), task)
    }
    /// Requests this deployment to stop without waiting for its own tasks.
    /// Use this inside the deployment instead of awaiting its own undeploy.
    /// A scope that no longer admits work returns `DeploymentTerminated`.
    pub fn request_undeploy(&self) -> Result<()> {
        self.ensure_alive()?;
        self.scope().request_stop();
        Ok(())
    }
    /// Creates a timer starting now, cancelled with this deployment.
    /// Rejects stopped scopes, closed instances and unrepresentable deadlines.
    /// Polling requires a Tokio time driver.
    pub fn timer(&self, delay: std::time::Duration) -> Result<crate::Timer> {
        drop(self.scope().admit()?);
        crate::Timer::new(delay, self.cancellation_token())
    }
    /// Creates a Skip-policy periodic timer bound to this deployment.
    /// Rejects zero periods, unrepresentable deadlines, stopped scopes and
    /// closed instances. Ticking requires a Tokio time driver.
    pub fn periodic(
        &self,
        init: std::time::Duration,
        period: std::time::Duration,
    ) -> Result<crate::Periodic> {
        drop(self.scope().admit()?);
        crate::Periodic::new(init, period, self.cancellation_token())
    }
}
pub(crate) struct ContextGuard {
    prev: Option<Context>,
    _not_send: PhantomData<Rc<()>>,
}
impl Drop for ContextGuard {
    fn drop(&mut self) {
        let entered = CURRENT.with(|c| c.replace(self.prev.take()));
        drop(entered);
    }
}

#[cfg(test)]
mod tests;
