mod blocking;
mod exception_handler;
mod future;

pub(crate) use exception_handler::ExceptionSlot;
pub(crate) use future::TaskResult;

use crate::deployment::DeploymentScope;
use crate::diagnostics::Diagnostics;
use crate::eventloop::EventLoopId;
use crate::scope::WorkScope;
use crate::tcp::TcpListeners;
use crate::vertx::{Executors, InstanceLifetime, InstanceServices, VertxHandle, VertxInner};
use crate::{
    Deployment, DeploymentId, EventBus, ExecutionInfo, ExecutorThread, Result, ThreadingModel,
    VertxError,
};
use crate::{TaskHandle, TaskId};
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::marker::PhantomData;
use std::mem::DropGuard;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use tokio::sync::Mutex;

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
pub(crate) struct ContextData {
    id: ContextId,
    target: Target,
    executors: Arc<Executors>,
    services: Arc<InstanceServices>,
    lifetime: Arc<InstanceLifetime>,
    deployment: Option<Arc<DeploymentScope>>,
    vertx: Weak<VertxInner>,
    blocking_order_lock: Arc<Mutex<()>>,
    exception_handler: ExceptionSlot,
}
enum Target {
    EventLoop(EventLoopId),
    Worker {
        driver: tokio::runtime::Handle,
        execution_gate: std::sync::Mutex<()>,
    },
}
/// Logical execution context. Its kind is independent of the physical thread
/// used by `execute_blocking`. Framework tasks restore it for each poll.
#[derive(Clone)]
pub(crate) struct Context {
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
    fn new(
        target: Target,
        vertx: &Arc<VertxInner>,
        deployment: Option<Arc<DeploymentScope>>,
    ) -> Self {
        Self {
            data: Arc::new(ContextData {
                id: ContextId(NEXT_CONTEXT.fetch_add(1, Ordering::Relaxed)),
                target,
                executors: vertx.executors.clone(),
                services: vertx.services.clone(),
                lifetime: vertx.lifetime.clone(),
                deployment,
                vertx: Arc::downgrade(vertx),
                blocking_order_lock: Arc::new(Mutex::new(())),
                exception_handler: ExceptionSlot::default(),
            }),
            stopping: false,
        }
    }
    pub(crate) fn new_event_loop(
        id: EventLoopId,
        vertx: &Arc<VertxInner>,
        deployment: Option<Arc<DeploymentScope>>,
    ) -> Self {
        Self::new(Target::EventLoop(id), vertx, deployment)
    }
    pub(crate) fn new_worker(vertx: &Arc<VertxInner>, scope: Arc<DeploymentScope>) -> Self {
        let loops = &vertx.executors.event_loops;
        Self::new(
            Target::Worker {
                driver: loops.driver(loops.pick()),
                execution_gate: std::sync::Mutex::new(()),
            },
            vertx,
            Some(scope),
        )
    }
    pub(crate) fn scope(&self) -> &Arc<WorkScope> {
        self.data
            .deployment
            .as_ref()
            .map_or(&self.data.lifetime.scope, |scope| &scope.work)
    }
    pub(crate) fn deployment_scope(&self) -> &DeploymentScope {
        self.data.deployment.as_deref().expect("deployment context")
    }
    pub(crate) fn deployment(&self) -> Option<Deployment> {
        self.data
            .deployment
            .as_ref()
            .map(|scope| Deployment::new(self, scope))
    }
    pub(crate) fn from_data(data: Arc<ContextData>) -> Self {
        Self {
            data,
            stopping: false,
        }
    }
    pub(crate) fn downgrade(&self) -> Weak<ContextData> {
        Arc::downgrade(&self.data)
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
    pub(crate) fn instance_admission_closed(&self) -> bool {
        self.scope().instance_admission_closed()
    }
    pub(crate) fn belongs_to(&self, vertx: &Weak<VertxInner>) -> bool {
        Weak::ptr_eq(&self.data.vertx, vertx)
    }
    pub(crate) fn for_stop(&self) -> Self {
        let mut c = self.clone();
        c.stopping = true;
        c
    }
    pub(crate) fn event_loop_id(&self) -> Option<EventLoopId> {
        match self.data.target {
            Target::EventLoop(id) => Some(id),
            Target::Worker { .. } => None,
        }
    }
    pub(crate) fn deployment_id(&self) -> Option<DeploymentId> {
        self.data.deployment.as_ref().map(|scope| scope.id)
    }
    pub(crate) fn vertx_handle(&self) -> VertxHandle {
        VertxHandle::new(self.data.vertx.clone())
    }
    pub(crate) fn instance(&self) -> Option<Arc<VertxInner>> {
        self.data.vertx.upgrade()
    }
    pub(crate) fn current() -> Option<Self> {
        CURRENT.with(|c| c.borrow().clone())
    }
    pub(crate) fn execution_info(&self) -> Option<ExecutionInfo> {
        let thread = match thread_identity()? {
            ThreadIdentity::EventLoop(_, id) => ExecutorThread::EventLoop(id),
            ThreadIdentity::Worker(_) => ExecutorThread::Worker,
        };
        let event_loop_id = self.event_loop_id();
        Some(ExecutionInfo {
            context_id: self.id(),
            deployment_id: self.deployment_id(),
            threading_model: if event_loop_id.is_some() {
                ThreadingModel::EventLoop
            } else {
                ThreadingModel::Worker
            },
            event_loop_id,
            thread,
        })
    }
    pub(crate) fn same(&self, other: &Context) -> bool {
        Arc::ptr_eq(&self.data, &other.data)
    }
    pub(crate) fn ensure_accepting_work(&self) -> Result<()> {
        self.scope().ensure_open()
    }
    pub(crate) fn is_on_event_loop(&self) -> bool {
        self.event_loop_id()
            .is_some_and(|id| self.data.executors.event_loops.check_thread(id))
    }
    pub(crate) fn ensure_current_event_loop_context(&self) -> Result<()> {
        self.ensure_accepting_work()?;
        if self.is_on_event_loop() && Self::current().is_some_and(|current| self.same(&current)) {
            return Ok(());
        }
        Err(VertxError::ContextMismatch)
    }
    /// Replaces this Context's hook for managed task and lifecycle failures.
    /// Returns whether the exception slot accepted the update.
    pub(crate) fn set_exception_handler<F>(&self, f: F) -> bool
    where
        F: Fn(&VertxError) + Send + Sync + 'static,
    {
        self.data.exception_handler.set(f)
    }
    /// Restores fallback to the owning instance's exception hook.
    pub(crate) fn clear_exception_handler(&self) -> bool {
        self.data.exception_handler.clear()
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
        let deployment_id = self.deployment_id();
        let context_id = self.id();
        if diagnostics.options.enabled {
            tracing::error!(
                instance_id, deployment_id = deployment_id.as_ref().map(tracing::field::display), context_id = %context_id,
                error_kind = err.kind(), error = %err, "framework operation failed"
            );
        }
        if let Some(handler) = handler {
            if let Err(payload) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(err)))
            {
                let error = crate::error::panic_error(payload);
                tracing::error!(
                    instance_id, deployment_id = deployment_id.as_ref().map(tracing::field::display), context_id = %context_id,
                    error_kind = error.kind(), %error, "exception handler panicked"
                );
            }
            if let Err(error) = exception_handler::drop_handler(Some(handler)) {
                tracing::error!(
                    instance_id, deployment_id = deployment_id.as_ref().map(tracing::field::display), context_id = %context_id,
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
            || matches!(error, VertxError::DeploymentTerminated(id) if Some(*id) == self.deployment_id() && self.scope().cancel.is_cancelled())
            || matches!(error, VertxError::VertxClosed if self.instance_admission_closed())
    }
    pub(crate) fn spawn_local<F, T>(&self, future: F) -> Result<TaskHandle<T>>
    where
        F: Future<Output = Result<T>> + 'static,
        T: 'static,
    {
        self.spawn_local_with_id(TaskId::next(), future)
    }

    pub(crate) fn spawn_local_with_id<F, T>(&self, id: TaskId, future: F) -> Result<TaskHandle<T>>
    where
        F: Future<Output = Result<T>> + 'static,
        T: 'static,
    {
        self.ensure_current_event_loop_context()?;
        let registration = self.scope().register_task(id)?;
        let frame = self.frame(id);
        let task = self.clone().track_future(frame, registration, future);
        let handle = TaskHandle::from_tokio(id, tokio::task::spawn_local(task));
        self.scope().attach_task(id, handle.abort_handle());
        Ok(handle)
    }

    pub(crate) fn spawn<F, T>(&self, future: F) -> Result<TaskHandle<T>>
    where
        F: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        if let Some(id) = self.event_loop_id()
            && !(self.data.executors.event_loops.check_thread(id) && Self::current().is_some())
        {
            return self.spawn_pinned(move || future);
        }
        let id = TaskId::next();
        let registration = self.scope().register_task(id)?;
        let task = self
            .clone()
            .track_future(self.frame(id), registration, future);
        let handle = match self.data.target {
            Target::EventLoop(_) => TaskHandle::from_tokio(id, tokio::task::spawn_local(task)),
            Target::Worker { .. } => self.spawn_worker(id, task)?,
        };
        self.scope().attach_task(id, handle.abort_handle());
        Ok(handle)
    }

    pub(crate) fn spawn_pinned<F, Fut, T>(&self, factory: F) -> Result<TaskHandle<T>>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + 'static,
        T: Send + 'static,
    {
        let loop_id = self.event_loop_id().ok_or(VertxError::ContextMismatch)?;
        let id = TaskId::next();
        let registration = self.scope().register_task(id)?;
        let frame = self.frame(id);
        let ctx = self.clone();
        let handle = self
            .data
            .executors
            .event_loops
            .spawn_pinned(loop_id, id, move || {
                ctx.track_future(frame, registration, factory())
            })?;
        self.scope().attach_task(id, handle.abort_handle());
        Ok(handle)
    }

    pub(crate) fn spawn_worker<F, T>(&self, id: TaskId, task: F) -> Result<TaskHandle<T>>
    where
        F: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        let Target::Worker { driver, .. } = &self.data.target else {
            unreachable!("worker task requires a Worker Context");
        };
        self.data
            .executors
            .workers
            .spawn(id, self.data.id.0, driver.clone(), task)
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
