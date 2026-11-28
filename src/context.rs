mod blocking;
mod current;
mod exception_handler;
mod future;
mod spawn;

pub(crate) use current::{ThreadIdentity, enter_thread, thread_identity};
pub(crate) use exception_handler::ExceptionSlot;
pub(crate) use future::TaskResult;

use crate::deployment::DeploymentScope;
use crate::diagnostics::Diagnostics;
use crate::eventloop::EventLoopId;
use crate::scope::WorkScope;
use crate::tcp::TcpListeners;
use crate::vertx::{Executors, InstanceLifetime, InstanceServices, VertxHandle, VertxInner};
use crate::{Deployment, DeploymentId, EventBus, Result, VertxError};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use tokio::sync::Mutex;

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
    pub(crate) fn same(&self, other: &Context) -> bool {
        Arc::ptr_eq(&self.data, &other.data)
    }
    pub(crate) fn ensure_accepting_work(&self) -> Result<()> {
        self.scope().ensure_open()
    }
    pub(crate) fn is_expected_cancellation(&self, error: &VertxError) -> bool {
        matches!(error, VertxError::TaskCancelled)
            || matches!(error, VertxError::DeploymentTerminated(id) if Some(*id) == self.deployment_id() && self.scope().cancel.is_cancelled())
            || matches!(error, VertxError::VertxClosed if self.instance_admission_closed())
    }
}

#[cfg(test)]
mod tests;
