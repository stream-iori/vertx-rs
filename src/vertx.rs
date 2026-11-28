mod builder;
mod deploy;
mod lifecycle;
mod shutdown;

pub use builder::VertxBuilder;

use crate::context::{Context, ExceptionSlot};
use crate::deployment::DeploymentScope;
use crate::diagnostics::{CloseStage, Diagnostics};
use crate::eventloop::EventLoopPool;
use crate::tcp::TcpListeners;
use crate::worker_pool::WorkerPool;
use crate::{DeploymentId, EventBus, Result, VertxError};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub(crate) struct Executors {
    pub event_loops: EventLoopPool,
    pub workers: WorkerPool,
}

pub(crate) struct InstanceServices {
    pub diagnostics: Arc<Diagnostics>,
    pub event_bus: EventBus,
    pub tcp: TcpListeners,
    pub exception_handler: ExceptionSlot,
}

pub(crate) struct InstanceLifetime {
    pub cancel: CancellationToken,
    pub blocking: TaskTracker,
    pub shutdown_grace: Duration,
}

#[derive(Default)]
struct Deployments {
    closing: bool,
    scopes: HashMap<DeploymentId, Arc<DeploymentScope>>,
}

pub(crate) struct VertxInner {
    pub executors: Arc<Executors>,
    pub services: Arc<InstanceServices>,
    pub lifetime: Arc<InstanceLifetime>,
    deployments: Mutex<Deployments>,
    close_result: watch::Sender<Option<Result<()>>>,
}
impl VertxInner {
    pub(crate) fn forget_deployment(&self, id: DeploymentId) {
        let removed = self.deployments.lock().unwrap().scopes.remove(&id);
        drop(removed);
    }
}
impl Drop for VertxInner {
    fn drop(&mut self) {
        self.services.tcp.close_admission();
        self.lifetime.cancel.cancel();
        for scope in self.deployments.get_mut().unwrap().scopes.values() {
            scope.request_stop();
        }
        self.services.event_bus.close();
    }
}
/// Shared handle to the instance's deployments, executors and local services.
#[derive(Clone)]
pub struct Vertx {
    pub(crate) inner: Arc<VertxInner>,
}

impl Vertx {
    pub(crate) fn from_inner(inner: Arc<VertxInner>) -> Self {
        Self { inner }
    }
    fn owns_context(&self, ctx: &Context) -> bool {
        ctx.belongs_to(&self.inner)
    }
    /// Creates an instance using default pool sizes and shutdown grace.
    /// Returns a construction error after joining any threads created during startup.
    pub fn new() -> Result<Self> {
        Self::builder().build()
    }
    /// Starts configuration without creating threads.
    pub fn builder() -> VertxBuilder {
        VertxBuilder::default()
    }
    /// Samples counters and current operations without dispatching work.
    /// The result is not a globally atomic snapshot.
    pub fn diagnostics(&self) -> crate::DiagnosticsSnapshot {
        let mut snapshot = self.inner.services.diagnostics.snapshot();
        if snapshot.enabled
            && let Some(result) = self.inner.close_result.borrow().as_ref()
        {
            snapshot.close_stage = if result.is_ok() {
                CloseStage::Closed
            } else {
                CloseStage::Failed
            }
            .as_str();
        }
        snapshot
    }
    /// Returns the instance bus. Consumer registration in one of this instance's
    /// Contexts binds to it; registration outside a matching Context is unbound.
    pub fn event_bus(&self) -> EventBus {
        self.inner.services.event_bus.clone()
    }
    /// Returns the configured number of EventLoops, each with one OS thread.
    pub fn event_loop_count(&self) -> usize {
        self.inner.executors.event_loops.size()
    }
    /// Returns the number of OS threads shared by Worker futures and blocking jobs.
    pub fn worker_thread_count(&self) -> usize {
        self.inner.executors.workers.threads()
    }
    /// Returns whether business admission has closed.
    /// Cleanup and thread joins may still be in progress. Await [`Self::close`]
    /// to confirm that all executor threads have exited.
    pub fn is_closed(&self) -> bool {
        self.inner.lifetime.cancel.is_cancelled()
    }
    /// Creates a timer with an absolute deadline starting now.
    /// Returns `VertxClosed` after closing, or `InvalidOptions` if the deadline
    /// cannot be represented. Polling requires a Tokio time driver.
    pub fn timer(&self, delay: Duration) -> Result<crate::Timer> {
        self.ensure_open()?;
        crate::Timer::new(delay, self.inner.lifetime.cancel.clone())
    }
    /// Creates a periodic timer with an absolute initial deadline and Skip policy.
    /// Returns `InvalidOptions` for a zero period or an unrepresentable deadline,
    /// or `VertxClosed` after closing. Ticking requires a Tokio time driver.
    pub fn periodic(&self, init: Duration, period: Duration) -> Result<crate::Periodic> {
        self.ensure_open()?;
        crate::Periodic::new(init, period, self.inner.lifetime.cancel.clone())
    }
    fn ensure_open(&self) -> Result<()> {
        if self.is_closed() {
            Err(VertxError::VertxClosed)
        } else {
            Ok(())
        }
    }
    /// Sets the fallback exception hook for Contexts without a local hook.
    /// A fully closed instance discards the hook.
    pub fn set_exception_handler<F>(&self, handler: F)
    where
        F: Fn(&VertxError) + Send + Sync + 'static,
    {
        self.inner.services.exception_handler.set(handler);
    }
    /// Removes the instance-wide fallback exception hook.
    pub fn clear_exception_handler(&self) {
        self.inner.services.exception_handler.clear();
    }
}
