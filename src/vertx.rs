mod builder;
mod deploy;
mod handle;
mod lifecycle;
mod shutdown;

pub use builder::VertxBuilder;
pub use handle::VertxHandle;

use crate::context::ExceptionSlot;
use crate::deployment::DeploymentScope;
use crate::diagnostics::{CloseStage, Diagnostics};
use crate::eventloop::EventLoopPool;
use crate::scope::WorkScope;
use crate::tcp::TcpListeners;
use crate::worker_pool::WorkerPool;
use crate::{DeploymentId, EventBus, Result, VertxError};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;

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
    pub scope: Arc<WorkScope>,
    pub shutdown_grace: Duration,
}

pub(crate) struct VertxInner {
    pub executors: Arc<Executors>,
    pub services: Arc<InstanceServices>,
    pub lifetime: Arc<InstanceLifetime>,
    deployments: Mutex<HashMap<DeploymentId, Arc<DeploymentScope>>>,
    close_result: watch::Sender<Option<Result<()>>>,
}
impl VertxInner {
    fn ensure_open(&self) -> Result<()> {
        if self.lifetime.scope.instance_admission_closed() {
            return Err(VertxError::VertxClosed);
        }
        Ok(())
    }

    pub(crate) fn forget_deployment(&self, id: DeploymentId) {
        let removed = self.deployments.lock().unwrap().remove(&id);
        drop(removed);
    }
}
impl Drop for VertxInner {
    fn drop(&mut self) {
        self.lifetime.scope.close_instance_admission();
        self.services.tcp.close_admission();
        self.lifetime.scope.request_stop();
        for scope in self.deployments.get_mut().unwrap().values() {
            scope.work.request_stop();
        }
        self.services.event_bus.close();
        self.lifetime.scope.unregister_resources();
        self.lifetime.scope.abort_tasks();
    }
}
/// Shared handle to the instance's deployments, executors and local services.
#[derive(Clone)]
pub struct Vertx {
    pub(crate) inner: Arc<VertxInner>,
}

impl Vertx {
    /// Returns a weak handle for business operations. It can be moved into
    /// callbacks and tasks without extending the instance's lifetime.
    pub fn handle(&self) -> VertxHandle {
        VertxHandle::new(Arc::downgrade(&self.inner))
    }

    /// Creates an instance using default pool sizes and shutdown grace.
    /// Returns a construction error after joining any threads created during startup.
    pub fn try_new() -> Result<Self> {
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
    pub fn is_admission_closed(&self) -> bool {
        self.inner.lifetime.scope.instance_admission_closed()
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
