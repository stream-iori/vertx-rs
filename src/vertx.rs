mod builder;
mod deploy;
mod lifecycle;
mod shutdown;

pub use builder::VertxBuilder;

use crate::context::{Context, ContextShared};
use crate::deployment::DeploymentScope;
use crate::diagnostics::CloseStage;
use crate::{DeploymentId, EventBus, Result, VertxError};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;

pub(crate) struct VertxInner {
    pub shared: Arc<ContextShared>,
    deployments: Mutex<HashMap<DeploymentId, Arc<DeploymentScope>>>,
    // Serializes deployment submission with the shutdown snapshot.
    close_started: Mutex<bool>,
    close_result: watch::Sender<Option<Result<()>>>,
}
impl Drop for VertxInner {
    fn drop(&mut self) {
        self.shared.cancel.cancel();
        for scope in self.deployments.get_mut().unwrap().values() {
            scope.request_stop();
        }
        self.shared.event_bus.close();
    }
}
#[derive(Clone)]
pub struct Vertx {
    pub(crate) inner: Arc<VertxInner>,
}

impl Vertx {
    pub(crate) fn from_inner(inner: Arc<VertxInner>) -> Self {
        Self { inner }
    }
    fn owns_context(&self, ctx: &Context) -> bool {
        ctx.owner()
            .is_some_and(|owner| Arc::ptr_eq(&owner.inner, &self.inner))
    }
    /// Creates an instance using default pool sizes and shutdown grace.
    ///
    /// # Panics
    /// Panics on runtime or thread initialization failure. For fallible construction,
    /// use `Vertx::builder().try_build()`.
    pub fn new() -> Self {
        Self::builder().build()
    }
    /// Starts configuration without creating threads.
    pub fn builder() -> VertxBuilder {
        VertxBuilder::default()
    }
    /// Samples counters and current operations without dispatching work.
    /// The result is not a globally atomic snapshot.
    pub fn diagnostics(&self) -> crate::DiagnosticsSnapshot {
        let mut snapshot = self.inner.shared.diagnostics.snapshot();
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
        self.inner.shared.event_bus.clone()
    }
    pub fn event_loop_count(&self) -> usize {
        self.inner.shared.event_loops.size()
    }
    pub fn worker_thread_count(&self) -> usize {
        self.inner.shared.workers.threads()
    }
    /// Returns whether business admission has closed.
    /// Cleanup and thread joins may still be in progress. Await [`Self::close`]
    /// to confirm that all executor threads have exited.
    pub fn is_closed(&self) -> bool {
        self.inner.shared.cancel.is_cancelled()
    }
    /// Creates a timer with an absolute deadline starting now.
    /// Returns `VertxClosed` after closing. Polling needs a Tokio time driver.
    /// An extremely large delay may panic if the deadline cannot be represented.
    pub fn timer(&self, delay: Duration) -> Result<crate::Timer> {
        self.event_bus().ensure_open()?;
        Ok(crate::Timer::new(delay, self.inner.shared.cancel.clone()))
    }
    /// Creates a periodic timer with an absolute initial deadline and Skip policy.
    /// Returns `InvalidOptions` for a zero period, or `VertxClosed` after closing.
    /// Ticking needs a Tokio time driver; an unrepresentable initial deadline may panic.
    pub fn periodic(&self, init: Duration, period: Duration) -> Result<crate::Periodic> {
        self.event_bus().ensure_open()?;
        crate::Periodic::new(init, period, self.inner.shared.cancel.clone())
    }
    pub fn set_exception_handler<F>(&self, handler: F)
    where
        F: Fn(&VertxError) + Send + Sync + 'static,
    {
        *self.inner.shared.exception_handler.write().unwrap() = Some(Arc::new(handler));
    }
    pub fn clear_exception_handler(&self) {
        self.inner.shared.exception_handler.write().unwrap().take();
    }
}
impl Default for Vertx {
    fn default() -> Self {
        Self::new()
    }
}
