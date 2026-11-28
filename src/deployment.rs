use crate::context::{Context, ContextData};
use crate::scope::WorkScope;
use crate::{Result, TaskHandle, VertxError};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::watch;

/// Process-unique deployment identifier, assigned before the factory runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DeploymentId(pub(crate) u64);
static NEXT: AtomicU64 = AtomicU64::new(1);
impl DeploymentId {
    pub(crate) fn next() -> Self {
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}
impl std::fmt::Display for DeploymentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "deployment#{}", self.0)
    }
}

/// A handle to one deployment. It can dispatch work, request stopping and
/// observe the final result without keeping the deployment or instance alive.
#[derive(Clone, Debug)]
pub struct Deployment {
    id: DeploymentId,
    target: Weak<ContextData>,
    done: watch::Receiver<Option<Result<()>>>,
    grace: Duration,
}

impl Deployment {
    pub(crate) fn new(ctx: &Context, scope: &DeploymentScope) -> Self {
        Self {
            id: scope.id,
            target: ctx.downgrade(),
            done: scope.done.subscribe(),
            grace: ctx.shutdown_grace(),
        }
    }

    /// Returns the stable ID used in diagnostics and error reports.
    pub fn id(&self) -> DeploymentId {
        self.id
    }

    /// Queues a Send future on this deployment from any thread.
    /// Returns `DeploymentTerminated` when its execution target is gone.
    pub fn spawn<F, T>(&self, future: F) -> Result<TaskHandle<T>>
    where
        F: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        self.context()?.spawn(future)
    }

    /// Sets the exception hook for this deployment.
    /// Returns `DeploymentTerminated` once the deployment has sealed its hook.
    pub fn set_exception_handler<F>(&self, handler: F) -> Result<()>
    where
        F: Fn(&VertxError) + Send + Sync + 'static,
    {
        self.context()?
            .set_exception_handler(handler)
            .then_some(())
            .ok_or(VertxError::DeploymentTerminated(self.id))
    }

    /// Clears this deployment's exception hook, restoring the instance fallback.
    /// Returns `DeploymentTerminated` once the deployment has sealed its hook.
    pub fn clear_exception_handler(&self) -> Result<()> {
        self.context()?
            .clear_exception_handler()
            .then_some(())
            .ok_or(VertxError::DeploymentTerminated(self.id))
    }

    /// Requests stopping without waiting. Repeated calls are harmless.
    pub fn request_stop(&self) {
        if let Some(data) = self.target.upgrade() {
            Context::from_data(data).scope().request_stop();
        }
    }

    /// Requests stopping and waits up to the configured shutdown grace.
    /// After a timeout, cleanup continues and can be awaited with
    /// [`Self::wait_stopped`]. Waiting from this deployment returns
    /// [`VertxError::WouldDeadlock`].
    pub async fn stop(&self) -> Result<()> {
        self.check_waiter()?;
        self.request_stop();
        tokio::time::timeout(self.grace, wait_for_stop(self.done.clone()))
            .await
            .map_err(|_| VertxError::ShutdownTimeout(self.grace))?
    }

    /// Waits for cleanup and returns the recorded result, including after
    /// the deployment has been removed from the instance. Waiting from this
    /// deployment returns [`VertxError::WouldDeadlock`].
    pub async fn wait_stopped(&self) -> Result<()> {
        self.check_waiter()?;
        wait_for_stop(self.done.clone()).await
    }

    fn context(&self) -> Result<Context> {
        self.target
            .upgrade()
            .map(Context::from_data)
            .ok_or(VertxError::DeploymentTerminated(self.id))
    }

    fn check_waiter(&self) -> Result<()> {
        if Context::current().is_some_and(|ctx| ctx.deployment_id() == Some(self.id)) {
            return Err(VertxError::WouldDeadlock);
        }
        Ok(())
    }
}

pub(crate) struct DeploymentScope {
    pub id: DeploymentId,
    pub work: Arc<WorkScope>,
    done: watch::Sender<Option<Result<()>>>,
}

impl DeploymentScope {
    pub fn new(id: DeploymentId, instance: &WorkScope) -> Self {
        Self {
            id,
            work: Arc::new(instance.deployment(id)),
            done: watch::channel(None).0,
        }
    }

    pub fn finish(&self, outcome: Result<()>) {
        self.done.send_replace(Some(outcome));
    }

    pub fn wait(&self) -> impl Future<Output = Result<()>> + use<> {
        wait_for_stop(self.done.subscribe())
    }
}

async fn wait_for_stop(mut done: watch::Receiver<Option<Result<()>>>) -> Result<()> {
    loop {
        if let Some(result) = done.borrow().clone() {
            return result;
        }
        done.changed()
            .await
            .map_err(|_| VertxError::Other("deployment completion closed".into()))?;
    }
}
