use super::Vertx;
use super::lifecycle::{ReadySender, drive_local, drive_worker};
use crate::deployment::DeploymentScope;
use crate::diagnostics::DeploymentStage;
use crate::{
    Context, DeploymentId, DeploymentOptions, Result, TaskHandle, Verticle, VertxError,
    WorkerVerticle,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

impl Vertx {
    /// Constructs a non-Send verticle on its assigned EventLoop and awaits start.
    /// Factory, lifecycle and managed work stay on that OS thread. Dropping the
    /// waiter requests background cleanup. Successful start leaves the deployment
    /// running until undeploy/close; startup and cleanup failures are retained.
    pub async fn deploy<F, V>(&self, factory: F) -> Result<DeploymentId>
    where
        F: FnOnce(Context) -> V + Send + 'static,
        V: Verticle,
    {
        let shared = &self.inner.shared;
        let pending = self.submit_deployment(|scope, ready| {
            let loop_id = shared.event_loops.pick();
            let ctx = Context::new_event_loop(loop_id, shared.clone(), scope);
            shared.event_loops.spawn_pinned(loop_id, move || {
                ctx.wrap(drive_local(ctx.clone(), factory, ready))
            })
        })?;
        pending.wait_ready("lifecycle driver terminated").await
    }

    /// Constructs a Send Worker verticle and awaits its asynchronous start.
    /// Same-Context polls are serialized but may migrate across Worker threads.
    /// Dropping the waiter requests background cleanup, as with [`Self::deploy`].
    pub async fn deploy_worker<F, V>(&self, factory: F) -> Result<DeploymentId>
    where
        F: FnOnce(Context) -> V + Send + 'static,
        V: WorkerVerticle,
    {
        let shared = &self.inner.shared;
        let pending = self.submit_deployment(|scope, ready| {
            let ctx = Context::new_worker(shared.clone(), scope);
            let future = ctx.wrap(drive_worker(ctx.clone(), factory, ready));
            shared
                .workers
                .spawn(ctx.worker_lane(), ctx.worker_driver(), future)
        })?;
        pending
            .wait_ready("worker lifecycle driver terminated")
            .await
    }

    /// Deploys instances sequentially with shared typed configuration.
    /// Failure rolls back prior instances and retains rollback errors. Dropping
    /// the waiter requests cleanup of current and prior instances. Zero instances
    /// are normalized to one by [`DeploymentOptions::instances`].
    pub async fn deploy_with<F, V, C>(
        &self,
        factory: F,
        options: DeploymentOptions<C>,
    ) -> Result<Vec<DeploymentId>>
    where
        F: Fn(Context, Arc<C>) -> V + Send + Sync + 'static,
        V: Verticle,
        C: Send + Sync + 'static,
    {
        let factory = Arc::new(factory);
        let config = options.shared_config();
        let mut ids = Vec::new();
        let mut rollback = Rollback::default();
        for _ in 0..options.instances_count() {
            let (factory, config) = (factory.clone(), config.clone());
            match self.deploy(move |ctx| factory(ctx, config)).await {
                Ok(id) => {
                    if let Some(scope) = self.deployment_scope(id) {
                        rollback.scopes.push(scope);
                    }
                    ids.push(id);
                }
                Err(cause) => {
                    let failures = rollback.wait(self.inner.shared.shutdown_grace).await;
                    return Err(VertxError::DeploymentFailed {
                        cause: Box::new(cause),
                        rollback: failures,
                    });
                }
            }
        }
        rollback.scopes.clear();
        Ok(ids)
    }

    /// Requests shutdown and waits for managed state destruction and stop.
    /// One shutdown grace bounds the wait; timeout leaves cleanup running.
    /// Waiting from the same deployment returns `WouldDeadlock`; use
    /// [`Context::request_undeploy`] there. Unknown IDs return `DeploymentNotFound`.
    pub async fn undeploy(&self, id: DeploymentId) -> Result<()> {
        if Context::current()
            .is_some_and(|ctx| ctx.deployment_id() == Some(id) && self.owns_context(&ctx))
        {
            return Err(VertxError::WouldDeadlock);
        }
        let scope = self
            .deployment_scope(id)
            .ok_or(VertxError::DeploymentNotFound(id))?;
        scope.request_stop();
        let result = scope
            .wait_with_grace(self.inner.shared.shutdown_grace)
            .await;
        if matches!(result, Err(VertxError::ShutdownTimeout(_))) {
            tracing::warn!(deployment = %id, "shutdown timed out; cleanup continues");
        }
        result
    }

    fn deployment_scope(&self, id: DeploymentId) -> Option<Arc<DeploymentScope>> {
        self.inner.deployments.lock().unwrap().get(&id).cloned()
    }

    fn submit_deployment(
        &self,
        submit: impl FnOnce(Arc<DeploymentScope>, ReadySender) -> Result<TaskHandle<()>>,
    ) -> Result<PendingDeployment> {
        let close_started = self.inner.close_started.lock().unwrap();
        if *close_started {
            return Err(VertxError::VertxClosed);
        }
        let shared = &self.inner.shared;
        shared.event_bus.ensure_open()?;

        let id = DeploymentId::next();
        let scope = Arc::new(DeploymentScope::new(id, shared.cancel.child_token()));
        let (started, ready) = oneshot::channel();
        shared.diagnostics.deployment(id, DeploymentStage::Queued);
        self.inner
            .deployments
            .lock()
            .unwrap()
            .insert(id, scope.clone());
        if let Err(error) = submit(scope.clone(), started) {
            self.inner.deployments.lock().unwrap().remove(&id);
            shared.diagnostics.deployment(id, DeploymentStage::Stopped);
            return Err(error);
        }
        Ok(PendingDeployment {
            scope: Some(scope),
            ready,
        })
    }
}

struct PendingDeployment {
    scope: Option<Arc<DeploymentScope>>,
    ready: oneshot::Receiver<Result<()>>,
}

impl PendingDeployment {
    async fn wait_ready(mut self, driver_terminated: &str) -> Result<DeploymentId> {
        (&mut self.ready)
            .await
            .map_err(|_| VertxError::StartFailed(driver_terminated.into()))??;
        Ok(self
            .scope
            .take()
            .expect("pending deployment owns its scope")
            .deployment)
    }
}

impl Drop for PendingDeployment {
    fn drop(&mut self) {
        if let Some(scope) = &self.scope {
            scope.request_stop();
        }
    }
}

#[derive(Default)]
struct Rollback {
    scopes: Vec<Arc<DeploymentScope>>,
}

impl Rollback {
    fn request_stop(&self) {
        for scope in &self.scopes {
            scope.request_stop();
        }
    }

    async fn wait(&self, grace: Duration) -> Vec<VertxError> {
        self.request_stop();
        let mut errors = Vec::new();
        for scope in &self.scopes {
            if let Err(error) = scope.wait_with_grace(grace).await {
                errors.push(error);
            }
        }
        errors
    }
}

impl Drop for Rollback {
    fn drop(&mut self) {
        self.request_stop();
    }
}
