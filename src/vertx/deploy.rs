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
        let loops = &self.inner.executors.event_loops;
        let pending = self.submit_deployment(|scope, ready| {
            let loop_id = loops.pick();
            let ctx = Context::new_event_loop(loop_id, &self.inner, scope);
            loops.spawn_pinned(loop_id, move || {
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
        let pending = self.submit_deployment(|scope, ready| {
            let ctx = Context::new_worker(&self.inner, scope);
            let future = ctx.wrap(drive_worker(ctx.clone(), factory, ready));
            ctx.spawn_worker(future)
        })?;
        pending
            .wait_ready("worker lifecycle driver terminated")
            .await
    }

    /// Deploys instances sequentially with shared typed configuration.
    /// Failure rolls back prior instances and retains rollback errors. Dropping
    /// the waiter requests cleanup of current and prior instances.
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
        for _ in 0..options.instances_count().get() {
            let (factory, config) = (factory.clone(), config.clone());
            match self.deploy(move |ctx| factory(ctx, config)).await {
                Ok(id) => {
                    if let Some(scope) = self.deployment_scope(id) {
                        rollback.scopes.push(scope);
                    }
                    ids.push(id);
                }
                Err(cause) => {
                    let failures = rollback.wait(self.inner.lifetime.shutdown_grace).await;
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
            .is_some_and(|ctx| ctx.deployment_id() == id && self.owns_context(&ctx))
        {
            return Err(VertxError::WouldDeadlock);
        }
        let scope = self
            .deployment_scope(id)
            .ok_or(VertxError::DeploymentNotFound(id))?;
        scope.request_stop();
        let result = scope
            .wait_with_grace(self.inner.lifetime.shutdown_grace)
            .await;
        if matches!(result, Err(VertxError::ShutdownTimeout(_))) {
            tracing::warn!(deployment = %id, "shutdown timed out; cleanup continues");
        }
        result
    }

    fn deployment_scope(&self, id: DeploymentId) -> Option<Arc<DeploymentScope>> {
        self.inner
            .deployments
            .lock()
            .unwrap()
            .scopes
            .get(&id)
            .cloned()
    }

    fn submit_deployment(
        &self,
        submit: impl FnOnce(Arc<DeploymentScope>, ReadySender) -> Result<TaskHandle<()>>,
    ) -> Result<PendingDeployment> {
        let diagnostics = &self.inner.services.diagnostics;
        let id = DeploymentId::next();
        let scope = Arc::new(DeploymentScope::new(
            id,
            self.inner.lifetime.cancel.child_token(),
        ));
        let (started, ready) = oneshot::channel();
        {
            let mut deployments = self.inner.deployments.lock().unwrap();
            if deployments.closing {
                return Err(VertxError::VertxClosed);
            }
            diagnostics.record_deployment(id, DeploymentStage::Queued);
            deployments.scopes.insert(id, scope.clone());
        }
        diagnostics.trace_deployment(id, DeploymentStage::Queued);
        // Close includes the scope in its snapshot even if submission is still in progress.
        if let Err(error) = submit(scope.clone(), started) {
            diagnostics.deployment(id, DeploymentStage::Stopped);
            self.inner.forget_deployment(id);
            scope.finish(Err(error.clone()));
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

#[cfg(test)]
mod tests {
    use crate::{Vertx, VertxError};
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn close_waits_for_a_registered_submission_to_fail() {
        let vertx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(1)
            .build()
            .unwrap();
        let (registered, ready) = oneshot::channel();
        let (fail, release) = std::sync::mpsc::channel();
        let submitter = vertx.clone();
        let submission = std::thread::spawn(move || {
            submitter.submit_deployment(|_, _| {
                registered.send(()).unwrap();
                release.recv().unwrap();
                Err(VertxError::Other("executor rejected submission".into()))
            })
        });
        ready.await.unwrap();
        let close = vertx.close();
        tokio::pin!(close);
        assert!(futures::poll!(&mut close).is_pending());
        assert_eq!(vertx.diagnostics().active_deployments, 1);
        fail.send(()).unwrap();
        assert!(matches!(
            submission.join().unwrap(),
            Err(VertxError::Other(_))
        ));
        assert!(matches!(close.await, Err(VertxError::CleanupFailed(errors))
            if matches!(errors.as_slice(), [VertxError::Other(error)] if error == "executor rejected submission")
        ));
        assert_eq!(vertx.diagnostics().active_deployments, 0);
    }
}
