use super::Vertx;
use super::lifecycle::{ReadySender, drive_local, drive_worker};
use crate::context::Context;
use crate::deployment::DeploymentScope;
use crate::diagnostics::DeploymentStage;
use crate::{
    Deployment, DeploymentId, DeploymentOptions, Result, TaskId, Verticle, VertxError,
    WorkerVerticle,
};
use std::mem::DropGuard;
use std::sync::Arc;
use tokio::sync::oneshot;

impl Vertx {
    /// Constructs a non-Send verticle on its assigned EventLoop and awaits start.
    /// Factory, lifecycle and managed work stay on that OS thread. Dropping the
    /// waiter requests background cleanup. Successful start leaves the deployment
    /// running until explicitly stopped; startup and cleanup failures are retained.
    pub async fn deploy<F, V>(&self, factory: F) -> Result<Deployment>
    where
        F: FnOnce() -> V + Send + 'static,
        V: Verticle,
    {
        let loops = &self.inner.executors.event_loops;
        let pending = self.submit_deployment(|scope, ready| {
            let loop_id = loops.pick();
            let ctx = Context::new_event_loop(loop_id, &self.inner, Some(scope));
            let deployment = ctx.deployment().expect("deployment context");
            let task_id = TaskId::next();
            loops.spawn_pinned(loop_id, task_id, move || {
                ctx.wrap(task_id, drive_local(ctx.clone(), factory, ready))
            })?;
            Ok(deployment)
        })?;
        pending.wait_ready("lifecycle driver terminated").await
    }

    /// Constructs a Send Worker verticle and awaits its asynchronous start.
    /// Same-Context polls are serialized but may migrate across Worker threads.
    /// Dropping the waiter requests background cleanup, as with [`Self::deploy`].
    pub async fn deploy_worker<F, V>(&self, factory: F) -> Result<Deployment>
    where
        F: FnOnce() -> V + Send + 'static,
        V: WorkerVerticle,
    {
        let pending = self.submit_deployment(|scope, ready| {
            let ctx = Context::new_worker(&self.inner, scope);
            let deployment = ctx.deployment().expect("deployment context");
            let task_id = TaskId::next();
            let future = ctx.wrap(task_id, drive_worker(ctx.clone(), factory, ready));
            ctx.spawn_worker(task_id, future)?;
            Ok(deployment)
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
    ) -> Result<Vec<Deployment>>
    where
        F: Fn(Arc<C>) -> V + Send + Sync + 'static,
        V: Verticle,
        C: Send + Sync + 'static,
    {
        let factory = Arc::new(factory);
        let config = options.shared_config();
        let mut rollback = DropGuard::new(Vec::<Deployment>::new(), |deployments| {
            request_stop_all(&deployments);
        });
        for _ in 0..options.instances_count().get() {
            let (factory, config) = (factory.clone(), config.clone());
            match self.deploy(move || factory(config)).await {
                Ok(deployment) => rollback.push(deployment),
                Err(cause) => {
                    request_stop_all(&rollback);
                    let mut failures = Vec::new();
                    for deployment in DropGuard::dismiss(rollback) {
                        if let Err(error) = deployment.stop().await {
                            failures.push(error);
                        }
                    }
                    return Err(VertxError::DeploymentFailed {
                        cause: Box::new(cause),
                        rollback: failures,
                    });
                }
            }
        }
        Ok(DropGuard::dismiss(rollback))
    }

    fn submit_deployment(
        &self,
        submit: impl FnOnce(Arc<DeploymentScope>, ReadySender) -> Result<Deployment>,
    ) -> Result<PendingDeployment> {
        let diagnostics = &self.inner.services.diagnostics;
        let id = DeploymentId::next();
        let scope = Arc::new(DeploymentScope::new(id, &self.inner.lifetime.scope));
        let (started, ready) = oneshot::channel();
        {
            let mut deployments = self.inner.deployments.lock().unwrap();
            self.inner.ensure_open()?;
            diagnostics.record_deployment(id, DeploymentStage::Queued);
            deployments.insert(id, scope.clone());
        }
        diagnostics.trace_deployment(id, DeploymentStage::Queued);
        // Close includes the scope in its snapshot even if submission is still in progress.
        match submit(scope.clone(), started) {
            Ok(deployment) => Ok(PendingDeployment {
                deployment: Some(deployment),
                ready,
            }),
            Err(error) => {
                diagnostics.deployment(id, DeploymentStage::Stopped);
                self.inner.forget_deployment(id);
                scope.finish(Err(error.clone()));
                Err(error)
            }
        }
    }
}

struct PendingDeployment {
    deployment: Option<Deployment>,
    ready: oneshot::Receiver<Result<()>>,
}

impl PendingDeployment {
    async fn wait_ready(mut self, driver_terminated: &str) -> Result<Deployment> {
        (&mut self.ready)
            .await
            .map_err(|_| VertxError::StartFailed(driver_terminated.into()))??;
        Ok(self
            .deployment
            .take()
            .expect("pending deployment owns its handle"))
    }
}

impl Drop for PendingDeployment {
    fn drop(&mut self) {
        if let Some(deployment) = &self.deployment {
            deployment.request_stop();
        }
    }
}

fn request_stop_all(deployments: &[Deployment]) {
    for deployment in deployments {
        deployment.request_stop();
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
        vertx.request_close().unwrap();
        let closed = vertx.wait_closed();
        tokio::pin!(closed);
        assert!(futures::poll!(&mut closed).is_pending());
        assert_eq!(vertx.diagnostics().active_deployments, 1);
        fail.send(()).unwrap();
        assert!(matches!(
            submission.join().unwrap(),
            Err(VertxError::Other(_))
        ));
        assert!(
            matches!(closed.await, Err(VertxError::CleanupFailed(errors))
                if matches!(errors.as_slice(), [VertxError::Other(error)] if error == "executor rejected submission")
            )
        );
        assert_eq!(vertx.diagnostics().active_deployments, 0);
    }
}
