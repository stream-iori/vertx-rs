use super::{Vertx, VertxInner};
use crate::deployment::DeploymentScope;
use crate::diagnostics::CloseStage;
use crate::{Result, TaskId, VertxError};
use std::sync::Arc;

impl Vertx {
    /// Closes admission and requests instance shutdown without waiting.
    /// This can be called from an EventLoop or Worker of the instance.
    /// Repeated calls share one shutdown attempt. Use [`Self::wait_closed`]
    /// to observe the final result. The first call returns an error if it
    /// cannot schedule the shutdown task.
    pub fn request_close(&self) -> Result<()> {
        self.inner.request_close()
    }

    /// Requests shutdown and waits up to the configured shutdown grace.
    /// After a timeout or cancellation of this waiter, cleanup continues.
    /// Use [`Self::wait_closed`] to observe the final result.
    ///
    /// Success means all Worker and EventLoop threads have exited and been joined.
    /// Calling this from the instance's Context or EventLoop threads returns
    /// [`VertxError::WouldDeadlock`] before requesting shutdown.
    pub async fn close(&self) -> Result<()> {
        self.check_close_waiter()?;
        self.request_close()?;
        let grace = self.inner.lifetime.shutdown_grace;
        tokio::time::timeout(grace, self.inner.wait_closed())
            .await
            .map_err(|_| VertxError::ShutdownTimeout(grace))?
    }

    /// Waits for the final shutdown result, even if a request has yet to arrive.
    /// Success means all Worker and EventLoop threads have exited and been joined.
    /// Waiting in this instance's Context or on any of its EventLoop threads
    /// returns [`VertxError::WouldDeadlock`], including from raw Tokio tasks.
    pub async fn wait_closed(&self) -> Result<()> {
        self.check_close_waiter()?;
        self.inner.wait_closed().await
    }

    fn check_close_waiter(&self) -> Result<()> {
        if self.inner.executors.event_loops.owns_current_thread()
            || self.handle().current_context().is_some()
        {
            return Err(VertxError::WouldDeadlock);
        }
        Ok(())
    }
}

impl VertxInner {
    pub(super) fn request_close(self: &Arc<Self>) -> Result<()> {
        let Some(scopes) = self.begin_close() else {
            return Ok(());
        };
        // Cancellation and task submission can wake code that reenters Vertx.
        let services = &self.services;
        services.diagnostics.close_stage(CloseStage::Deployments);
        services.tcp.close_admission();
        // Existing EventBus reservations observe this root cancellation.
        self.lifetime.scope.request_stop();
        for scope in &scopes {
            scope.work.request_stop();
        }

        let owner = self.clone();
        let loops = &self.executors.event_loops;
        let control_loop = loops.pick();
        if let Err(error) = loops.spawn_pinned(control_loop, TaskId::next(), move || {
            owner.finish_close(scopes)
        }) {
            services.event_bus.close();
            self.close_result.send_replace(Some(Err(error.clone())));
            return Err(error);
        }
        Ok(())
    }

    fn begin_close(&self) -> Option<Vec<Arc<DeploymentScope>>> {
        let deployments = self.deployments.lock().unwrap();
        self.lifetime
            .scope
            .close_instance_admission()
            .then(|| deployments.values().cloned().collect())
    }

    async fn finish_close(self: Arc<Self>, scopes: Vec<Arc<DeploymentScope>>) -> Result<()> {
        self.services.event_bus.close();
        let root = &self.lifetime.scope;
        root.unregister_resources();
        let wait_deployments = async {
            let mut errors = Vec::new();
            for scope in scopes {
                if let Err(error) = scope.wait().await {
                    errors.push(error);
                }
            }
            errors
        };
        let (mut errors, ()) = tokio::join!(
            wait_deployments,
            root.drain_tasks(self.lifetime.shutdown_grace),
        );

        let diagnostics = &self.services.diagnostics;
        diagnostics.close_stage(CloseStage::Blocking);
        root.wait_blocking().await;
        if let Err(error) = self.services.exception_handler.seal() {
            errors.push(error);
        }
        diagnostics.close_stage(CloseStage::WorkerJoin);
        errors.extend(self.executors.workers.shutdown().await);

        diagnostics.close_stage(CloseStage::EventLoopJoin);
        self.executors
            .event_loops
            .finish_shutdown(errors, self.close_result.clone());
        Ok(())
    }

    async fn wait_closed(&self) -> Result<()> {
        let mut result = self.close_result.subscribe();
        loop {
            if let Some(outcome) = result.borrow().clone() {
                return outcome;
            }
            result
                .changed()
                .await
                .map_err(|_| VertxError::VertxClosed)?;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Vertx, VertxError};
    use std::time::Duration;
    use tokio::sync::watch;

    #[tokio::test]
    async fn request_close_records_dispatch_failure_and_clears_routes() {
        let vx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(1)
            .build()
            .unwrap();
        let bus = vx.event_bus().unwrap();
        let _consumer = bus.consumer::<()>("close-dispatch-failure").unwrap();
        assert_eq!(vx.diagnostics().active_consumers, 1);

        let (done, mut event_loops_exited) = watch::channel(None);
        vx.inner
            .executors
            .event_loops
            .finish_shutdown(Vec::new(), done);
        assert!(matches!(vx.request_close(), Err(VertxError::VertxClosed)));
        assert!(vx.is_admission_closed());
        assert_eq!(vx.diagnostics().active_consumers, 0);
        assert_eq!(vx.diagnostics().close_stage, "failed");

        vx.request_close().unwrap();
        let (first, second) = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(vx.wait_closed(), vx.wait_closed())
        })
        .await
        .unwrap();
        assert!(matches!(first, Err(VertxError::VertxClosed)));
        assert!(matches!(second, Err(VertxError::VertxClosed)));

        assert!(matches!(
            event_loops_exited
                .wait_for(Option::is_some)
                .await
                .unwrap()
                .as_ref(),
            Some(Ok(()))
        ));
        assert!(vx.inner.executors.workers.shutdown().await.is_empty());
    }
}
