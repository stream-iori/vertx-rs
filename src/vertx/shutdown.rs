use super::{Vertx, VertxInner};
use crate::deployment::DeploymentScope;
use crate::diagnostics::CloseStage;
use crate::{Context, Result, TaskId, VertxError};
use std::sync::Arc;

impl Vertx {
    /// Idempotent asynchronous shutdown. Cancellation of this waiter does not
    /// cancel cleanup. A timeout leaves the instance closed to new work.
    ///
    /// Success means all Worker and EventLoop threads have exited and been joined.
    /// Repeated/concurrent calls observe the same cleanup; a later call may wait
    /// again after timeout. Waiting in this instance's Context or on any of its
    /// EventLoop threads returns `WouldDeadlock`, including from raw Tokio tasks.
    pub async fn close(&self) -> Result<()> {
        if self.inner.executors.event_loops.owns_current_thread()
            || Context::current().is_some_and(|ctx| self.owns_context(&ctx))
        {
            return Err(VertxError::WouldDeadlock);
        }
        self.start_close()?;
        let grace = self.inner.lifetime.shutdown_grace;
        tokio::time::timeout(grace, self.inner.wait_closed())
            .await
            .map_err(|_| VertxError::ShutdownTimeout(grace))?
    }

    fn start_close(&self) -> Result<()> {
        let Some(scopes) = self.inner.begin_close() else {
            return Ok(());
        };
        // Cancellation and task submission can wake code that reenters Vertx.
        let services = &self.inner.services;
        services.diagnostics.close_stage(CloseStage::Deployments);
        services.tcp.close_admission();
        self.inner.lifetime.cancel.cancel();
        services.event_bus.close();
        for scope in &scopes {
            scope.request_stop();
        }
        self.inner.lifetime.blocking.close();

        let owner = self.inner.clone();
        let loops = &self.inner.executors.event_loops;
        let control_loop = loops.pick();
        if let Err(error) = loops.spawn_pinned(control_loop, TaskId::next(), move || {
            owner.finish_close(scopes)
        }) {
            self.inner
                .close_result
                .send_replace(Some(Err(error.clone())));
            return Err(error);
        }
        Ok(())
    }
}

impl VertxInner {
    fn begin_close(&self) -> Option<Vec<Arc<DeploymentScope>>> {
        let mut deployments = self.deployments.lock().unwrap();
        if deployments.closing {
            return None;
        }
        deployments.closing = true;
        Some(deployments.scopes.values().cloned().collect())
    }

    async fn finish_close(self: Arc<Self>, scopes: Vec<Arc<DeploymentScope>>) -> Result<()> {
        let mut errors = Vec::new();
        for scope in scopes {
            if let Err(error) = scope.wait().await {
                errors.push(error);
            }
        }

        let diagnostics = &self.services.diagnostics;
        diagnostics.close_stage(CloseStage::Blocking);
        self.lifetime.blocking.wait().await;
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
