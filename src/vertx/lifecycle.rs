use crate::diagnostics::DeploymentStage;
use crate::error::{panic_error, retain_failure};
use crate::{Context, Result, Verticle, VertxError, WorkerVerticle};
use futures::FutureExt;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use tokio::sync::oneshot;

pub(super) type ReadySender = oneshot::Sender<Result<()>>;

// The drivers share bookkeeping while the compiler checks Send independently.
pub(super) async fn drive_local<V: Verticle>(
    ctx: Context,
    factory: impl FnOnce(Context) -> V,
    ready: ReadySender,
) -> Result<()> {
    let mut lifecycle = Lifecycle::new(&ctx, ready);
    if let Some(mut verticle) = lifecycle.create(factory) {
        let startup = lifecycle.start(verticle.start(ctx.clone())).await;
        let run_stop = startup.runs_stop();
        lifecycle.quiesce_after_start(startup).await;
        if run_stop {
            let stop_ctx = lifecycle.stop_context();
            lifecycle
                .stop(&stop_ctx, verticle.stop(stop_ctx.clone()))
                .await;
        }
        lifecycle.destroy(verticle);
        lifecycle.wait_for_stop_work().await;
    } else {
        lifecycle.quiesce().await;
    }
    lifecycle.finish();
    Ok(())
}

pub(super) async fn drive_worker<V: WorkerVerticle>(
    ctx: Context,
    factory: impl FnOnce(Context) -> V,
    ready: ReadySender,
) -> Result<()> {
    let mut lifecycle = Lifecycle::new(&ctx, ready);
    if let Some(mut verticle) = lifecycle.create(factory) {
        let startup = lifecycle.start(verticle.start(ctx.clone())).await;
        let run_stop = startup.runs_stop();
        lifecycle.quiesce_after_start(startup).await;
        if run_stop {
            let stop_ctx = lifecycle.stop_context();
            lifecycle
                .stop(&stop_ctx, verticle.stop(stop_ctx.clone()))
                .await;
        }
        lifecycle.destroy(verticle);
        lifecycle.wait_for_stop_work().await;
    } else {
        lifecycle.quiesce().await;
    }
    lifecycle.finish();
    Ok(())
}

enum Startup {
    Started,
    Cancelled(VertxError),
    Failed(VertxError),
    Panicked(VertxError),
}

impl Startup {
    fn runs_stop(&self) -> bool {
        match self {
            Self::Started | Self::Cancelled(_) | Self::Failed(_) => true,
            Self::Panicked(_) => false,
        }
    }
}

struct Lifecycle<'a> {
    ctx: &'a Context,
    ready: Option<ReadySender>,
    outcome: Result<()>,
    startup_cancellation: Option<VertxError>,
}

impl<'a> Lifecycle<'a> {
    fn new(ctx: &'a Context, ready: ReadySender) -> Self {
        let lifecycle = Self {
            ctx,
            ready: Some(ready),
            outcome: Ok(()),
            startup_cancellation: None,
        };
        lifecycle.stage(DeploymentStage::Starting);
        lifecycle
    }

    fn create<V>(&mut self, factory: impl FnOnce(Context) -> V) -> Option<V> {
        match catch_unwind(AssertUnwindSafe(|| factory(self.ctx.clone()))) {
            Ok(verticle) => Some(verticle),
            Err(error) => {
                self.outcome = Err(panic_error(error));
                None
            }
        }
    }

    async fn start(&self, start: impl Future<Output = Result<()>>) -> Startup {
        let scope = &self.ctx.scope;
        let cancelled = Startup::Cancelled(VertxError::DeploymentTerminated(scope.deployment));
        let result = tokio::select! { biased;
            _ = scope.cancel.cancelled() => return cancelled,
            result = AssertUnwindSafe(start).catch_unwind() => result,
        };
        match result {
            Ok(Ok(())) if scope.running() => Startup::Started,
            Ok(Ok(())) => cancelled,
            Ok(Err(error @ VertxError::DeploymentTerminated(_))) => Startup::Cancelled(error),
            Ok(Err(error)) => Startup::Failed(error),
            Err(error) => Startup::Panicked(panic_error(error)),
        }
    }

    async fn quiesce_after_start(&mut self, startup: Startup) {
        match startup {
            Startup::Started => {
                self.notify_started();
                self.ctx.scope.cancel.cancelled().await;
            }
            Startup::Cancelled(error) => {
                // Report non-readiness to the deploy waiter and allow orderly close.
                self.startup_cancellation = Some(error);
            }
            Startup::Failed(error) | Startup::Panicked(error) => {
                self.outcome = Err(error);
            }
        }
        self.quiesce().await;
    }

    fn notify_started(&mut self) {
        self.stage(DeploymentStage::Running);
        let ready = self.ready.take().expect("readiness sent only once");
        if ready.send(Ok(())).is_err() {
            self.ctx.scope.request_stop();
        }
    }

    async fn quiesce(&self) {
        self.stage(DeploymentStage::Quiescing);
        let scope = &self.ctx.scope;
        scope.request_stop();
        self.ctx
            .inner
            .event_bus
            .unregister_deployment(scope.deployment);
        self.ctx.inner.tcp.unregister_deployment(scope.deployment);
        scope.drain_tasks(self.ctx.inner.shutdown_grace).await;
        scope.wait_blocking().await;
    }

    fn stop_context(&self) -> Context {
        self.stage(DeploymentStage::Stopping);
        self.ctx.for_stop()
    }

    async fn stop(&mut self, ctx: &Context, stop: impl Future<Output = Result<()>>) {
        let result = AssertUnwindSafe(ctx.scope_future(stop))
            .catch_unwind()
            .await
            .unwrap_or_else(|error| Err(panic_error(error)));
        if let Err(error) = result {
            retain_failure(&mut self.outcome, error);
        }
    }

    fn destroy<V>(&mut self, verticle: V) {
        if let Err(error) = catch_unwind(AssertUnwindSafe(|| drop(verticle))) {
            retain_failure(&mut self.outcome, panic_error(error));
        }
    }

    async fn wait_for_stop_work(&self) {
        self.stage(DeploymentStage::Quiescing);
        self.ctx.scope.wait_blocking().await;
    }

    fn finish(self) {
        let Self {
            ctx,
            ready,
            outcome,
            startup_cancellation,
        } = self;
        let id = ctx.scope.deployment;
        if let Err(error) = &outcome {
            ctx.report_error(error);
        }
        ctx.clear_exception_handler();
        ctx.scope.finish(outcome.clone());
        if let Some(ready) = ready {
            let error = outcome
                .err()
                .or(startup_cancellation)
                .unwrap_or_else(|| VertxError::StartFailed("startup cancelled".into()));
            let _ = ready.send(Err(error));
        }
        if let Some(owner) = ctx.inner.vertx.upgrade() {
            owner.deployments.lock().unwrap().remove(&id);
        }
        ctx.inner
            .diagnostics
            .deployment(id, DeploymentStage::Stopped);
    }

    fn stage(&self, stage: DeploymentStage) {
        self.ctx
            .inner
            .diagnostics
            .deployment(self.ctx.scope.deployment, stage);
    }
}
