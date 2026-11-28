use crate::context::{Context, TaskResult};
use crate::deployment::DeploymentScope;
use crate::diagnostics::DeploymentStage;
use crate::error::{panic_error, retain_failure};
use crate::{Result, Verticle, VertxError, WorkerVerticle};
use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::task::Poll;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

pub(super) type ReadySender = oneshot::Sender<Result<()>>;

// The drivers share bookkeeping while the compiler checks Send independently.
pub(super) async fn drive_local<V: Verticle>(
    ctx: Context,
    factory: impl FnOnce() -> V,
    ready: ReadySender,
) -> TaskResult<()> {
    let mut lifecycle = Lifecycle::new(&ctx, ready);
    if let Some(mut verticle) = lifecycle.create(factory) {
        let startup = lifecycle.start(|| verticle.start(ctx.vertx_handle())).await;
        let run_stop = startup.runs_stop();
        lifecycle.quiesce_after_start(startup).await;
        if run_stop {
            let stop_ctx = lifecycle.stop_context();
            lifecycle
                .stop(&stop_ctx, || verticle.stop(stop_ctx.vertx_handle()))
                .await;
        }
        lifecycle.destroy(verticle);
        lifecycle.wait_for_stop_work().await;
    } else {
        lifecycle.quiesce().await;
    }
    lifecycle.finish()
}

pub(super) async fn drive_worker<V: WorkerVerticle>(
    ctx: Context,
    factory: impl FnOnce() -> V,
    ready: ReadySender,
) -> TaskResult<()> {
    let mut lifecycle = Lifecycle::new(&ctx, ready);
    if let Some(mut verticle) = lifecycle.create(factory) {
        let startup = lifecycle.start(|| verticle.start(ctx.vertx_handle())).await;
        let run_stop = startup.runs_stop();
        lifecycle.quiesce_after_start(startup).await;
        if run_stop {
            let stop_ctx = lifecycle.stop_context();
            lifecycle
                .stop(&stop_ctx, || verticle.stop(stop_ctx.vertx_handle()))
                .await;
        }
        lifecycle.destroy(verticle);
        lifecycle.wait_for_stop_work().await;
    } else {
        lifecycle.quiesce().await;
    }
    lifecycle.finish()
}

enum CallbackOutcome {
    Returned(Result<()>),
    Panicked(VertxError),
    Cancelled,
}

// The lifecycle driver owns the execution gate. Switch the logical Context
// inside that gate for each operation, including the stop future's destructor.
async fn invoke<F: Future<Output = Result<()>>>(
    ctx: &Context,
    callback: impl FnOnce() -> F,
    cancel: Option<&CancellationToken>,
) -> CallbackOutcome {
    let constructed = {
        let _context = ctx.enter();
        catch_unwind(AssertUnwindSafe(callback))
    };
    let mut future = match constructed {
        Ok(future) => Box::pin(future),
        Err(payload) => return CallbackOutcome::Panicked(panic_error(payload)),
    };
    let poll = poll_fn(|cx| {
        let _context = ctx.enter();
        match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(poll) => poll.map(CallbackOutcome::Returned),
            Err(payload) => Poll::Ready(CallbackOutcome::Panicked(panic_error(payload))),
        }
    });
    let outcome = if let Some(cancel) = cancel {
        tokio::select! { biased;
            _ = cancel.cancelled() => CallbackOutcome::Cancelled,
            outcome = poll => outcome,
        }
    } else {
        poll.await
    };
    let dropped = {
        let _context = ctx.enter();
        catch_unwind(AssertUnwindSafe(|| drop(future)))
    };
    if let Err(payload) = dropped {
        let mut error = match outcome {
            CallbackOutcome::Returned(result) => result,
            CallbackOutcome::Panicked(error) => Err(error),
            CallbackOutcome::Cancelled => Ok(()),
        };
        retain_failure(&mut error, panic_error(payload));
        CallbackOutcome::Panicked(error.expect_err("future destructor failed"))
    } else {
        outcome
    }
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
    scope: &'a DeploymentScope,
    ready: Option<ReadySender>,
    outcome: Result<()>,
    startup_cancellation: Option<VertxError>,
}

impl<'a> Lifecycle<'a> {
    fn new(ctx: &'a Context, ready: ReadySender) -> Self {
        let lifecycle = Self {
            ctx,
            scope: ctx.deployment_scope(),
            ready: Some(ready),
            outcome: Ok(()),
            startup_cancellation: None,
        };
        lifecycle.stage(DeploymentStage::Starting);
        lifecycle
    }

    fn create<V>(&mut self, factory: impl FnOnce() -> V) -> Option<V> {
        match catch_unwind(AssertUnwindSafe(factory)) {
            Ok(verticle) => Some(verticle),
            Err(error) => {
                self.outcome = Err(panic_error(error));
                None
            }
        }
    }

    async fn start<F: Future<Output = Result<()>>>(&self, start: impl FnOnce() -> F) -> Startup {
        let scope = &self.scope.work;
        match invoke(self.ctx, start, Some(&scope.cancel)).await {
            CallbackOutcome::Returned(Ok(())) if scope.try_mark_running() => Startup::Started,
            CallbackOutcome::Returned(Ok(())) | CallbackOutcome::Cancelled => {
                Startup::Cancelled(VertxError::DeploymentTerminated(self.scope.id))
            }
            CallbackOutcome::Returned(Err(error)) => {
                if matches!(error, VertxError::DeploymentTerminated(id) if id == self.scope.id && scope.cancel.is_cancelled())
                    || matches!(error, VertxError::VertxClosed if self.ctx.instance_admission_closed())
                {
                    Startup::Cancelled(error)
                } else {
                    Startup::Failed(error)
                }
            }
            CallbackOutcome::Panicked(error) => Startup::Panicked(error),
        }
    }

    async fn quiesce_after_start(&mut self, startup: Startup) {
        match startup {
            Startup::Started => {
                self.notify_started();
                self.ctx.scope().cancel.cancelled().await;
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
            self.ctx.scope().request_stop();
        }
    }

    async fn quiesce(&self) {
        self.stage(DeploymentStage::Quiescing);
        let scope = self.ctx.scope();
        scope.request_stop();
        scope.unregister_resources();
        scope.drain_tasks(self.ctx.shutdown_grace()).await;
        scope.wait_blocking().await;
    }

    fn stop_context(&self) -> Context {
        self.stage(DeploymentStage::Stopping);
        self.ctx.for_stop()
    }

    async fn stop<F: Future<Output = Result<()>>>(
        &mut self,
        ctx: &Context,
        stop: impl FnOnce() -> F,
    ) {
        match invoke(ctx, stop, None).await {
            CallbackOutcome::Returned(Err(error)) | CallbackOutcome::Panicked(error) => {
                retain_failure(&mut self.outcome, error);
            }
            CallbackOutcome::Returned(Ok(())) => {}
            CallbackOutcome::Cancelled => unreachable!("stop is not cancelled"),
        }
    }

    fn destroy<V>(&mut self, verticle: V) {
        if let Err(error) = catch_unwind(AssertUnwindSafe(|| drop(verticle))) {
            retain_failure(&mut self.outcome, panic_error(error));
        }
    }

    async fn wait_for_stop_work(&self) {
        self.stage(DeploymentStage::Quiescing);
        self.ctx.scope().wait_blocking().await;
    }

    fn finish(self) -> TaskResult<()> {
        let Self {
            ctx,
            scope,
            ready,
            mut outcome,
            startup_cancellation,
        } = self;
        let id = scope.id;
        if let Err(error) = &outcome {
            ctx.report_error(error);
        }
        if let Err(error) = ctx.seal_exception_handler() {
            tracing::error!(deployment = %id, error_kind = error.kind(), %error, "exception handler destructor panicked");
            retain_failure(&mut outcome, error);
        }
        ctx.diagnostics().deployment(id, DeploymentStage::Stopped);
        if let Some(instance) = ctx.instance() {
            instance.forget_deployment(id);
        }
        scope.finish(outcome.clone());
        if let Some(ready) = ready {
            let readiness = match (startup_cancellation.clone(), outcome.clone()) {
                (Some(cancellation), cleanup) => {
                    let mut result = Err(cancellation);
                    if let Err(error) = cleanup {
                        retain_failure(&mut result, error);
                    }
                    result
                }
                (None, Err(error)) => Err(error),
                (None, Ok(())) => Err(VertxError::StartFailed("startup cancelled".into())),
            };
            let _ = ready.send(readiness);
        }
        match (outcome, startup_cancellation) {
            (Ok(()), Some(error)) => TaskResult::Cancelled(error),
            (outcome, _) => TaskResult::Finished(outcome),
        }
    }

    fn stage(&self, stage: DeploymentStage) {
        self.ctx.diagnostics().deployment(self.scope.id, stage);
    }
}
