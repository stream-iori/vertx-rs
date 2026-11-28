use super::Context;
use crate::diagnostics::{TaskRecord, TaskStage};
use crate::error::panic_error;
use crate::{Result, TaskId, VertxError};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::Instant;
use tokio::sync::OwnedMutexGuard;
use tokio_util::task::task_tracker::TaskTrackerToken;

enum BlockingOrder {
    Ordered,
    Unordered,
}

impl Context {
    /// Runs a Send closure on the shared Worker pool, FIFO per source Context.
    /// The logical Context is retained. Queued work is skipped after cancellation;
    /// started work finishes even if the waiter is dropped. Cleanup blocking is
    /// admitted only during the actual stop callback.
    pub async fn execute_blocking<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.execute_blocking_with(f, BlockingOrder::Ordered).await
    }
    /// Executes blocking work without the per-Context FIFO lock.
    ///
    /// Operations may overlap up to the shared Worker pool limit. The source
    /// logical Context is retained, but execution takes place on a Worker.
    /// Queued work is skipped on scope cancellation; started work cannot be
    /// interrupted. Dropping the waiter does not retract submitted work.
    pub async fn execute_blocking_unordered<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.execute_blocking_with(f, BlockingOrder::Unordered)
            .await
    }

    async fn execute_blocking_with<F, R>(&self, f: F, order: BlockingOrder) -> Result<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let mut task = BlockingTask::admit(self)?;
        let order = task.wait_for_order(order).await?;
        task.record.stage(TaskStage::BlockingQueued);
        let queued_at = Instant::now();
        let result = self
            .data
            .executors
            .workers
            .spawn_blocking(move || task.run(f, order, queued_at))?;
        result
            .await
            .map_err(|_| VertxError::Join("blocking result channel closed".into()))?
    }
}

struct BlockingTask {
    ctx: Context,
    record: TaskRecord,
    dispatch: tracing::Dispatch,
    span: tracing::Span,
    scope_token: TaskTrackerToken,
    instance_token: TaskTrackerToken,
}

impl BlockingTask {
    fn admit(ctx: &Context) -> Result<Self> {
        let in_stop_callback = ctx.stopping
            && Context::current().is_some_and(|current| current.stopping && current.same(ctx));
        // Track queued work before the first await.
        let scope_token = ctx
            .scope()
            .admit_blocking(&ctx.data.lifetime.cancel, in_stop_callback)?;
        let instance_token = ctx.data.lifetime.blocking.token();
        let diagnostics = ctx.diagnostics();
        let record = diagnostics.task(
            TaskId::next(),
            ctx.id(),
            ctx.scope().deployment,
            TaskStage::BlockingOrderedWait,
        );
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        let span = if diagnostics.options.enabled {
            tracing::info_span!(
                "blocking_task", instance_id = diagnostics.instance,
                deployment_id = %ctx.scope().deployment, context_id = %ctx.id(), task_id = %record.id
            )
        } else {
            tracing::Span::none()
        };
        Ok(Self {
            ctx: ctx.clone(),
            record,
            dispatch,
            span,
            scope_token,
            instance_token,
        })
    }

    async fn wait_for_order(&self, ordering: BlockingOrder) -> Result<Option<OwnedMutexGuard<()>>> {
        let ctx = &self.ctx;
        let started = Instant::now();
        let ordered = matches!(ordering, BlockingOrder::Ordered);
        let order = if ordered {
            Some(tokio::select! { biased;
                _ = ctx.scope().cancel.cancelled(), if !ctx.stopping => {
                    return Err(VertxError::DeploymentTerminated(ctx.scope().deployment));
                }
                guard = ctx.data.blocking_order_lock.clone().lock_owned() => guard,
            })
        } else {
            None
        };
        if ctx.diagnostics().options.enabled {
            tracing::debug!(
                task_id = %self.record.id, ordered, ordered_wait = ?started.elapsed(),
                "blocking order admitted"
            );
        }
        Ok(order)
    }

    fn run<R>(
        self,
        f: impl FnOnce() -> R,
        order: Option<OwnedMutexGuard<()>>,
        queued_at: Instant,
    ) -> Result<R> {
        let Self {
            ctx,
            mut record,
            dispatch,
            span,
            scope_token,
            instance_token,
        } = self;
        let _dispatch = tracing::dispatcher::set_default(&dispatch);
        let _span = span.enter();
        let diagnostics = ctx.diagnostics();
        if diagnostics.options.enabled {
            tracing::debug!(task_id = %record.id, worker_queue_wait = ?queued_at.elapsed(), "blocking dequeued");
        }
        record.stage(TaskStage::BlockingRunning);
        let operation = diagnostics.operation(
            &record,
            None,
            "blocking",
            diagnostics.options.blocking_threshold,
        );

        // Keep tracking and FIFO ownership until the closure and diagnostics are finished.
        let _tracking_and_order = (scope_token, instance_token, order);
        let mut blocking_context = ctx.clone();
        blocking_context.stopping = false;
        let _context = blocking_context.enter();
        let result = call_blocking(&ctx, f);
        let cancelled = result
            .as_ref()
            .is_err_and(|error| ctx.is_expected_cancellation(error));
        record.result(&result, cancelled);
        drop(operation);
        drop(record);
        result
    }
}

fn call_blocking<R>(ctx: &Context, f: impl FnOnce() -> R) -> Result<R> {
    catch_unwind(AssertUnwindSafe(|| {
        if !ctx.stopping && ctx.scope().cancel.is_cancelled() {
            // Drop skipped work under the same Context and panic boundary.
            drop(f);
            Err(VertxError::DeploymentTerminated(ctx.scope().deployment))
        } else {
            Ok(f())
        }
    }))
    .unwrap_or_else(|error| Err(panic_error(error)))
}
