use super::{Context, Target};
use crate::deployment::TaskRegistration;
use crate::diagnostics::{OperationGuard, TaskRecord, TaskStage};
use crate::error::{panic_error, panic_error_ref};
use crate::{Result, TaskId, VertxError};
use futures::FutureExt;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};
use tracing::{Dispatch, Span};

impl Context {
    pub(super) fn frame(&self, id: TaskId) -> TaskFrame {
        let diagnostics = self.diagnostics();
        let record = diagnostics.task(id, self.id(), self.scope().deployment, TaskStage::Ready);
        let span = if diagnostics.options.enabled {
            let span = tracing::info_span!(
                "context_task", instance_id = diagnostics.instance,
                deployment_id = %self.scope().deployment, context_id = %self.id(), task_id = %record.id
            );
            span.in_scope(|| tracing::debug!(task_id = %record.id, "task submitted"));
            span
        } else {
            Span::none()
        };
        TaskFrame {
            record,
            dispatch: tracing::dispatcher::get_default(Clone::clone),
            span,
        }
    }

    pub(crate) fn wrap<F: Future>(&self, id: TaskId, future: F) -> ContextFuture<F> {
        ContextFuture::new(self.clone(), self.frame(id), future)
    }

    pub(super) fn track_future<F>(
        self,
        frame: TaskFrame,
        registration: TaskRegistration,
        future: F,
    ) -> impl Future<Output = Result<()>> + use<F>
    where
        F: Future<Output = Result<()>>,
    {
        Tracked {
            future: ContextFuture::new(self.clone(), frame, run_managed(self, future)),
            _registration: registration,
        }
    }
}

pub(crate) enum TaskResult<T> {
    Finished(Result<T>),
    Cancelled(VertxError),
}

async fn run_managed(ctx: Context, future: impl Future<Output = Result<()>>) -> TaskResult<()> {
    if ctx.scope().cancel.is_cancelled() {
        return TaskResult::Cancelled(VertxError::DeploymentTerminated(ctx.scope().deployment));
    }
    let result = AssertUnwindSafe(future)
        .catch_unwind()
        .await
        .unwrap_or_else(|error| Err(panic_error(error)));
    if let Err(error) = result {
        if ctx.is_expected_cancellation(&error) {
            let error = if matches!(error, VertxError::VertxClosed) {
                VertxError::DeploymentTerminated(ctx.scope().deployment)
            } else {
                error
            };
            return TaskResult::Cancelled(error);
        }
        ctx.report_error(&error);
        return TaskResult::Finished(Err(error));
    }
    TaskResult::Finished(result)
}

pub(super) struct TaskFrame {
    record: TaskRecord,
    dispatch: Dispatch,
    span: Span,
}

pub(crate) struct ContextFuture<F> {
    ctx: Context,
    future: Option<Pin<Box<F>>>,
    frame: TaskFrame,
}

impl<F> ContextFuture<F> {
    fn new(ctx: Context, frame: TaskFrame, future: F) -> Self {
        Self {
            ctx,
            frame,
            future: Some(Box::pin(future)),
        }
    }
}

impl<F: Future<Output = TaskResult<T>>, T> Future for ContextFuture<F> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let Self {
            ctx,
            future,
            frame:
                TaskFrame {
                    record,
                    dispatch,
                    span,
                },
        } = self.get_mut();
        with_execution(ctx, dispatch, span, || {
            record.stage(TaskStage::Polling);
            if ctx.diagnostics().options.enabled {
                tracing::trace!(
                    thread_id = ?std::thread::current().id(), event_loop_id = ?ctx.event_loop_id(),
                    "task poll"
                );
            }
            let _operation = operation_guard(ctx, record, "future_poll");
            let waker = record.waker(cx.waker());
            let future = future.as_mut().expect("poll after future drop").as_mut();
            let result = catch_unwind(AssertUnwindSafe(|| {
                future.poll(&mut TaskContext::from_waker(&waker))
            }))
            .unwrap_or_else(|payload| {
                // Poll can also drop an awaited future that has just completed.
                let error = panic_error(payload);
                ctx.report_error(&error);
                Poll::Ready(TaskResult::Finished(Err(error)))
            });
            match result {
                Poll::Ready(outcome) => {
                    let (result, cancelled) = match outcome {
                        TaskResult::Finished(result) => (result, false),
                        TaskResult::Cancelled(error) => (Err(error), true),
                    };
                    record.result(&result, cancelled);
                    Poll::Ready(result)
                }
                Poll::Pending => {
                    record.pending();
                    Poll::Pending
                }
            }
        })
    }
}

impl<F> Drop for ContextFuture<F> {
    fn drop(&mut self) {
        let Self {
            ctx,
            future,
            frame:
                TaskFrame {
                    record,
                    dispatch,
                    span,
                },
        } = self;
        let result = with_execution(ctx, dispatch, span, || {
            let _operation = operation_guard(ctx, record, "future_drop");
            let result = catch_unwind(AssertUnwindSafe(|| drop(future.take())));
            if let Err(payload) = &result {
                ctx.report_error(&panic_error_ref(payload.as_ref()));
                record.failed();
            }
            record.finish();
            result
        });
        // Restore the guards normally before resuming a user destructor panic.
        if let Err(payload) = result {
            resume_unwind(payload);
        }
    }
}

fn with_execution<T>(
    ctx: &Context,
    dispatch: &Dispatch,
    span: &Span,
    run: impl FnOnce() -> T,
) -> T {
    let _execution = match &ctx.data.target {
        Target::EventLoop(id) => {
            assert!(
                ctx.data.executors.event_loops.check_thread(*id),
                "EventLoop affinity violated"
            );
            None
        }
        Target::Worker { execution_gate, .. } => Some(
            execution_gate
                .lock()
                .expect("Worker execution gate poisoned by an internal failure"),
        ),
    };
    let _context = ctx.enter();
    let _dispatch = tracing::dispatcher::set_default(dispatch);
    let _span = span.enter();
    run()
}

fn operation_guard(ctx: &Context, record: &TaskRecord, operation: &'static str) -> OperationGuard {
    let diagnostics = ctx.diagnostics();
    let event_loop = ctx.event_loop_id();
    let threshold = if event_loop.is_some() {
        diagnostics.options.event_loop_poll_threshold
    } else {
        diagnostics.options.worker_poll_threshold
    };
    diagnostics.operation(record, event_loop, operation, threshold)
}

struct Tracked<F> {
    // Fields drop in order, including when the future's destructor unwinds.
    future: ContextFuture<F>,
    _registration: TaskRegistration,
}

impl<F: Future<Output = TaskResult<()>>> Future for Tracked<F> {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().future).poll(cx)
    }
}
