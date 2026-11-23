use super::Context;
use crate::diagnostics::{OperationGuard, TaskRecord, TaskStage};
use crate::error::{panic_error, panic_error_ref};
use crate::{Result, TaskId, VertxError};
use futures::FutureExt;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};
use tokio_util::task::task_tracker::TaskTrackerToken;
use tracing::{Dispatch, Span};

impl Context {
    pub(crate) fn wrap<F: Future>(&self, future: F) -> ContextFuture<F> {
        let diagnostics = &self.inner.diagnostics;
        let record = diagnostics.task(self.id(), self.scope.deployment, TaskStage::Ready);
        let span = if diagnostics.options.enabled {
            let span = tracing::info_span!(
                "context_task", instance_id = diagnostics.instance,
                deployment_id = %self.scope.deployment, context_id = %self.id(), task_id = %record.id
            );
            span.in_scope(|| tracing::debug!(task_id = %record.id, "task submitted"));
            span
        } else {
            Span::none()
        };
        ContextFuture {
            ctx: self.clone(),
            future: Some(Box::pin(future)),
            record,
            dispatch: tracing::dispatcher::get_default(Clone::clone),
            span,
        }
    }

    // The lifecycle driver already owns the execution gate and tracing guards.
    pub(crate) fn scope_future<F: Future>(&self, future: F) -> impl Future<Output = F::Output> {
        let ctx = self.clone();
        let mut future = Box::pin(future);
        futures::future::poll_fn(move |cx| {
            let _context = ctx.enter();
            future.as_mut().poll(cx)
        })
    }

    // The caller holds deployment admission through scheduling and registration.
    pub(super) fn track_future<F>(
        &self,
        future: F,
    ) -> (impl Future<Output = Result<()>> + use<F>, TaskId)
    where
        F: Future<Output = Result<()>>,
    {
        let future = self.wrap(run_managed(self.clone(), future));
        let id = future.record.id;
        let tracked = Tracked {
            future,
            _token: self.scope.task_token(),
        };
        (tracked, id)
    }
}

async fn run_managed(ctx: Context, future: impl Future<Output = Result<()>>) -> Result<()> {
    if ctx.scope.cancel.is_cancelled() {
        return Err(VertxError::DeploymentTerminated(ctx.scope.deployment));
    }
    let result = AssertUnwindSafe(future)
        .catch_unwind()
        .await
        .unwrap_or_else(|error| Err(panic_error(error)));
    if let Err(error) = &result {
        if !ctx.is_expected_cancellation(error) {
            ctx.report_error(error);
        } else if matches!(error, VertxError::VertxClosed) {
            return Err(VertxError::DeploymentTerminated(ctx.scope.deployment));
        }
    }
    result
}

pub(crate) struct ContextFuture<F> {
    ctx: Context,
    future: Option<Pin<Box<F>>>,
    record: TaskRecord,
    dispatch: Dispatch,
    span: Span,
}

impl<F: Future<Output = Result<T>>, T> Future for ContextFuture<F> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let Self {
            ctx,
            future,
            record,
            dispatch,
            span,
        } = self.get_mut();
        if let Some(id) = ctx.event_loop_id() {
            assert!(
                ctx.inner.event_loops.check_thread(id),
                "EventLoop affinity violated"
            );
        }
        with_execution(ctx, dispatch, span, || {
            record.stage(TaskStage::Polling);
            if ctx.inner.diagnostics.options.enabled {
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
                Poll::Ready(Err(error))
            });
            match &result {
                Poll::Ready(result) => record.result(result),
                Poll::Pending => record.pending(),
            }
            result
        })
    }
}

impl<F> Drop for ContextFuture<F> {
    fn drop(&mut self) {
        let Self {
            ctx,
            future,
            record,
            dispatch,
            span,
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
    let _execution = ctx
        .data
        .execution
        .lock()
        .expect("Context execution gate poisoned by an internal failure");
    let _context = ctx.enter();
    let _dispatch = tracing::dispatcher::set_default(dispatch);
    let _span = span.enter();
    run()
}

fn operation_guard(ctx: &Context, record: &TaskRecord, operation: &'static str) -> OperationGuard {
    let diagnostics = &ctx.inner.diagnostics;
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
    _token: TaskTrackerToken,
}

impl<F: Future<Output = Result<()>>> Future for Tracked<F> {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().future).poll(cx)
    }
}
