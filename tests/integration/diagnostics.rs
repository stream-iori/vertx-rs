//! Read-only snapshots, bounded details and lifecycle accounting.

use super::support::{TraceOutput, capture, capture_worker, vx};
use rstest::rstest;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::oneshot;
use vertx_rs::{DiagnosticsOptions, Result, Verticle, Vertx, VertxError, VertxHandle};

#[tokio::test]
async fn diagnostics_are_bounded_and_counts_follow_actual_task_drop() {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .diagnostics(DiagnosticsOptions {
            task_details: true,
            max_task_details: 2,
            ..Default::default()
        })
        .build()
        .unwrap();
    let (ctx, _) = capture(&vx).await;
    let before = vx.diagnostics();
    let ok = ctx.spawn(async { Ok(()) }).unwrap();
    ok.await.unwrap();
    let err = ctx
        .spawn(async { Err::<(), _>(VertxError::Other("task failed".into())) })
        .unwrap();
    assert!(err.await.is_err());
    let panic = ctx
        .spawn(async {
            panic!("task panic");
            #[allow(unreachable_code)]
            Ok(())
        })
        .unwrap();
    assert!(matches!(panic.await, Err(VertxError::Panic(_))));
    let mut handles = Vec::new();
    for _ in 0..4 {
        handles.push(ctx.spawn(futures::future::pending::<Result<()>>()).unwrap());
    }
    let snapshot = vx.diagnostics();
    assert_eq!(snapshot.active_tasks, before.active_tasks + 4);
    assert_eq!(snapshot.tasks.len(), 2);
    assert!(snapshot.task_details_truncated);
    for handle in handles {
        handle.abort();
        assert!(matches!(handle.await, Err(VertxError::TaskCancelled)));
    }
    let snapshot = vx.diagnostics();
    assert_eq!(snapshot.active_tasks, before.active_tasks);
    assert_eq!(snapshot.completed, before.completed + 1);
    assert_eq!(snapshot.failed, before.failed + 2);
    assert_eq!(snapshot.cancelled, before.cancelled + 4);
    vx.close().await.unwrap();
    let snapshot = vx.diagnostics();
    assert_eq!(snapshot.active_tasks, 0);
    assert!(snapshot.tasks.is_empty());
    assert!(snapshot.threads.is_empty());
    assert_eq!(snapshot.active_deployments, 0);
    assert_eq!(snapshot.close_stage, "closed");
}

#[tokio::test]
async fn diagnostics_read_current_poll_without_waiting_for_blocked_eventloop() {
    let output = TraceOutput::default();
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .diagnostics(vertx_rs::DiagnosticsOptions {
            event_loop_poll_threshold: Duration::ZERO,
            ..Default::default()
        })
        .build()
        .unwrap();
    let (ctx, execution) = capture(&vx).await;
    let dispatch = output.dispatch();
    let (entered, ready) = oneshot::channel();
    let (release, gate) = std::sync::mpsc::channel();
    let task = tracing::dispatcher::with_default(&dispatch, || {
        ctx.spawn(async move {
            entered.send(()).ok();
            gate.recv().unwrap();
            Ok(())
        })
    })
    .unwrap();
    ready.await.unwrap();
    let snapshot = vx.diagnostics();
    let warned_before_return = output
        .text()
        .contains("slow synchronous operation returned");
    let operation = snapshot
        .threads
        .iter()
        .find(|t| t.task_id == task.id())
        .unwrap();
    let correct =
        operation.context_id == execution.context_id && operation.operation == "future_poll";
    release.send(()).unwrap();
    task.await.unwrap();
    assert!(correct);
    assert!(!warned_before_return);
    assert!(
        output
            .text()
            .contains("slow synchronous operation returned"),
        "trace: {}",
        output.text()
    );
    assert!(snapshot.tasks.is_empty(), "details must be opt-in");
    vx.close().await.unwrap();
}

#[rstest]
#[case::spawn_local(TaskSubmission::Local)]
#[case::same_event_loop(TaskSubmission::SameEventLoop)]
#[case::cross_thread(TaskSubmission::CrossThread)]
#[case::worker(TaskSubmission::Worker)]
#[tokio::test]
async fn handle_and_diagnostics_use_the_same_task_id(#[case] submission: TaskSubmission) {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .diagnostics(DiagnosticsOptions {
            task_details: true,
            ..Default::default()
        })
        .build()
        .unwrap();
    let (deployment, execution) = if matches!(submission, TaskSubmission::Worker) {
        capture_worker(&vx).await
    } else {
        capture(&vx).await
    };
    let (report, observed) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    let owner = vx.clone();
    let task = async move {
        let thread = std::thread::current().id();
        let operation = owner
            .diagnostics()
            .threads
            .into_iter()
            .find(|operation| operation.thread_id == thread)
            .unwrap();
        report.send(operation).unwrap();
        wait.await.unwrap();
        Ok(())
    };

    let handle = match submission {
        TaskSubmission::Local | TaskSubmission::SameEventLoop => {
            let (send, recv) = oneshot::channel();
            let target = vx.handle();
            deployment
                .spawn(async move {
                    let handle = match submission {
                        TaskSubmission::Local => target.spawn_local(task),
                        _ => target.spawn(task),
                    }?;
                    send.send(handle).map_err(|_| VertxError::ReplyClosed)?;
                    Ok(())
                })
                .unwrap()
                .await
                .unwrap();
            recv.await.unwrap()
        }
        TaskSubmission::CrossThread | TaskSubmission::Worker => deployment.spawn(task).unwrap(),
    };

    let operation = observed.await.unwrap();
    assert_eq!(operation.task_id, handle.id());
    assert_eq!(operation.context_id, execution.context_id);
    assert_eq!(operation.operation, "future_poll");
    assert!(vx.diagnostics().tasks.iter().any(|record| {
        record.task_id == handle.id()
            && record.context_id == execution.context_id
            && record.deployment_id == Some(deployment.id())
    }));
    release.send(()).unwrap();
    handle.await.unwrap();
    vx.close().await.unwrap();
}

enum TaskSubmission {
    Local,
    SameEventLoop,
    CrossThread,
    Worker,
}

#[rstest]
#[case::enabled(true)]
#[case::disabled(false)]
#[tokio::test]
async fn diagnostics_can_be_disabled_and_consumers_do_not_double_decrement(#[case] enabled: bool) {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .diagnostics(vertx_rs::DiagnosticsOptions {
            enabled,
            ..Default::default()
        })
        .build()
        .unwrap();
    let consumer = vx
        .event_bus()
        .unwrap()
        .consumer::<()>("diagnostic-count")
        .unwrap();
    assert_eq!(vx.diagnostics().active_consumers, usize::from(enabled));
    vx.close().await.unwrap();
    drop(consumer);
    let snapshot = vx.diagnostics();
    assert_eq!(snapshot.active_consumers, 0);
    assert_eq!(snapshot.enabled, enabled);
}

#[tokio::test]
async fn diagnostics_distinguish_ordered_wait_worker_queue_and_running_work() {
    use tracing::instrument::WithSubscriber;
    use vertx_rs::{DiagnosticsOptions, TaskStage};
    let output = TraceOutput::default();
    let dispatch = output.dispatch();
    async {
        let vx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(1)
            .diagnostics(DiagnosticsOptions {
                task_details: true,
                blocking_threshold: Duration::ZERO,
                ..Default::default()
            })
            .build()
            .unwrap();
        let (ctx, _) = capture(&vx).await;
        let (entered, ready) = oneshot::channel();
        let (release, gate) = std::sync::mpsc::channel();
        let c = vx.handle();
        let first = ctx
            .spawn(async move {
                c.execute_blocking(move || {
                    entered.send(()).ok();
                    gate.recv().unwrap();
                })
                .await
            })
            .unwrap();
        ready.await.unwrap();
        let c = vx.handle();
        let (submitted, observed) = oneshot::channel();
        let second = ctx
            .spawn(async move {
                submitted.send(()).ok();
                c.execute_blocking(|| ()).await
            })
            .unwrap();
        observed.await.unwrap();
        let c = vx.handle();
        let (submitted, observed) = oneshot::channel();
        let third = ctx
            .spawn(async move {
                submitted.send(()).ok();
                c.execute_blocking_unordered(|| ()).await
            })
            .unwrap();
        observed.await.unwrap();
        // A round trip on this EventLoop follows the submitters' synchronous polls.
        ctx.spawn(async { Ok(()) }).unwrap().await.unwrap();
        let snapshot = vx.diagnostics();
        release.send(()).unwrap();
        first.await.unwrap();
        second.await.unwrap();
        third.await.unwrap();
        assert_eq!(snapshot.blocking_running, 1);
        assert_eq!(snapshot.blocking_waiting, 2);
        for stage in [
            TaskStage::BlockingOrderedWait,
            TaskStage::BlockingQueued,
            TaskStage::BlockingRunning,
        ] {
            assert!(
                snapshot.tasks.iter().any(|t| t.stage == stage),
                "missing {stage:?}: {snapshot:?}"
            );
        }
        vx.close().await.unwrap();
        assert_eq!(vx.diagnostics().blocking_running, 0);
        assert_eq!(vx.diagnostics().blocking_waiting, 0);
    }
    .with_subscriber(dispatch)
    .await;
    let text = output.text();
    assert!(
        text.contains("ordered_wait=") && text.contains("worker_queue_wait="),
        "{text}"
    );
    assert!(
        text.contains("slow synchronous operation returned")
            && text.contains("synchronous_elapsed=")
    );
}

#[rstest]
#[tokio::test]
async fn diagnostics_show_cleanup_stage_while_stop_is_pending(vx: Vertx) {
    struct StopGate {
        entered: Option<oneshot::Sender<()>>,
        release: Option<oneshot::Receiver<()>>,
    }
    impl Verticle for StopGate {
        async fn start(&mut self, _: VertxHandle) -> Result<()> {
            Ok(())
        }
        async fn stop(&mut self, _: VertxHandle) -> Result<()> {
            self.entered.take().unwrap().send(()).ok();
            self.release
                .take()
                .unwrap()
                .await
                .map_err(|_| VertxError::ReplyClosed)
        }
    }
    let (entered, ready) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    let id = vx
        .deploy(|| StopGate {
            entered: Some(entered),
            release: Some(wait),
        })
        .await
        .unwrap();
    let owner = vx.clone();
    let close = tokio::spawn(async move { owner.close().await });
    ready.await.unwrap();
    let snapshot = vx.diagnostics();
    release.send(()).unwrap();
    close.await.unwrap().unwrap();
    assert_eq!(snapshot.close_stage, "deployments");
    assert!(
        snapshot
            .deployments
            .iter()
            .any(|d| d.deployment_id == id.id() && d.stage == "stopping")
    );
    assert!(vx.diagnostics().deployments.is_empty());
}

#[rstest]
#[tokio::test]
async fn diagnostics_keep_detached_blocking_work_until_its_actual_completion(vx: Vertx) {
    let (ctx, _) = capture(&vx).await;
    let (send, ready) = oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let c = vx.handle();
    let task = ctx
        .spawn(async move {
            c.execute_blocking(move || {
                send.send(()).ok();
                wait.recv().unwrap();
            })
            .await
        })
        .unwrap();
    ready.await.unwrap();
    task.abort();
    assert!(matches!(task.await, Err(VertxError::TaskCancelled)));
    let snapshot = vx.diagnostics();
    release.send(()).unwrap();
    vx.close().await.unwrap();
    assert_eq!(snapshot.blocking_running, 1);
    assert_eq!(vx.diagnostics().active_tasks, 0);
}

#[rstest]
#[tokio::test]
async fn diagnostics_mark_destructor_panics_failed_and_invoke_hook_once(vx: Vertx) {
    struct DropPanic;
    impl Future for DropPanic {
        type Output = Result<()>;
        fn poll(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }
    impl Drop for DropPanic {
        fn drop(&mut self) {
            panic!("diagnostic drop panic");
        }
    }
    let (ctx, _) = capture(&vx).await;
    let before = vx.diagnostics().failed;
    let calls = Arc::new(AtomicUsize::new(0));
    let hook = calls.clone();
    ctx.set_exception_handler(move |_| {
        hook.fetch_add(1, Ordering::SeqCst);
    })
    .unwrap();
    let result = ctx.spawn(DropPanic).unwrap().await;
    assert!(matches!(result, Err(VertxError::Panic(_))));
    assert_eq!(vx.diagnostics().failed, before + 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    vx.close().await.unwrap();
}

#[rstest]
#[case::enabled(true)]
#[case::disabled(false)]
#[tokio::test]
async fn queue_details_are_bounded_and_optional(#[case] enabled: bool) {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .diagnostics(vertx_rs::DiagnosticsOptions {
            enabled,
            queue_details: true,
            max_queue_details: 1,
            ..Default::default()
        })
        .build()
        .unwrap();
    let a = vx.event_bus().unwrap().consumer::<()>("a").unwrap();
    let b = vx.event_bus().unwrap().consumer::<()>("b").unwrap();
    vx.event_bus().unwrap().send("a", ()).await.unwrap();
    let snapshot = vx.diagnostics();
    assert_eq!(snapshot.queues.len(), usize::from(enabled));
    assert_eq!(snapshot.queue_details_truncated, enabled);
    assert_eq!(snapshot.queued_messages, usize::from(enabled));
    vx.close().await.unwrap();
    drop((a, b));
    let snapshot = vx.diagnostics();
    assert_eq!(
        (
            snapshot.queued_messages,
            snapshot.reserved_slots,
            snapshot.waiting_senders
        ),
        (0, 0, 0)
    );
    assert!(snapshot.queues.is_empty());
}
