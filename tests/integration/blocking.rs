//! Shared fixed pool, ordered admission and non-preemptible work.

use super::support::{WorkerDropProbe, capture, in_test_subprocess, local, vx};
use rstest::rstest;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use vertx_rs::{ExecutionInfo, ExecutorThread, ThreadingModel, Vertx, VertxError};

#[tokio::test]
async fn ordered_blocking_reentry_rejects_waiting_for_its_own_fifo_slot() {
    if !in_test_subprocess(
        "blocking::ordered_blocking_reentry_rejects_waiting_for_its_own_fifo_slot",
    ) {
        return;
    }
    let vertx = vx();
    let handle = vertx.handle();
    let result = vertx
        .execute_blocking(move || {
            futures::executor::block_on(handle.execute_blocking(|| panic!("nested job ran")))
        })
        .await
        .unwrap();
    assert!(matches!(result, Err(VertxError::WouldDeadlock)));
    vertx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn ordered_blocking_created_in_a_closure_can_be_polled_later(vx: Vertx) {
    let handle = vx.handle();
    let (context_id, deferred) = vx
        .execute_blocking(move || {
            let context_id = ExecutionInfo::current().unwrap().context_id;
            let deferred = handle.execute_blocking(|| ExecutionInfo::current().unwrap().context_id);
            (context_id, deferred)
        })
        .await
        .unwrap();
    assert_eq!(deferred.await.unwrap(), context_id);
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn execute_blocking_from_event_loop_verticle(vx: Vertx) {
    vx.deploy(|| {
        local(|ctx| async move {
            let thread = std::thread::current().id();
            let id = ctx.current_execution().unwrap().deployment_id;
            let result = ctx
                .execute_blocking(move || {
                    assert_eq!(ExecutionInfo::current().unwrap().deployment_id, id);
                    assert_eq!(
                        ExecutionInfo::current().unwrap().threading_model,
                        ThreadingModel::EventLoop
                    );
                    assert!(matches!(
                        ExecutionInfo::current().map(|info| info.thread),
                        Some(ExecutorThread::Worker)
                    ));
                    42
                })
                .await?;
            assert_eq!(result, 42);
            assert_eq!(std::thread::current().id(), thread);
            assert_eq!(ExecutionInfo::current().unwrap().deployment_id, id);
            Ok(())
        })
    })
    .await
    .unwrap();
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn execute_blocking_after_undeploy_returns_terminated(vx: Vertx) {
    let (deployment, _) = capture(&vx).await;
    let vertx = vx.handle();
    let blocking = deployment
        .spawn(async move { Ok(vertx.execute_blocking(|| panic!("stopped work ran"))) })
        .unwrap()
        .await
        .unwrap();
    deployment.stop().await.unwrap();
    assert!(matches!(
        blocking.await,
        Err(VertxError::DeploymentTerminated(id)) if id == deployment.id()
    ));
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn execute_blocking_is_fifo_by_default(vx: Vertx) {
    let order = Arc::new(Mutex::new(Vec::new()));
    let o = order.clone();
    vx.deploy(move || {
        local(move |ctx| async move {
            let (a, b, c) = (o.clone(), o.clone(), o.clone());
            let (x, y, z) = tokio::join!(
                ctx.execute_blocking(move || {
                    a.lock().unwrap().push(0);
                    std::thread::sleep(Duration::from_millis(5));
                }),
                ctx.execute_blocking(move || {
                    b.lock().unwrap().push(1);
                }),
                ctx.execute_blocking(move || {
                    c.lock().unwrap().push(2);
                })
            );
            x?;
            y?;
            z?;
            Ok(())
        })
    })
    .await
    .unwrap();
    assert_eq!(*order.lock().unwrap(), vec![0, 1, 2]);
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn unordered_blocking_obeys_configured_limit_and_can_overlap(vx: Vertx) {
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let m = maximum.clone();
    vx.deploy(move || {
        local(move |ctx| async move {
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let jobs = (0..8).map(|_| {
                let (a, m, b) = (active.clone(), m.clone(), barrier.clone());
                let ctx = &ctx;
                async move {
                    let id = ctx.current_execution().unwrap().context_id;
                    let operation = move || {
                        assert_eq!(ExecutionInfo::current().unwrap().context_id, id);
                        assert!(matches!(
                            ExecutionInfo::current().map(|info| info.thread),
                            Some(ExecutorThread::Worker)
                        ));
                        let n = a.fetch_add(1, Ordering::SeqCst) + 1;
                        m.fetch_max(n, Ordering::SeqCst);
                        b.wait();
                        a.fetch_sub(1, Ordering::SeqCst);
                    };
                    ctx.execute_blocking_unordered(operation).await
                }
            });
            for r in futures::future::join_all(jobs).await {
                r?;
            }
            Ok(())
        })
    })
    .await
    .unwrap();
    assert_eq!(maximum.load(Ordering::SeqCst), 2);
    vx.close().await.unwrap();
}

#[tokio::test]
async fn started_blocking_task_is_not_reported_as_aborted() {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(2)
        .shutdown_grace(Duration::from_millis(40))
        .build()
        .unwrap();
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let id = vx
        .deploy(move || {
            local(move |ctx| async move {
                let c = ctx.clone();
                ctx.spawn_local(async move {
                    c.execute_blocking(move || {
                        started_tx.send(()).ok();
                        release_rx.recv().unwrap();
                    })
                    .await?;
                    Ok(())
                })?;
                Ok(())
            })
        })
        .await
        .unwrap();
    started_rx.await.unwrap();
    assert!(matches!(
        id.stop().await,
        Err(VertxError::ShutdownTimeout(_))
    ));
    release_tx.send(()).unwrap();
    vx.close().await.unwrap();
}

#[tokio::test]
async fn queued_blocking_operation_is_skipped_after_scope_cancellation() {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build()
        .unwrap();
    let (deployment, _) = capture(&vx).await;
    let (entered, started) = oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let vertx = vx.handle();
    let first = deployment
        .spawn(async move {
            vertx
                .execute_blocking_unordered(move || {
                    entered.send(()).ok();
                    wait.recv().unwrap();
                    42
                })
                .await
        })
        .unwrap();
    started.await.unwrap();
    let executed = Arc::new(AtomicUsize::new(0));
    let count = executed.clone();
    let dropped = Arc::new(AtomicUsize::new(0));
    let probe = WorkerDropProbe {
        id: deployment.id(),
        dropped: dropped.clone(),
        panic: false,
    };
    let vertx = vx.handle();
    let (submitted, ready) = oneshot::channel();
    let queued = deployment
        .spawn(async move {
            let job = vertx.execute_blocking_unordered(move || {
                let _probe = probe;
                count.fetch_add(1, Ordering::SeqCst);
            });
            tokio::pin!(job);
            assert!(futures::poll!(&mut job).is_pending());
            submitted.send(()).unwrap();
            job.await
        })
        .unwrap();
    ready.await.unwrap();
    deployment.request_stop();
    release.send(()).unwrap();
    assert_eq!(first.await.unwrap(), 42);
    assert!(matches!(
        queued.await,
        Err(VertxError::DeploymentTerminated(id)) if id == deployment.id()
    ));
    deployment.wait_stopped().await.unwrap();
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    vx.close().await.unwrap();
}
