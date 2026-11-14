//! Shared fixed pool, ordered admission and non-preemptible work.

use super::support::{WorkerDropProbe, capture, local, vx};
use rstest::rstest;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use vertx_rs::{Context, Vertx, VertxError};

#[rstest]
#[tokio::test]
async fn execute_blocking_from_event_loop_verticle(vx: Vertx) {
    vx.deploy(|_| {
        local(|ctx| async move {
            let thread = std::thread::current().id();
            let id = ctx.deployment_id();
            let result = ctx
                .execute_blocking(move || {
                    assert_eq!(Context::current().unwrap().deployment_id(), id);
                    assert!(Context::current().unwrap().is_event_loop());
                    assert!(Context::is_on_worker_thread());
                    assert!(!Context::is_on_event_loop_thread());
                    42
                })
                .await?;
            assert_eq!(result, 42);
            assert_eq!(std::thread::current().id(), thread);
            assert_eq!(Context::current().unwrap().deployment_id(), id);
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
    let (id, ctx) = capture(&vx).await;
    vx.undeploy(id).await.unwrap();
    assert!(matches!(
        ctx.execute_blocking(|| 1).await,
        Err(VertxError::DeploymentTerminated(_))
    ));
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn execute_blocking_is_fifo_by_default(vx: Vertx) {
    let order = Arc::new(Mutex::new(Vec::new()));
    let o = order.clone();
    vx.deploy(move |_| {
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
#[case::boolean_api(false)]
#[case::named_api(true)]
#[tokio::test]
async fn unordered_blocking_obeys_configured_limit_and_can_overlap(vx: Vertx, #[case] named: bool) {
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let m = maximum.clone();
    vx.deploy(move |_| {
        local(move |ctx| async move {
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let jobs = (0..8).map(|_| {
                let (a, m, b) = (active.clone(), m.clone(), barrier.clone());
                let ctx = &ctx;
                async move {
                    let id = ctx.id();
                    let operation = move || {
                        assert_eq!(Context::current().unwrap().id(), id);
                        assert!(Context::is_on_worker_thread());
                        let n = a.fetch_add(1, Ordering::SeqCst) + 1;
                        m.fetch_max(n, Ordering::SeqCst);
                        b.wait();
                        a.fetch_sub(1, Ordering::SeqCst);
                    };
                    if named {
                        ctx.execute_blocking_unordered(operation).await
                    } else {
                        ctx.execute_blocking_with(operation, false).await
                    }
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
        .build();
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let id = vx
        .deploy(move |_| {
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
        vx.undeploy(id).await,
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
        .build();
    let (id, ctx) = capture(&vx).await;
    let (entered, started) = oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let first = ctx.execute_blocking_with(
        move || {
            entered.send(()).ok();
            wait.recv().unwrap();
            42
        },
        false,
    );
    tokio::pin!(first);
    assert!(futures::poll!(&mut first).is_pending());
    started.await.unwrap();
    let executed = Arc::new(AtomicUsize::new(0));
    let count = executed.clone();
    let dropped = Arc::new(AtomicUsize::new(0));
    let probe = WorkerDropProbe {
        id: ctx.deployment_id(),
        dropped: dropped.clone(),
        panic: false,
    };
    let queued = ctx.execute_blocking_with(
        move || {
            let _probe = probe;
            count.fetch_add(1, Ordering::SeqCst);
        },
        false,
    );
    tokio::pin!(queued);
    assert!(futures::poll!(&mut queued).is_pending());
    let undeploy = vx.undeploy(id);
    tokio::pin!(undeploy);
    assert!(futures::poll!(&mut undeploy).is_pending());
    release.send(()).unwrap();
    assert_eq!(first.await.unwrap(), 42);
    assert!(matches!(
        queued.await,
        Err(VertxError::DeploymentTerminated(_))
    ));
    undeploy.await.unwrap();
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    vx.close().await.unwrap();
}
