//! Stop/close ordering, deadlines, joins and deadlock rejection.

use super::support::{capture, capture_worker, echo, local, vx};
use async_trait::async_trait;
use rstest::rstest;
use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::oneshot;
use vertx_rs::{Context, Result, Verticle, Vertx, VertxError, WorkerVerticle};

#[tokio::test]
async fn stop_timeout_reports_error_then_cleanup_continues() {
    struct SlowStop(Option<oneshot::Receiver<()>>);
    #[async_trait(?Send)]
    impl Verticle for SlowStop {
        async fn start(&mut self, _: Context) -> Result<()> {
            Ok(())
        }
        async fn stop(&mut self, ctx: Context) -> Result<()> {
            self.0.take().unwrap().await.ok();
            assert!(Context::current().unwrap().deployment_id() == ctx.deployment_id());
            Ok(())
        }
    }
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(2)
        .shutdown_grace(Duration::from_millis(30))
        .build();
    let (tx, rx) = oneshot::channel();
    let id = vx.deploy(|_| SlowStop(Some(rx))).await.unwrap();
    assert!(matches!(
        vx.undeploy(id).await,
        Err(VertxError::ShutdownTimeout(_))
    ));
    tx.send(()).unwrap();
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn stop_can_execute_blocking_cleanup_during_global_close(vx: Vertx) {
    struct Cleanup;
    #[async_trait(?Send)]
    impl Verticle for Cleanup {
        async fn start(&mut self, _: Context) -> Result<()> {
            Ok(())
        }
        async fn stop(&mut self, ctx: Context) -> Result<()> {
            assert_eq!(
                Context::current().unwrap().execute_blocking(|| 42).await?,
                42
            );
            assert!(matches!(
                ctx.timer(Duration::ZERO),
                Err(VertxError::DeploymentTerminated(_))
            ));
            Ok(())
        }
    }
    vx.deploy(|_| Cleanup).await.unwrap();
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn undeploy_waits_for_detached_blocking_work_from_stop(vx: Vertx) {
    struct Cleanup {
        release: Option<std::sync::mpsc::Receiver<()>>,
        destroyed: Option<oneshot::Sender<()>>,
    }

    #[async_trait(?Send)]
    impl Verticle for Cleanup {
        async fn start(&mut self, _: Context) -> Result<()> {
            Ok(())
        }

        async fn stop(&mut self, ctx: Context) -> Result<()> {
            let (started, running) = oneshot::channel();
            let release = self.release.take().unwrap();
            tokio::select! {
                result = ctx.execute_blocking(move || {
                    started.send(()).unwrap();
                    let _ = release.recv();
                }) => {
                    result?;
                    panic!("blocking cleanup must wait for release");
                }
                result = running => result.unwrap(),
            }
            Ok(())
        }
    }

    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = self.destroyed.take().unwrap().send(());
        }
    }

    let (release, wait) = std::sync::mpsc::channel();
    let (destroyed, destruction) = oneshot::channel();
    let (thread, thread_info) = oneshot::channel();
    let id = vx
        .deploy(move |ctx| {
            let _ = thread.send((ctx, tokio::runtime::Handle::current()));
            Cleanup {
                release: Some(wait),
                destroyed: Some(destroyed),
            }
        })
        .await
        .unwrap();
    let (ctx, driver) = thread_info.await.unwrap();
    ctx.request_undeploy().unwrap();
    destruction.await.unwrap();
    // Let the owning EventLoop finish the poll that destroyed the verticle.
    driver.spawn(async {}).await.unwrap();

    let undeploy = vx.undeploy(id);
    tokio::pin!(undeploy);
    assert!(futures::poll!(&mut undeploy).is_pending());
    release.send(()).unwrap();
    undeploy.await.unwrap();
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn close_is_idempotent_cancels_global_resources_and_rejects_work(vx: Vertx) {
    echo(&vx, "echo").await;
    let bus = vx.event_bus();
    let mut external = bus.consumer::<()>("external").unwrap();
    let timer = vx.timer(Duration::from_secs(99)).unwrap();
    let mut periodic = vx.periodic(Duration::ZERO, Duration::from_secs(1)).unwrap();
    let (a, b) = tokio::join!(vx.close(), vx.close());
    a.unwrap();
    b.unwrap();
    vx.close().await.unwrap();
    assert!(timer.is_cancelled());
    assert!(periodic.is_cancelled());
    assert!(matches!(timer.await, Err(VertxError::TimerCancelled)));
    assert!(matches!(
        periodic.tick().await,
        Err(VertxError::TimerCancelled)
    ));
    assert!(matches!(
        external.recv().await,
        Err(VertxError::VertxClosed)
    ));
    assert!(matches!(
        bus.send("echo", "x".to_owned()).await,
        Err(VertxError::VertxClosed)
    ));
    assert!(matches!(
        vx.deploy(|_| local(|_| async { Ok(()) })).await,
        Err(VertxError::VertxClosed)
    ));
    assert!(matches!(
        vx.timer(Duration::ZERO),
        Err(VertxError::VertxClosed)
    ));
}

#[rstest]
#[tokio::test]
async fn own_context_waiting_shutdown_is_rejected_and_request_stop_works(vx: Vertx) {
    let (tx, rx) = oneshot::channel();
    let id = vx
        .deploy(move |_| {
            local(move |ctx| async move {
                let c = ctx.clone();
                ctx.spawn_local(async move {
                    let owner = c.owner().unwrap();
                    assert!(matches!(
                        owner.undeploy(c.deployment_id().unwrap()).await,
                        Err(VertxError::WouldDeadlock)
                    ));
                    assert!(matches!(
                        owner.close().await,
                        Err(VertxError::WouldDeadlock)
                    ));
                    c.request_undeploy()?;
                    tx.send(()).ok();
                    Ok(())
                })?;
                Ok(())
            })
        })
        .await
        .unwrap();
    rx.await.unwrap();
    match vx.undeploy(id).await {
        Ok(()) | Err(VertxError::DeploymentNotFound(_)) => {}
        other => panic!("unexpected: {other:?}"),
    }
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn close_during_start_cancels_deploy_without_cleanup_error(vx: Vertx) {
    let (tx, rx) = oneshot::channel();
    let owner = vx.clone();
    let deployment = tokio::spawn(async move {
        owner
            .deploy(move |_| {
                local(move |_| async move {
                    tx.send(()).ok();
                    std::future::pending::<Result<()>>().await
                })
            })
            .await
    });
    rx.await.unwrap();
    vx.close().await.unwrap();
    assert!(matches!(
        deployment.await.unwrap(),
        Err(VertxError::DeploymentTerminated(_))
    ));
}

#[rstest]
#[tokio::test]
async fn stop_error_is_returned_and_resources_still_close(vx: Vertx) {
    struct BadStop;
    #[async_trait(?Send)]
    impl Verticle for BadStop {
        async fn start(&mut self, ctx: Context) -> Result<()> {
            ctx.event_bus()
                .consumer::<()>("bad-stop")?
                .handler(|_| async { Ok(()) })
        }
        async fn stop(&mut self, _: Context) -> Result<()> {
            Err(VertxError::Other("stop failed".into()))
        }
    }
    vx.deploy(|_| BadStop).await.unwrap();
    assert!(matches!(
        vx.close().await,
        Err(VertxError::CleanupFailed(_))
    ));
    assert!(vx.is_closed());
    assert!(matches!(
        vx.event_bus().try_send("bad-stop", ()),
        Err(VertxError::VertxClosed)
    ));
    assert!(matches!(
        vx.close().await,
        Err(VertxError::CleanupFailed(_))
    ));
}

#[rstest]
#[tokio::test]
async fn concurrent_undeploy_runs_stop_exactly_once(vx: Vertx) {
    struct OnceStop {
        count: Arc<AtomicUsize>,
        release: Option<oneshot::Receiver<()>>,
        started: Option<oneshot::Sender<()>>,
    }
    #[async_trait(?Send)]
    impl Verticle for OnceStop {
        async fn start(&mut self, _: Context) -> Result<()> {
            Ok(())
        }
        async fn stop(&mut self, _: Context) -> Result<()> {
            self.count.fetch_add(1, Ordering::SeqCst);
            self.started.take().unwrap().send(()).ok();
            self.release.take().unwrap().await.ok();
            Ok(())
        }
    }
    let count = Arc::new(AtomicUsize::new(0));
    let n = count.clone();
    let (tx, rx) = oneshot::channel();
    let (start, started) = oneshot::channel();
    let id = vx
        .deploy(move |_| OnceStop {
            count: n,
            release: Some(rx),
            started: Some(start),
        })
        .await
        .unwrap();
    let a = vx.clone();
    let first = tokio::spawn(async move { a.undeploy(id).await });
    started.await.unwrap();
    let second = vx.undeploy(id);
    tokio::pin!(second);
    assert!(futures::poll!(&mut second).is_pending());
    tx.send(()).unwrap();
    first.await.unwrap().unwrap();
    second.await.unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn cancelled_close_waiter_does_not_cancel_cleanup(vx: Vertx) {
    struct Gate(Option<oneshot::Receiver<()>>);
    #[async_trait(?Send)]
    impl Verticle for Gate {
        async fn start(&mut self, _: Context) -> Result<()> {
            Ok(())
        }
        async fn stop(&mut self, _: Context) -> Result<()> {
            self.0.take().unwrap().await.ok();
            Ok(())
        }
    }
    let (tx, rx) = oneshot::channel();
    vx.deploy(|_| Gate(Some(rx))).await.unwrap();
    let mut close = Box::pin(vx.close());
    assert!(futures::poll!(close.as_mut()).is_pending());
    drop(close);
    tx.send(()).unwrap();
    vx.close().await.unwrap();
    assert!(vx.is_closed());
}

#[rstest]
#[tokio::test]
async fn retained_stop_context_cannot_admit_cleanup_from_external_tasks(vx: Vertx) {
    struct Retained {
        tx: Option<oneshot::Sender<Context>>,
        release: Option<oneshot::Receiver<()>>,
    }
    #[async_trait(?Send)]
    impl Verticle for Retained {
        async fn start(&mut self, _: Context) -> Result<()> {
            Ok(())
        }
        async fn stop(&mut self, ctx: Context) -> Result<()> {
            self.tx.take().unwrap().send(ctx).ok();
            self.release.take().unwrap().await.ok();
            Ok(())
        }
    }
    let (tx, rx) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    let id = vx
        .deploy(move |_| Retained {
            tx: Some(tx),
            release: Some(wait),
        })
        .await
        .unwrap();
    let v = vx.clone();
    let shutdown = tokio::spawn(async move { v.undeploy(id).await });
    let ctx = rx.await.unwrap();
    assert!(matches!(
        ctx.execute_blocking(|| 42).await,
        Err(VertxError::DeploymentTerminated(_))
    ));
    release.send(()).unwrap();
    shutdown.await.unwrap().unwrap();
    vx.close().await.unwrap();
}

#[tokio::test]
async fn saturated_worker_pool_does_not_prevent_close_timeout_or_later_cleanup() {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .shutdown_grace(Duration::from_millis(30))
        .build();
    let (_, ctx) = capture_worker(&vx).await;
    let (entered, started) = oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    ctx.spawn(async move {
        entered.send(()).ok();
        wait.recv().unwrap();
        Ok(())
    })
    .unwrap();
    started.await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), vx.close()).await;
    release.send(()).unwrap();
    assert!(matches!(
        result.unwrap(),
        Err(VertxError::ShutdownTimeout(_))
    ));
    vx.close().await.unwrap();
}

#[tokio::test]
async fn single_worker_stop_can_await_blocking_cleanup_in_the_shared_pool() {
    struct StopWorker(Option<oneshot::Sender<()>>);
    #[async_trait]
    impl WorkerVerticle for StopWorker {
        async fn start(&mut self, _: Context) -> Result<()> {
            Ok(())
        }
        async fn stop(&mut self, ctx: Context) -> Result<()> {
            let id = ctx.deployment_id();
            ctx.execute_blocking(move || {
                assert!(Context::is_on_worker_thread());
                assert_eq!(Context::current().unwrap().deployment_id(), id);
            })
            .await?;
            assert_eq!(Context::current().unwrap().deployment_id(), id);
            self.0.take().unwrap().send(()).ok();
            Ok(())
        }
    }
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build();
    let (done, finished) = oneshot::channel();
    vx.deploy_worker(move |_| StopWorker(Some(done)))
        .await
        .unwrap();
    vx.close().await.unwrap();
    finished.await.unwrap();
}

#[tokio::test]
async fn raw_tokio_task_on_owned_eventloop_cannot_wait_for_its_own_close() {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build();
    let (_, ctx) = capture(&vx).await;
    let owner = vx.clone();
    ctx.spawn(async move {
        tokio::spawn(async move {
            assert!(Context::current().is_none());
            assert!(Context::is_on_event_loop_thread());
            assert!(matches!(
                owner.close().await,
                Err(VertxError::WouldDeadlock)
            ));
        })
        .await
        .unwrap();
        Ok(())
    })
    .unwrap()
    .await
    .unwrap();
    assert!(!vx.is_closed());
    vx.close().await.unwrap();
}

#[tokio::test]
async fn close_joins_every_eventloop_even_with_retained_contexts_and_task_handles() {
    struct ExitCount(Arc<AtomicUsize>);
    impl Drop for ExitCount {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    thread_local! {
        static EXIT_COUNT: RefCell<Option<ExitCount>> = const { RefCell::new(None) };
    }
    let vx = Vertx::builder()
        .event_loop_threads(2)
        .worker_threads(1)
        .build();
    let exited = Arc::new(AtomicUsize::new(0));
    let mut contexts = Vec::new();
    let mut tasks = Vec::new();
    for _ in 0..2 {
        let (_, ctx) = capture(&vx).await;
        let count = exited.clone();
        let (entered, started) = oneshot::channel();
        tasks.push(
            ctx.spawn(async move {
                EXIT_COUNT.with(|slot| *slot.borrow_mut() = Some(ExitCount(count)));
                entered.send(()).ok();
                std::future::pending::<Result<()>>().await
            })
            .unwrap(),
        );
        started.await.unwrap();
        contexts.push(ctx);
    }
    vx.close().await.unwrap();
    assert_eq!(exited.load(Ordering::SeqCst), 2);
    for task in tasks {
        assert!(task.is_finished());
        assert!(matches!(task.await, Err(VertxError::TaskCancelled)));
    }
    for ctx in contexts {
        assert!(ctx.spawn(async { Ok(()) }).is_err());
    }
    vx.close().await.unwrap();
}

#[tokio::test]
async fn eventloop_can_await_close_of_a_different_vertx_instance() {
    let first = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build();
    let second = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build();
    let (_, ctx) = capture(&first).await;
    let id = ctx.deployment_id();
    let target = second.clone();
    ctx.spawn(async move {
        target.close().await?;
        assert_eq!(Context::current().unwrap().deployment_id(), id);
        Ok(())
    })
    .unwrap()
    .await
    .unwrap();
    assert!(second.is_closed());
    assert!(!first.is_closed());
    first.close().await.unwrap();
}
