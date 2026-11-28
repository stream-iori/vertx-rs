//! Stop/close ordering, deadlines, joins and deadlock rejection.

use super::support::{capture, capture_worker, echo, local, report_on_thread_exit, vx};
use rstest::rstest;
use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;
use tokio::sync::oneshot;
use vertx_rs::{
    ExecutionInfo, ExecutorThread, Result, ThreadingModel, Verticle, Vertx, VertxError,
    VertxHandle, WorkerVerticle,
};

#[tokio::test]
async fn stop_timeout_reports_error_then_cleanup_continues() {
    struct SlowStop(Option<oneshot::Receiver<()>>);
    impl Verticle for SlowStop {
        async fn start(&mut self, _: VertxHandle) -> Result<()> {
            Ok(())
        }
        async fn stop(&mut self, ctx: VertxHandle) -> Result<()> {
            self.0.take().unwrap().await.ok();
            assert!(ExecutionInfo::current() == ctx.current_execution());
            Ok(())
        }
    }
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(2)
        .shutdown_grace(Duration::from_millis(30))
        .build()
        .unwrap();
    let (tx, rx) = oneshot::channel();
    let id = vx.deploy(|| SlowStop(Some(rx))).await.unwrap();
    assert!(matches!(
        id.stop().await,
        Err(VertxError::ShutdownTimeout(_))
    ));
    tx.send(()).unwrap();
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn stop_can_execute_blocking_cleanup_during_global_close(vx: Vertx) {
    struct Cleanup;
    impl Verticle for Cleanup {
        async fn start(&mut self, _: VertxHandle) -> Result<()> {
            Ok(())
        }
        async fn stop(&mut self, ctx: VertxHandle) -> Result<()> {
            assert_eq!(ctx.execute_blocking(|| 42).await?, 42);
            assert!(matches!(
                ctx.timer(Duration::ZERO),
                Err(VertxError::VertxClosed)
            ));
            Ok(())
        }
    }
    vx.deploy(|| Cleanup).await.unwrap();
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn undeploy_waits_for_detached_blocking_work_from_stop(vx: Vertx) {
    struct Cleanup {
        release: Option<std::sync::mpsc::Receiver<()>>,
        destroyed: Option<oneshot::Sender<()>>,
    }
    impl Verticle for Cleanup {
        async fn start(&mut self, _: VertxHandle) -> Result<()> {
            Ok(())
        }

        async fn stop(&mut self, ctx: VertxHandle) -> Result<()> {
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
        .deploy(move || {
            let _ = thread.send(tokio::runtime::Handle::current());
            Cleanup {
                release: Some(wait),
                destroyed: Some(destroyed),
            }
        })
        .await
        .unwrap();
    let driver = thread_info.await.unwrap();
    id.request_stop();
    destruction.await.unwrap();
    // Let the owning EventLoop finish the poll that destroyed the verticle.
    driver.spawn(async {}).await.unwrap();

    let undeploy = id.stop();
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
    let bus = vx.event_bus().unwrap();
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
        vx.deploy(|| local(|_| async { Ok(()) })).await,
        Err(VertxError::VertxClosed)
    ));
    assert!(matches!(
        vx.timer(Duration::ZERO),
        Err(VertxError::VertxClosed)
    ));
}

#[rstest]
#[case::event_loop(ThreadingModel::EventLoop)]
#[case::worker(ThreadingModel::Worker)]
#[tokio::test]
async fn managed_task_can_request_close_with_an_external_waiter(#[case] model: ThreadingModel) {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build()
        .unwrap();
    let (deployment, _) = match model {
        ThreadingModel::EventLoop => capture(&vx).await,
        ThreadingModel::Worker => capture_worker(&vx).await,
    };
    let closed = vx.wait_closed();
    tokio::pin!(closed);
    assert!(futures::poll!(&mut closed).is_pending());
    assert!(!vx.is_admission_closed());

    let handle = vx.handle();
    let (report, mut exited) = oneshot::channel();
    deployment
        .spawn(async move {
            assert_eq!(ExecutionInfo::current().unwrap().threading_model, model);
            report_on_thread_exit(report);
            handle.request_close()
        })
        .unwrap()
        .await
        .unwrap();
    assert!(vx.is_admission_closed());

    tokio::time::timeout(Duration::from_secs(3), closed)
        .await
        .unwrap()
        .unwrap();
    exited.try_recv().unwrap();
    vx.wait_closed().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn concurrent_close_requests_run_stop_once(vx: Vertx) {
    struct CountStops(Arc<AtomicUsize>);
    impl Verticle for CountStops {
        async fn start(&mut self, _: VertxHandle) -> Result<()> {
            Ok(())
        }

        async fn stop(&mut self, _: VertxHandle) -> Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let stops = Arc::new(AtomicUsize::new(0));
    let count = stops.clone();
    vx.deploy(move || CountStops(count)).await.unwrap();
    let handle = vx.handle();
    let ready = Barrier::new(2);
    std::thread::scope(|threads| {
        let request = threads.spawn(|| {
            ready.wait();
            handle.request_close()
        });
        ready.wait();
        vx.request_close().unwrap();
        request.join().unwrap().unwrap();
    });

    let (first, second) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(vx.wait_closed(), vx.wait_closed())
    })
    .await
    .unwrap();
    first.unwrap();
    second.unwrap();
    assert_eq!(stops.load(Ordering::SeqCst), 1);
}

#[rstest]
#[tokio::test]
async fn own_context_waiting_shutdown_is_rejected_and_request_stop_works(vx: Vertx) {
    let (tx, rx) = oneshot::channel();
    let owner = vx.clone();
    let deployment = vx
        .deploy(move || {
            local(move |ctx| async move {
                let current = ctx.current_deployment().unwrap();
                ctx.spawn_local(async move {
                    assert!(matches!(
                        current.stop().await,
                        Err(VertxError::WouldDeadlock)
                    ));
                    assert!(matches!(
                        current.wait_stopped().await,
                        Err(VertxError::WouldDeadlock)
                    ));
                    assert!(matches!(
                        owner.close().await,
                        Err(VertxError::WouldDeadlock)
                    ));
                    assert!(matches!(
                        owner.wait_closed().await,
                        Err(VertxError::WouldDeadlock)
                    ));
                    current.request_stop();
                    tx.send(()).ok();
                    Ok(())
                })?;
                Ok(())
            })
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), rx)
        .await
        .unwrap()
        .unwrap();
    deployment.wait_stopped().await.unwrap();
    assert!(!vx.is_admission_closed());
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn close_during_start_cancels_deploy_without_cleanup_error(vx: Vertx) {
    let (tx, rx) = oneshot::channel();
    let owner = vx.clone();
    let deployment = tokio::spawn(async move {
        owner
            .deploy(move || {
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
    impl Verticle for BadStop {
        async fn start(&mut self, ctx: VertxHandle) -> Result<()> {
            ctx.event_bus()
                .unwrap()
                .consumer::<()>("bad-stop")?
                .handler(async |_| Ok(()))?;
            Ok(())
        }
        async fn stop(&mut self, _: VertxHandle) -> Result<()> {
            Err(VertxError::Other("stop failed".into()))
        }
    }
    vx.deploy(|| BadStop).await.unwrap();
    let bus = vx.event_bus().unwrap();
    assert!(matches!(
        vx.close().await,
        Err(VertxError::CleanupFailed(_))
    ));
    assert!(vx.is_admission_closed());
    assert!(matches!(
        bus.try_send("bad-stop", ()),
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
    impl Verticle for OnceStop {
        async fn start(&mut self, _: VertxHandle) -> Result<()> {
            Ok(())
        }
        async fn stop(&mut self, _: VertxHandle) -> Result<()> {
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
        .deploy(move || OnceStop {
            count: n,
            release: Some(rx),
            started: Some(start),
        })
        .await
        .unwrap();
    let a = id.clone();
    let first = tokio::spawn(async move { a.stop().await });
    started.await.unwrap();
    let second = id.stop();
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
    impl Verticle for Gate {
        async fn start(&mut self, _: VertxHandle) -> Result<()> {
            Ok(())
        }
        async fn stop(&mut self, _: VertxHandle) -> Result<()> {
            self.0.take().unwrap().await.ok();
            Ok(())
        }
    }
    let (tx, rx) = oneshot::channel();
    vx.deploy(|| Gate(Some(rx))).await.unwrap();
    let mut close = Box::pin(vx.close());
    assert!(futures::poll!(close.as_mut()).is_pending());
    drop(close);
    tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), vx.wait_closed())
        .await
        .unwrap()
        .unwrap();
    assert!(vx.is_admission_closed());
}

#[rstest]
#[tokio::test]
async fn blocking_created_during_stop_cannot_be_admitted_by_an_external_task(vx: Vertx) {
    type Blocking = std::pin::Pin<Box<dyn std::future::Future<Output = Result<u32>> + Send>>;
    struct Retained {
        tx: Option<oneshot::Sender<Blocking>>,
        release: Option<oneshot::Receiver<()>>,
    }
    impl Verticle for Retained {
        async fn start(&mut self, _: VertxHandle) -> Result<()> {
            Ok(())
        }
        async fn stop(&mut self, ctx: VertxHandle) -> Result<()> {
            self.tx
                .take()
                .unwrap()
                .send(Box::pin(ctx.execute_blocking(|| 42)))
                .ok();
            self.release.take().unwrap().await.ok();
            Ok(())
        }
    }
    let (tx, rx) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    let id = vx
        .deploy(move || Retained {
            tx: Some(tx),
            release: Some(wait),
        })
        .await
        .unwrap();
    let shutdown = tokio::spawn(async move { id.stop().await });
    let blocking = rx.await.unwrap();
    assert!(matches!(
        blocking.await,
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
        .build()
        .unwrap();
    let (ctx, _) = capture_worker(&vx).await;
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
    tokio::time::timeout(Duration::from_secs(3), vx.wait_closed())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn single_worker_stop_can_await_blocking_cleanup_in_the_shared_pool() {
    struct StopWorker(Option<oneshot::Sender<()>>);
    impl WorkerVerticle for StopWorker {
        async fn start(&mut self, _: VertxHandle) -> Result<()> {
            Ok(())
        }
        async fn stop(&mut self, ctx: VertxHandle) -> Result<()> {
            let id = ctx.current_execution().unwrap().deployment_id;
            ctx.execute_blocking(move || {
                assert!(matches!(
                    ExecutionInfo::current().map(|info| info.thread),
                    Some(ExecutorThread::Worker)
                ));
                assert_eq!(ExecutionInfo::current().unwrap().deployment_id, id);
            })
            .await?;
            assert_eq!(ExecutionInfo::current().unwrap().deployment_id, id);
            self.0.take().unwrap().send(()).ok();
            Ok(())
        }
    }
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build()
        .unwrap();
    let (done, finished) = oneshot::channel();
    vx.deploy_worker(move || StopWorker(Some(done)))
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
        .build()
        .unwrap();
    let (ctx, _) = capture(&vx).await;
    let owner = vx.clone();
    ctx.spawn(async move {
        let thread = std::thread::current().id();
        tokio::spawn(async move {
            assert!(ExecutionInfo::current().is_none());
            assert_eq!(std::thread::current().id(), thread);
            assert!(matches!(
                owner.close().await,
                Err(VertxError::WouldDeadlock)
            ));
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(3), owner.wait_closed()).await,
                Ok(Err(VertxError::WouldDeadlock))
            ));
        })
        .await
        .unwrap();
        Ok(())
    })
    .unwrap()
    .await
    .unwrap();
    assert!(!vx.is_admission_closed());
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
        .build()
        .unwrap();
    let exited = Arc::new(AtomicUsize::new(0));
    let mut contexts = Vec::new();
    let mut tasks = Vec::new();
    for _ in 0..2 {
        let (ctx, _) = capture(&vx).await;
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
        .build()
        .unwrap();
    let second = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build()
        .unwrap();
    let (ctx, _) = capture(&first).await;
    let id = Some(ctx.id());
    let target = second.clone();
    ctx.spawn(async move {
        target.request_close()?;
        target.wait_closed().await?;
        assert_eq!(ExecutionInfo::current().unwrap().deployment_id, id);
        Ok(())
    })
    .unwrap()
    .await
    .unwrap();
    assert!(second.is_admission_closed());
    assert!(!first.is_admission_closed());
    first.close().await.unwrap();
}
