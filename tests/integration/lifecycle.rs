//! Deployment readiness, rollback, cancellation and resource ownership.

use super::support::{Local, capture, capture_worker, echo, local, vx, worker};
use async_trait::async_trait;
use rstest::rstest;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use vertx_rs::{Context, DeploymentOptions, Result, Verticle, Vertx, VertxError, WorkerVerticle};

struct Lifecycle {
    started: Arc<AtomicUsize>,
    stopped: Arc<AtomicUsize>,
}

#[async_trait(?Send)]
impl Verticle for Lifecycle {
    async fn start(&mut self, _: Context) -> Result<()> {
        tokio::task::yield_now().await;
        self.started.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn stop(&mut self, _: Context) -> Result<()> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[rstest]
#[tokio::test]
async fn deploy_runs_factory_then_start_then_undeploy_stops(vx: Vertx) {
    let started = Arc::new(AtomicUsize::new(0));
    let stopped = Arc::new(AtomicUsize::new(0));
    let (a, b) = (started.clone(), stopped.clone());
    let id = vx
        .deploy(move |_| Lifecycle {
            started: a,
            stopped: b,
        })
        .await
        .unwrap();
    assert_eq!(started.load(Ordering::SeqCst), 1);
    assert_eq!(stopped.load(Ordering::SeqCst), 0);
    vx.undeploy(id).await.unwrap();
    assert_eq!(stopped.load(Ordering::SeqCst), 1);
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn undeploy_unknown_id_errors(vx: Vertx) {
    let (id, _) = capture(&vx).await;
    vx.undeploy(id).await.unwrap();
    assert!(matches!(
        vx.undeploy(id).await,
        Err(VertxError::DeploymentNotFound(_))
    ));
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn undeploy_aborts_misbehaving_child(vx: Vertx) {
    let (tx, rx) = oneshot::channel();
    let id = vx
        .deploy(move |_| {
            local(move |ctx| async move {
                let (entered, started) = oneshot::channel();
                let handle = ctx.spawn_local(async move {
                    entered.send(()).ok();
                    std::future::pending::<()>().await;
                    Ok(())
                })?;
                // This test requires an already-running, non-cooperative child.
                started.await.unwrap();
                tx.send(handle).ok();
                Ok(())
            })
        })
        .await
        .unwrap();
    let child = rx.await.unwrap();
    vx.undeploy(id).await.unwrap();
    assert!(matches!(child.await, Err(VertxError::TaskCancelled)));
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn undeploy_unregisters_bound_consumers(vx: Vertx) {
    let id = echo(&vx, "echo").await;
    assert_eq!(
        vx.event_bus()
            .request::<_, String>("echo", "hi".to_owned())
            .await
            .unwrap(),
        "hi"
    );
    vx.undeploy(id).await.unwrap();
    assert!(matches!(
        vx.event_bus().send("echo", "x".to_owned()).await,
        Err(VertxError::NoConsumer(_))
    ));
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn deploy_with_instances_and_typed_config(vx: Vertx) {
    let ids = vx
        .deploy_with(
            |_, cfg: Arc<String>| {
                local(move |ctx| async move {
                    ctx.event_bus()
                        .consumer::<String>("configured")?
                        .handler(move |env| {
                            let body = format!("{}:{}", cfg, env.body());
                            async move { env.reply(body) }
                        })
                })
            },
            DeploymentOptions::new().instances(3).config("P".to_owned()),
        )
        .await
        .unwrap();
    assert_eq!(ids.len(), 3);
    for _ in 0..6 {
        assert_eq!(
            vx.event_bus()
                .request::<_, String>("configured", "x".to_owned())
                .await
                .unwrap(),
            "P:x"
        );
    }
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn start_failure_and_factory_panic_clean_resources(vx: Vertx) {
    let result = vx
        .deploy(|_| {
            local(|ctx| async move {
                ctx.event_bus()
                    .consumer::<()>("failed")?
                    .handler(|_| async { Ok(()) })?;
                Err(VertxError::StartFailed("expected".into()))
            })
        })
        .await;
    assert!(matches!(result, Err(VertxError::StartFailed(_))));
    assert!(matches!(
        vx.event_bus().try_send("failed", ()),
        Err(VertxError::NoConsumer(_))
    ));
    let result = vx
        .deploy::<_, Local<fn(Context) -> std::future::Ready<Result<()>>>>(|ctx| {
            let _c = ctx.event_bus().consumer::<()>("panic").unwrap();
            panic!("factory panic")
        })
        .await;
    assert!(matches!(result, Err(VertxError::Panic(_))));
    assert!(matches!(
        vx.event_bus().try_send("panic", ()),
        Err(VertxError::NoConsumer(_))
    ));
    vx.close().await.unwrap();
}

#[rstest]
#[case::event_loop(false)]
#[case::worker(true)]
#[tokio::test]
async fn start_panic_skips_stop_and_drops_before_deploy_returns(#[case] worker: bool) {
    struct PanicStart {
        stopped: Arc<AtomicBool>,
        dropped: Arc<AtomicBool>,
    }

    #[async_trait(?Send)]
    impl Verticle for PanicStart {
        async fn start(&mut self, _: Context) -> Result<()> {
            panic!("start panicked");
        }

        async fn stop(&mut self, _: Context) -> Result<()> {
            self.stopped.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl WorkerVerticle for PanicStart {
        async fn start(&mut self, _: Context) -> Result<()> {
            panic!("start panicked");
        }

        async fn stop(&mut self, _: Context) -> Result<()> {
            self.stopped.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    impl Drop for PanicStart {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build();
    let stopped = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));
    let verticle = PanicStart {
        stopped: stopped.clone(),
        dropped: dropped.clone(),
    };
    let result = if worker {
        vx.deploy_worker(move |_| verticle).await
    } else {
        vx.deploy(move |_| verticle).await
    };
    assert!(matches!(result, Err(VertxError::Panic(message)) if message == "start panicked"));
    assert!(!stopped.load(Ordering::SeqCst));
    assert!(dropped.load(Ordering::SeqCst));

    // The next start on the same executor follows the failed driver's final poll.
    if worker {
        capture_worker(&vx).await;
    } else {
        capture(&vx).await;
    }
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn cancellation_of_pending_deploy_cleans_startup(vx: Vertx) {
    let (tx, rx) = oneshot::channel();
    let v = vx.clone();
    let deploy = tokio::spawn(async move {
        v.deploy(move |_| {
            local(move |ctx| async move {
                ctx.event_bus()
                    .consumer::<()>("startup")?
                    .handler(|_| async { Ok(()) })?;
                tx.send(ctx).ok();
                std::future::pending::<Result<()>>().await
            })
        })
        .await
    });
    let ctx = rx.await.unwrap();
    deploy.abort();
    assert!(deploy.await.unwrap_err().is_cancelled());
    vx.close().await.unwrap();
    assert!(!ctx.is_alive());
}

#[rstest]
#[tokio::test]
async fn multi_instance_failure_rolls_back_successful_instances(vx: Vertx) {
    let count = Arc::new(AtomicUsize::new(0));
    let c = count.clone();
    let result = vx
        .deploy_with(
            move |_, _: Arc<()>| {
                let n = c.fetch_add(1, Ordering::SeqCst);
                local(move |ctx| async move {
                    if n == 1 {
                        return Err(VertxError::StartFailed("second instance".into()));
                    }
                    ctx.event_bus()
                        .consumer::<()>("group")?
                        .handler(|_| async { Ok(()) })
                })
            },
            DeploymentOptions::new().instances(3),
        )
        .await;
    assert!(matches!(result, Err(VertxError::DeploymentFailed { .. })));
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert!(matches!(
        vx.event_bus().try_send("group", ()),
        Err(VertxError::NoConsumer(_))
    ));
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn undeploy_waits_for_inflight_handler_to_be_dropped(vx: Vertx) {
    let (started_tx, started_rx) = oneshot::channel();
    let dropped = Arc::new(AtomicUsize::new(0));
    let d = dropped.clone();
    struct OnDrop(Arc<AtomicUsize>);
    impl Drop for OnDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            assert!(Context::current().is_some());
        }
    }
    let id = vx
        .deploy(move |_| {
            local(move |ctx| async move {
                let mut start = Some(started_tx);
                ctx.event_bus().consumer::<()>("hang")?.handler(move |_| {
                    let start = start.take().unwrap();
                    let guard = OnDrop(d.clone());
                    async move {
                        let _guard = guard;
                        start.send(()).ok();
                        std::future::pending::<Result<()>>().await
                    }
                })
            })
        })
        .await
        .unwrap();
    vx.event_bus().send("hang", ()).await.unwrap();
    started_rx.await.unwrap();
    vx.undeploy(id).await.unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn worker_failed_start_waits_for_its_resource_cleanup(vx: Vertx) {
    let result = vx
        .deploy_worker(|_| {
            worker(|ctx| async move {
                ctx.event_bus()
                    .consumer::<()>("worker-start")?
                    .handler_worker(|_| async { Ok(()) })?;
                Err(VertxError::StartFailed("worker startup failed".into()))
            })
        })
        .await;
    assert!(matches!(result, Err(VertxError::StartFailed(_))));
    assert!(matches!(
        vx.event_bus().try_send("worker-start", ()),
        Err(VertxError::NoConsumer(_))
    ));
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn cancelling_multi_instance_deploy_rolls_back_prior_instances(vx: Vertx) {
    let created = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = oneshot::channel();
    let signal = Arc::new(Mutex::new(Some(tx)));
    let v = vx.clone();
    let future = tokio::spawn(async move {
        v.deploy_with(
            move |_, _: Arc<()>| {
                let n = created.fetch_add(1, Ordering::SeqCst);
                let signal = signal.clone();
                local(move |ctx| async move {
                    ctx.event_bus()
                        .consumer::<()>("cancel-group")?
                        .handler(|_| async { Ok(()) })?;
                    if n == 1 {
                        signal.lock().unwrap().take().unwrap().send(()).ok();
                        std::future::pending::<()>().await;
                    }
                    Ok(())
                })
            },
            DeploymentOptions::new().instances(3),
        )
        .await
    });
    rx.await.unwrap();
    future.abort();
    assert!(future.await.unwrap_err().is_cancelled());
    // Both the pending instance and the already-ready instance must stop.
    vx.close().await.unwrap();
}
