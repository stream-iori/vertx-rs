//! Deployment readiness, rollback, cancellation and resource ownership.

use super::support::{Local, capture, capture_worker, echo, local, vx, worker};
use rstest::rstest;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll};
use tokio::sync::oneshot;
use vertx_rs::{
    Consumer, Context, DeploymentOptions, Result, Verticle, Vertx, VertxError, WorkerVerticle,
};

#[derive(Clone, Copy)]
enum Fault {
    StartConstruction,
    StartDrop,
    StopConstruction,
    StopDrop,
}

struct ReadyCallback {
    ctx: Context,
    drop_panic: Option<&'static str>,
}

impl Future for ReadyCallback {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Result<()>> {
        assert_eq!(Context::current().unwrap().id(), self.ctx.id());
        Poll::Ready(Ok(()))
    }
}

impl Drop for ReadyCallback {
    fn drop(&mut self) {
        assert_eq!(Context::current().unwrap().id(), self.ctx.id());
        assert_eq!(Context::is_on_worker_thread(), self.ctx.is_worker());
        if let Some(message) = self.drop_panic {
            panic!("{message}");
        }
    }
}

struct CallbackFault {
    fault: Fault,
    consumer: Option<Consumer<()>>,
    stops: Arc<AtomicUsize>,
}

impl CallbackFault {
    fn start_callback(&mut self, ctx: Context) -> ReadyCallback {
        assert_eq!(Context::current().unwrap().id(), ctx.id());
        self.consumer = Some(ctx.event_bus().consumer("callback_fault").unwrap());
        assert!(
            !matches!(self.fault, Fault::StartConstruction),
            "start construction"
        );
        ReadyCallback {
            ctx,
            drop_panic: matches!(self.fault, Fault::StartDrop).then_some("start future destructor"),
        }
    }

    fn stop_callback(&mut self, ctx: Context) -> ReadyCallback {
        assert_eq!(Context::current().unwrap().id(), ctx.id());
        self.stops.fetch_add(1, Ordering::SeqCst);
        assert!(
            !matches!(self.fault, Fault::StopConstruction),
            "stop construction"
        );
        ReadyCallback {
            ctx,
            drop_panic: matches!(self.fault, Fault::StopDrop).then_some("stop future destructor"),
        }
    }
}

impl Verticle for CallbackFault {
    fn start(&mut self, ctx: Context) -> impl Future<Output = Result<()>> {
        self.start_callback(ctx)
    }

    fn stop(&mut self, ctx: Context) -> impl Future<Output = Result<()>> {
        self.stop_callback(ctx)
    }
}

impl WorkerVerticle for CallbackFault {
    fn start(&mut self, ctx: Context) -> impl Future<Output = Result<()>> + Send {
        self.start_callback(ctx)
    }

    fn stop(&mut self, ctx: Context) -> impl Future<Output = Result<()>> + Send {
        self.stop_callback(ctx)
    }
}

#[rstest]
#[case::start_construction(Fault::StartConstruction, "start construction", 0)]
#[case::start_drop(Fault::StartDrop, "start future destructor", 0)]
#[case::stop_construction(Fault::StopConstruction, "stop construction", 1)]
#[case::stop_drop(Fault::StopDrop, "stop future destructor", 1)]
#[tokio::test]
async fn lifecycle_contains_panics_outside_future_poll(
    vx: Vertx,
    #[case] fault: Fault,
    #[case] message: &str,
    #[case] expected_stops: usize,
    #[values(false, true)] worker: bool,
) {
    let stops = Arc::new(AtomicUsize::new(0));
    let count = stops.clone();
    let factory = move |_| CallbackFault {
        fault,
        consumer: None,
        stops: count,
    };
    let deployment = if worker {
        vx.deploy_worker(factory).await
    } else {
        vx.deploy(factory).await
    };
    let result = match deployment {
        Ok(id) => vx.undeploy(id).await,
        Err(error) => Err(error),
    };
    assert!(matches!(result, Err(VertxError::Panic(actual)) if actual == message));
    assert_eq!(stops.load(Ordering::SeqCst), expected_stops);
    assert_eq!(vx.diagnostics().active_deployments, 0);
    assert!(matches!(
        vx.event_bus().try_send("callback_fault", ()),
        Err(VertxError::NoConsumer(_))
    ));
    vx.close().await.unwrap();
    assert_eq!(vx.diagnostics().failed, 1);
}

#[rstest]
#[case::foreign_deployment(false)]
#[case::own_live_deployment(true)]
#[tokio::test]
async fn a_live_start_reports_deployment_terminated_as_failure(vx: Vertx, #[case] own: bool) {
    let (foreign, _) = capture(&vx).await;
    vx.undeploy(foreign).await.unwrap();
    let reports = Arc::new(AtomicUsize::new(0));
    let count = reports.clone();
    vx.set_exception_handler(move |_| {
        count.fetch_add(1, Ordering::SeqCst);
    });
    let result = vx
        .deploy(move |_| {
            local(move |ctx| async move {
                let id = if own { ctx.deployment_id() } else { foreign };
                Err(VertxError::DeploymentTerminated(id))
            })
        })
        .await;
    assert!(matches!(result, Err(VertxError::DeploymentTerminated(id)) if (id != foreign) == own));
    vx.close().await.unwrap();
    assert_eq!(reports.load(Ordering::SeqCst), 1);
    assert_eq!(vx.diagnostics().failed, 1);
}

struct Lifecycle {
    started: Arc<AtomicUsize>,
    stopped: Arc<AtomicUsize>,
}
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
                        .handler(async move |env| {
                            let body = format!("{}:{}", cfg, env.body());
                            env.reply(body)
                        })
                })
            },
            DeploymentOptions::new()
                .instances(NonZeroUsize::new(3).unwrap())
                .config("P".to_owned()),
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
                    .handler(async |_| Ok(()))?;
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
    impl Verticle for PanicStart {
        async fn start(&mut self, _: Context) -> Result<()> {
            panic!("start panicked");
        }

        async fn stop(&mut self, _: Context) -> Result<()> {
            self.stopped.store(true, Ordering::SeqCst);
            Ok(())
        }
    }
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
        .build()
        .unwrap();
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
                    .handler(async |_| Ok(()))?;
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
    struct Instance {
        index: usize,
        second_stopping: Arc<tokio::sync::Notify>,
    }
    impl Verticle for Instance {
        async fn start(&mut self, ctx: Context) -> Result<()> {
            if self.index == 2 {
                return Err(VertxError::StartFailed("third instance".into()));
            }
            ctx.event_bus()
                .consumer::<()>("group")?
                .handler(async |_| Ok(()))
        }

        async fn stop(&mut self, _: Context) -> Result<()> {
            match self.index {
                0 => self.second_stopping.notified().await,
                1 => {
                    self.second_stopping.notify_one();
                    return Err(VertxError::Other("second instance cleanup".into()));
                }
                _ => {}
            }
            Ok(())
        }
    }

    let count = Arc::new(AtomicUsize::new(0));
    let created = count.clone();
    let second_stopping = Arc::new(tokio::sync::Notify::new());
    let result = vx
        .deploy_with(
            move |_, _: Arc<()>| Instance {
                index: created.fetch_add(1, Ordering::SeqCst),
                second_stopping: second_stopping.clone(),
            },
            DeploymentOptions::new().instances(NonZeroUsize::new(4).unwrap()),
        )
        .await;
    let Err(VertxError::DeploymentFailed { cause, rollback }) = result else {
        panic!("expected startup and rollback errors, got {result:?}");
    };
    assert!(matches!(*cause, VertxError::StartFailed(message) if message == "third instance"));
    assert!(
        matches!(rollback.as_slice(), [VertxError::Other(message)] if message == "second instance cleanup")
    );
    assert_eq!(count.load(Ordering::SeqCst), 3);
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
                ctx.event_bus()
                    .consumer::<()>("hang")?
                    .handler(async move |_| {
                        let start = start.take().unwrap();
                        let _guard = OnDrop(d.clone());
                        start.send(()).ok();
                        std::future::pending::<Result<()>>().await
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
    struct Instance {
        index: usize,
        pending: Option<oneshot::Sender<()>>,
        stopped: tokio::sync::mpsc::UnboundedSender<usize>,
    }
    impl Verticle for Instance {
        async fn start(&mut self, ctx: Context) -> Result<()> {
            ctx.event_bus()
                .consumer::<()>("cancel-group")?
                .handler(async |_| Ok(()))?;
            if let Some(pending) = self.pending.take() {
                pending.send(()).unwrap();
                std::future::pending::<()>().await;
            }
            Ok(())
        }

        async fn stop(&mut self, _: Context) -> Result<()> {
            self.stopped.send(self.index).unwrap();
            Ok(())
        }
    }

    let created = AtomicUsize::new(0);
    let (pending, started) = oneshot::channel();
    let pending = Mutex::new(Some(pending));
    let (stopped, mut exits) = tokio::sync::mpsc::unbounded_channel();
    let v = vx.clone();
    let future = tokio::spawn(async move {
        v.deploy_with(
            move |_, _: Arc<()>| {
                let index = created.fetch_add(1, Ordering::SeqCst);
                Instance {
                    index,
                    pending: if index == 1 {
                        pending.lock().unwrap().take()
                    } else {
                        None
                    },
                    stopped: stopped.clone(),
                }
            },
            DeploymentOptions::new().instances(NonZeroUsize::new(3).unwrap()),
        )
        .await
    });
    started.await.unwrap();
    future.abort();
    assert!(future.await.unwrap_err().is_cancelled());
    let mut stopped = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        [exits.recv().await.unwrap(), exits.recv().await.unwrap()]
    })
    .await
    .unwrap();
    stopped.sort_unstable();
    assert_eq!(stopped, [0, 1]);
    assert!(matches!(
        vx.event_bus().try_send("cancel-group", ()),
        Err(VertxError::NoConsumer(_))
    ));
    vx.close().await.unwrap();
}
