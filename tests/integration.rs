//! Contract tests for the Vert.x 5-style execution core.
//! Filter a suite with `cargo test --test integration streams::`.
//! Each case owns its instance and explicitly awaits close; no shared runtime fixture.
use async_trait::async_trait;
use rstest::{fixture, rstest};
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use vertx_rs::{
    Context, DeliveryOptions, DeploymentOptions, Result, ThreadingModel, Verticle, Vertx,
    VertxError, WorkerVerticle,
};

mod support {
    use super::*;

    #[fixture]
    pub(super) fn vx() -> Vertx {
        Vertx::builder()
            .event_loop_threads(2)
            .worker_threads(2)
            .shutdown_grace(Duration::from_millis(500))
            .build()
    }

    pub(super) struct Local<F>(Option<F>);

    pub(super) fn local<F, Fut>(f: F) -> Local<F>
    where
        F: FnOnce(Context) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        Local(Some(f))
    }

    #[async_trait(?Send)]
    impl<F, Fut> Verticle for Local<F>
    where
        F: FnOnce(Context) -> Fut + 'static,
        Fut: Future<Output = Result<()>> + 'static,
    {
        async fn start(&mut self, ctx: Context) -> Result<()> {
            self.0.take().unwrap()(ctx).await
        }
    }

    pub(super) struct Worker<F>(Option<F>);

    pub(super) fn worker<F, Fut>(f: F) -> Worker<F>
    where
        F: FnOnce(Context) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        Worker(Some(f))
    }

    #[async_trait]
    impl<F, Fut> WorkerVerticle for Worker<F>
    where
        F: FnOnce(Context) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        async fn start(&mut self, ctx: Context) -> Result<()> {
            self.0.take().unwrap()(ctx).await
        }
    }

    pub(super) async fn capture(vx: &Vertx) -> (vertx_rs::DeploymentId, Context) {
        let (tx, rx) = oneshot::channel();
        let id = vx
            .deploy(move |_| {
                local(move |ctx| async move {
                    tx.send(ctx).ok();
                    Ok(())
                })
            })
            .await
            .unwrap();
        (id, rx.await.unwrap())
    }

    pub(super) async fn echo(vx: &Vertx, address: &'static str) -> vertx_rs::DeploymentId {
        vx.deploy(move |_| {
            local(move |ctx| async move {
                ctx.event_bus()
                    .consumer::<String>(address)?
                    .handler(|env| async move {
                        let body = env.body().clone();
                        env.reply(body)
                    })
            })
        })
        .await
        .unwrap()
    }

    pub(super) async fn capture_worker(vx: &Vertx) -> (vertx_rs::DeploymentId, Context) {
        let (tx, rx) = oneshot::channel();
        let id = vx
            .deploy_worker(move |_| {
                worker(move |ctx| async move {
                    tx.send(ctx).ok();
                    Ok(())
                })
            })
            .await
            .unwrap();
        (id, rx.await.unwrap())
    }

    pub(super) struct WorkerDropProbe {
        pub(super) id: Option<vertx_rs::DeploymentId>,
        pub(super) dropped: Arc<AtomicUsize>,
        pub(super) panic: bool,
    }

    impl Drop for WorkerDropProbe {
        fn drop(&mut self) {
            assert!(Context::is_on_worker_thread());
            assert_eq!(Context::current().unwrap().deployment_id(), self.id);
            self.dropped.fetch_add(1, Ordering::SeqCst);
            assert!(!self.panic, "worker destructor panic");
        }
    }

    #[derive(Clone, Default)]
    pub(super) struct TraceOutput(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for TraceOutput {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for TraceOutput {
        type Writer = Self;
        fn make_writer(&'a self) -> Self {
            self.clone()
        }
    }

    impl TraceOutput {
        pub(super) fn text(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
        pub(super) fn dispatch(&self) -> tracing::Dispatch {
            // Keep two dispatchers registered so tracing-core checks callsite
            // interest against registered subscribers. Its single-dispatcher fast
            // path can cache Never if a parallel test first visits a callsite
            // without a thread-local default. This dispatcher is never installed.
            static INACTIVE: std::sync::OnceLock<tracing::Dispatch> = std::sync::OnceLock::new();
            INACTIVE.get_or_init(|| {
                tracing::Dispatch::new(
                    tracing_subscriber::fmt()
                        .with_max_level(tracing_subscriber::filter::LevelFilter::OFF)
                        .with_writer(std::io::sink)
                        .finish(),
                )
            });
            tracing::Dispatch::new(
                tracing_subscriber::fmt()
                    .with_max_level(tracing::Level::TRACE)
                    .with_ansi(false)
                    .without_time()
                    .with_writer(self.clone())
                    .finish(),
            )
        }
    }

    #[derive(Default)]
    pub(super) struct PipeProbe {
        pub(super) calls: Mutex<Vec<&'static str>>,
        pub(super) values: Mutex<Vec<u32>>,
        pub(super) ready: std::sync::atomic::AtomicBool,
        pub(super) wake: Mutex<Option<std::task::Waker>>,
    }

    pub(super) struct ProbeSink {
        pub(super) probe: Arc<PipeProbe>,
        pub(super) fail: Option<&'static str>,
        pub(super) fail_close: bool,
        pub(super) closed: bool,
        pub(super) flush_gate: Option<oneshot::Receiver<()>>,
        pub(super) close_gate: Option<oneshot::Receiver<()>>,
    }

    impl ProbeSink {
        pub(super) fn new(probe: Arc<PipeProbe>) -> Self {
            Self {
                probe,
                fail: None,
                fail_close: false,
                closed: false,
                flush_gate: None,
                close_gate: None,
            }
        }
    }

    impl futures::Sink<u32> for ProbeSink {
        type Error = VertxError;
        fn poll_ready(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<()>> {
            if self.fail == Some("ready") {
                return std::task::Poll::Ready(Err(VertxError::Other("ready".into())));
            }
            if self.closed {
                return std::task::Poll::Ready(Err(VertxError::Other("closed".into())));
            }
            if self.probe.ready.load(Ordering::SeqCst) {
                std::task::Poll::Ready(Ok(()))
            } else {
                *self.probe.wake.lock().unwrap() = Some(cx.waker().clone());
                std::task::Poll::Pending
            }
        }
        fn start_send(self: std::pin::Pin<&mut Self>, value: u32) -> Result<()> {
            if self.fail == Some("write") {
                return Err(VertxError::Other("write".into()));
            }
            self.probe.values.lock().unwrap().push(value);
            Ok(())
        }
        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<()>> {
            if let Some(gate) = &mut self.flush_gate {
                if std::pin::Pin::new(gate).poll(cx).is_pending() {
                    return std::task::Poll::Pending;
                }
                self.flush_gate = None;
            }
            self.probe.calls.lock().unwrap().push("flush");
            std::task::Poll::Ready(if self.fail == Some("flush") {
                Err(VertxError::Other("flush".into()))
            } else {
                Ok(())
            })
        }
        fn poll_close(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<()>> {
            if let Some(gate) = &mut self.close_gate {
                if std::pin::Pin::new(gate).poll(cx).is_pending() {
                    return std::task::Poll::Pending;
                }
                self.close_gate = None;
            }
            self.closed = true;
            self.probe.calls.lock().unwrap().push("close");
            std::task::Poll::Ready(if self.fail_close {
                Err(VertxError::Other("close".into()))
            } else {
                Ok(())
            })
        }
    }
}

use support::*;

mod configuration {
    //! Builder defaults and explicit overrides.
    use super::*;

    #[rstest]
    #[case::default(Vertx::builder())]
    #[case::explicit(Vertx::builder().event_loop_threads(1).worker_threads(2))]
    #[tokio::test]
    async fn fallible_builder_creates_a_closable_instance(#[case] builder: vertx_rs::VertxBuilder) {
        let vx = builder.try_build().unwrap();
        assert!(vx.event_loop_count() > 0);
        assert!(vx.worker_thread_count() > 0);
        vx.close().await.unwrap();
        assert_eq!(vx.diagnostics().close_stage, "closed");
    }

    #[rstest]
    #[case::event_loop(0, 1, Duration::from_secs(1))]
    #[case::worker(1, 0, Duration::from_secs(1))]
    #[case::grace(1, 1, Duration::ZERO)]
    #[test]
    #[should_panic(expected = "failed to initialize Vertx")]
    fn infallible_builder_keeps_panic_behavior(
        #[case] event_loops: usize,
        #[case] workers: usize,
        #[case] grace: Duration,
    ) {
        let _ = Vertx::builder()
            .event_loop_threads(event_loops)
            .worker_threads(workers)
            .shutdown_grace(grace)
            .build();
    }

    #[rstest]
    #[tokio::test]
    async fn zero_instances_keeps_one_instance_compatibility(vx: Vertx) {
        let ids = vx
            .deploy_with(
                |_, _| local(|_| async { Ok(()) }),
                DeploymentOptions::new().instances(0),
            )
            .await
            .unwrap();
        assert_eq!(ids.len(), 1);
        vx.close().await.unwrap();
    }

    #[tokio::test]
    async fn default_worker_pool_size_is_20() {
        let vx = Vertx::builder().event_loop_threads(1).build();
        assert_eq!(vx.worker_thread_count(), 20);
        vx.close().await.unwrap();
    }

    #[tokio::test]
    async fn builder_overrides() {
        let vx = Vertx::builder()
            .event_loop_threads(3)
            .worker_threads(4)
            .build();
        assert_eq!(vx.event_loop_count(), 3);
        assert_eq!(vx.worker_thread_count(), 4);
        vx.close().await.unwrap();
    }
}

mod lifecycle {
    //! Deployment readiness, rollback, cancellation and resource ownership.
    use super::*;

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
}

mod shutdown {
    //! Stop/close ordering, deadlines, joins and deadlock rejection.
    use super::*;

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
}

mod context {
    //! Logical Context isolation, physical thread affinity and managed dispatch.
    use super::*;

    #[rstest]
    #[tokio::test]
    async fn verticle_pinned_to_single_thread(vx: Vertx) {
        struct Affinity {
            samples: Arc<Mutex<Vec<std::thread::ThreadId>>>,
            non_send: Rc<Cell<u64>>,
        }
        impl Drop for Affinity {
            fn drop(&mut self) {
                self.samples
                    .lock()
                    .unwrap()
                    .push(std::thread::current().id());
                assert!(Context::current().is_some());
            }
        }
        #[async_trait(?Send)]
        impl Verticle for Affinity {
            async fn start(&mut self, ctx: Context) -> Result<()> {
                self.samples
                    .lock()
                    .unwrap()
                    .push(std::thread::current().id());
                let (samples, count) = (self.samples.clone(), self.non_send.clone());
                ctx.event_bus()
                    .consumer::<()>("affinity")?
                    .handler(move |env| {
                        let (samples, count) = (samples.clone(), count.clone());
                        async move {
                            samples.lock().unwrap().push(std::thread::current().id());
                            tokio::task::yield_now().await;
                            let ctx = Context::current().unwrap();
                            ctx.execute_blocking(|| 42).await?;
                            samples.lock().unwrap().push(std::thread::current().id());
                            count.set(count.get() + 1);
                            env.reply(count.get())
                        }
                    })
            }
            async fn stop(&mut self, _: Context) -> Result<()> {
                tokio::task::yield_now().await;
                self.samples
                    .lock()
                    .unwrap()
                    .push(std::thread::current().id());
                Ok(())
            }
        }
        let samples = Arc::new(Mutex::new(Vec::new()));
        let s = samples.clone();
        let id = vx
            .deploy(move |_| {
                s.lock().unwrap().push(std::thread::current().id());
                Affinity {
                    samples: s,
                    non_send: Rc::new(Cell::new(0)),
                }
            })
            .await
            .unwrap();
        for i in 1..=20 {
            assert_eq!(
                vx.event_bus()
                    .request::<_, u64>("affinity", ())
                    .await
                    .unwrap(),
                i
            );
        }
        vx.undeploy(id).await.unwrap();
        {
            let s = samples.lock().unwrap();
            assert!(s.len() >= 44);
            assert!(s.iter().all(|t| *t == s[0]));
        }
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn verticle_can_hold_non_send_state_directly(vx: Vertx) {
        vx.deploy(|_| {
            local(|ctx| async move {
                let count = Cell::new(0u64);
                ctx.event_bus().consumer::<()>("inc")?.handler(move |env| {
                    count.set(count.get() + 1);
                    let n = count.get();
                    async move { env.reply(n) }
                })
            })
        })
        .await
        .unwrap();
        for n in 1..=3 {
            assert_eq!(
                vx.event_bus().request::<_, u64>("inc", ()).await.unwrap(),
                n
            );
        }
        vx.close().await.unwrap();
    }

    #[tokio::test]
    async fn context_current_is_none_outside_vertx() {
        assert!(Context::current().is_none());
        assert!(!Context::is_on_vertx_thread());
    }

    #[rstest]
    #[tokio::test]
    async fn context_introspection_event_loop_full(vx: Vertx) {
        let (id, ctx) = capture(&vx).await;
        assert_eq!(ctx.threading_model(), ThreadingModel::EventLoop);
        assert_eq!(ctx.deployment_id(), Some(id));
        assert!(ctx.owner().is_some());
        assert!(ctx.event_loop_id().is_some());
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn context_introspection_worker_full(vx: Vertx) {
        vx.deploy_worker(|_| {
            worker(|ctx| async move {
                assert_eq!(ctx.threading_model(), ThreadingModel::Worker);
                assert!(ctx.event_loop_id().is_none());
                assert!(Context::is_on_worker_thread());
                let id = ctx.deployment_id();
                tokio::task::yield_now().await;
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
    async fn external_thread_can_hop_into_event_loop_via_run_on_context(vx: Vertx) {
        let (_, ctx) = capture(&vx).await;
        let (tx, rx) = oneshot::channel();
        let id = ctx.deployment_id();
        std::thread::spawn(move || {
            ctx.run_on_context(async move {
                assert!(Context::is_on_event_loop_thread());
                tx.send(Context::current().unwrap().deployment_id()).ok();
                Ok(())
            })
            .unwrap()
        })
        .join()
        .unwrap();
        assert_eq!(rx.await.unwrap(), id);
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn run_on_context_after_undeploy_returns_terminated(vx: Vertx) {
        let (id, ctx) = capture(&vx).await;
        vx.undeploy(id).await.unwrap();
        assert!(matches!(
            ctx.run_on_context(async { Ok(()) }),
            Err(VertxError::DeploymentTerminated(_))
        ));
        vx.close().await.unwrap();
    }

    #[tokio::test]
    async fn two_contexts_on_one_event_loop_never_leak_identity() {
        let vx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(2)
            .build();
        for address in ["a", "b"] {
            vx.deploy(move |_| {
                local(move |ctx| async move {
                    let id = ctx.deployment_id();
                    ctx.event_bus()
                        .consumer::<()>(address)?
                        .handler(move |env| async move {
                            for _ in 0..100 {
                                tokio::task::yield_now().await;
                                assert_eq!(Context::current().unwrap().deployment_id(), id);
                            }
                            env.reply(id)
                        })
                })
            })
            .await
            .unwrap();
        }
        let bus = vx.event_bus();
        let (a, b) = tokio::join!(
            bus.request::<_, Option<vertx_rs::DeploymentId>>("a", ()),
            bus.request::<_, Option<vertx_rs::DeploymentId>>("b", ())
        );
        assert_ne!(a.unwrap(), b.unwrap());
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn raw_tokio_spawn_has_no_logical_context(vx: Vertx) {
        vx.deploy(|_| {
            local(|ctx| async move {
                let own = ctx.deployment_id();
                tokio::spawn(async {
                    assert!(Context::current().is_none());
                    assert!(Context::is_on_event_loop_thread());
                })
                .await?;
                assert_eq!(Context::current().unwrap().deployment_id(), own);
                Ok(())
            })
        })
        .await
        .unwrap();
        vx.close().await.unwrap();
    }

    #[tokio::test]
    async fn spawn_local_rejects_other_context_even_on_same_thread() {
        let vx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(2)
            .build();
        let (_, a) = capture(&vx).await;
        let (tx, rx) = oneshot::channel();
        let other = a.clone();
        vx.deploy(move |_| {
            local(move |ctx| async move {
                assert!(matches!(
                    other.spawn_local(async { Ok(()) }),
                    Err(VertxError::ContextMismatch)
                ));
                assert!(matches!(
                    other
                        .event_bus()
                        .consumer::<()>("wrong")?
                        .handler(|_| async { Ok(()) }),
                    Err(VertxError::ContextMismatch)
                ));
                let id = ctx.deployment_id();
                ctx.spawn_local(async move {
                    tokio::task::yield_now().await;
                    assert_eq!(Context::current().unwrap().deployment_id(), id);
                    tx.send(()).ok();
                    Ok(())
                })?;
                Ok(())
            })
        })
        .await
        .unwrap();
        rx.await.unwrap();
        assert!(matches!(
            a.spawn_local(async { Ok(()) }),
            Err(VertxError::ContextMismatch)
        ));
        vx.close().await.unwrap();
    }

    #[tokio::test]
    async fn spawn_local_rejects_another_vertx_instance() {
        let a = vx();
        let b = vx();
        let (_, ctx) = capture(&a).await;
        b.deploy(move |_| {
            local(move |_| async move {
                assert!(matches!(
                    ctx.spawn_local(async { Ok(()) }),
                    Err(VertxError::ContextMismatch)
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
        a.close().await.unwrap();
        b.close().await.unwrap();
    }

    #[tokio::test]
    async fn same_eventloop_send_spawn_switches_context_without_running_inline() {
        let vx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(1)
            .build();
        let (_, first) = capture(&vx).await;
        let (_, second) = capture(&vx).await;
        let first_id = first.deployment_id();
        let second_id = second.deployment_id();
        let ran = Arc::new(AtomicUsize::new(0));
        let observed = ran.clone();
        first
            .spawn(async move {
                let thread = std::thread::current().id();
                assert_eq!(Context::current().unwrap().deployment_id(), first_id);
                let task = second.spawn(async move {
                    assert_eq!(std::thread::current().id(), thread);
                    assert_eq!(Context::current().unwrap().deployment_id(), second_id);
                    ran.fetch_add(1, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                    assert_eq!(Context::current().unwrap().deployment_id(), second_id);
                    Ok(())
                })?;
                assert_eq!(observed.load(Ordering::SeqCst), 0);
                assert_eq!(Context::current().unwrap().deployment_id(), first_id);
                task.await?;
                assert_eq!(observed.load(Ordering::SeqCst), 1);
                assert_eq!(Context::current().unwrap().deployment_id(), first_id);
                Ok(())
            })
            .unwrap()
            .await
            .unwrap();
        vx.close().await.unwrap();
    }

    #[tokio::test]
    async fn queued_cross_thread_abort_preserves_target_context_during_destruction() {
        struct DropCheck {
            id: Option<vertx_rs::DeploymentId>,
            dropped: Arc<AtomicUsize>,
        }
        impl Drop for DropCheck {
            fn drop(&mut self) {
                assert!(Context::is_on_event_loop_thread());
                assert_eq!(Context::current().unwrap().deployment_id(), self.id);
                self.dropped.fetch_add(1, Ordering::SeqCst);
            }
        }
        let vx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(1)
            .build();
        let (_, ctx) = capture(&vx).await;
        let (entered, started) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let blocker = ctx
            .spawn(async move {
                entered.send(()).ok();
                wait.recv().unwrap();
                Ok(())
            })
            .unwrap();
        started.await.unwrap();
        let dropped = Arc::new(AtomicUsize::new(0));
        let guard = DropCheck {
            id: ctx.deployment_id(),
            dropped: dropped.clone(),
        };
        let task = ctx
            .spawn(async move {
                let _guard = guard;
                panic!("queued cancelled task was polled");
                #[allow(unreachable_code)]
                Ok(())
            })
            .unwrap();
        let abort = task.abort_handle();
        abort.abort();
        assert!(!abort.is_finished());
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        release.send(()).unwrap();
        assert!(matches!(task.await, Err(VertxError::TaskCancelled)));
        assert!(abort.is_finished());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        blocker.await.unwrap();
        assert!(Context::current().is_none());
        vx.close().await.unwrap();
    }
}

mod worker {
    //! Native Worker scheduling, parallelism, migration and destructor ownership.
    use super::*;

    #[rstest]
    #[tokio::test]
    async fn worker_verticle_processes_messages(vx: Vertx) {
        vx.deploy_worker(|_| {
            worker(|ctx| async move {
                ctx.event_bus()
                    .consumer::<u64>("worker")?
                    .handler_worker(|env| async move {
                        assert!(Context::is_on_worker_thread());
                        let n = *env.body() * 2;
                        env.reply(n)
                    })
            })
        })
        .await
        .unwrap();
        assert_eq!(
            vx.event_bus()
                .request::<_, u64>("worker", 21u64)
                .await
                .unwrap(),
            42
        );
        vx.close().await.unwrap();
    }

    #[tokio::test]
    async fn worker_context_serializes_all_ordinary_executions() {
        let vx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(4)
            .build();
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = oneshot::channel();
        let (a, m) = (active.clone(), maximum.clone());
        vx.deploy_worker(move |_| {
            worker(move |ctx| async move {
                let id = ctx.deployment_id();
                let mut handles = Vec::new();
                for _ in 0..32 {
                    let (a, m) = (a.clone(), m.clone());
                    handles.push(ctx.spawn(async move {
                        for _ in 0..5 {
                            let n = a.fetch_add(1, Ordering::SeqCst) + 1;
                            m.fetch_max(n, Ordering::SeqCst);
                            assert_eq!(Context::current().unwrap().deployment_id(), id);
                            std::thread::sleep(Duration::from_micros(100));
                            a.fetch_sub(1, Ordering::SeqCst);
                            tokio::task::yield_now().await;
                        }
                        Ok(())
                    })?);
                }
                ctx.spawn(async move {
                    for handle in handles {
                        handle.await?;
                    }
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
        assert_eq!(maximum.load(Ordering::SeqCst), 1);
        vx.close().await.unwrap();
    }

    #[tokio::test]
    async fn worker_contexts_can_execute_in_parallel() {
        let vx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(4)
            .build();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let mut jobs = Vec::new();
        for _ in 0..2 {
            let v = vx.clone();
            let b = barrier.clone();
            jobs.push(tokio::spawn(async move {
                v.deploy_worker(move |_| {
                    worker(move |_| async move {
                        b.wait();
                        Ok(())
                    })
                })
                .await
            }));
        }
        for job in jobs {
            tokio::time::timeout(Duration::from_secs(2), job)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        vx.close().await.unwrap();
    }

    #[tokio::test]
    async fn single_worker_reuses_thread_while_future_waits_and_can_await_blocking() {
        let vx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(1)
            .build();
        let (_, ctx) = capture_worker(&vx).await;
        let (entered, started) = oneshot::channel();
        let (release, wait) = oneshot::channel();
        let id = ctx.deployment_id();
        let first = ctx
            .spawn(async move {
                entered.send(std::thread::current().id()).ok();
                wait.await.unwrap();
                assert_eq!(Context::current().unwrap().deployment_id(), id);
                Ok(())
            })
            .unwrap();
        let thread = started.await.unwrap();
        let c = ctx.clone();
        let second = ctx
            .spawn(async move {
                assert_eq!(std::thread::current().id(), thread);
                assert_eq!(
                    tokio::runtime::Handle::current().runtime_flavor(),
                    tokio::runtime::RuntimeFlavor::CurrentThread
                );
                let result = c
                    .execute_blocking(move || {
                        assert_eq!(std::thread::current().id(), thread);
                        assert!(Context::is_on_worker_thread());
                        assert_eq!(Context::current().unwrap().deployment_id(), id);
                        42
                    })
                    .await?;
                assert_eq!(result, 42);
                assert_eq!(Context::current().unwrap().deployment_id(), id);
                release.send(()).ok();
                Ok(())
            })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            first.await?;
            second.await
        })
        .await
        .unwrap()
        .unwrap();
        vx.close().await.unwrap();
    }

    #[tokio::test]
    async fn worker_and_blocking_use_exactly_the_same_fixed_thread_set() {
        use std::collections::HashSet;
        let vx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(2)
            .build();
        let threads = Arc::new(Mutex::new(HashSet::new()));
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let mut deployments = Vec::new();
        for _ in 0..2 {
            let (v, threads, barrier) = (vx.clone(), threads.clone(), barrier.clone());
            deployments.push(tokio::spawn(async move {
                v.deploy_worker(move |_| {
                    worker(move |_| async move {
                        threads.lock().unwrap().insert(std::thread::current().id());
                        barrier.wait();
                        Ok(())
                    })
                })
                .await
            }));
        }
        for deployment in deployments {
            deployment.await.unwrap().unwrap();
        }
        let (_, ctx) = capture(&vx).await;
        let blocking_threads = Arc::new(Mutex::new(HashSet::new()));
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let jobs = (0..2).map(|_| {
            let (barrier, threads) = (barrier.clone(), blocking_threads.clone());
            ctx.execute_blocking_with(
                move || {
                    threads.lock().unwrap().insert(std::thread::current().id());
                    barrier.wait();
                },
                false,
            )
        });
        for result in futures::future::join_all(jobs).await {
            result.unwrap();
        }
        assert_eq!(threads.lock().unwrap().len(), 2);
        assert_eq!(*threads.lock().unwrap(), *blocking_threads.lock().unwrap());
        vx.close().await.unwrap();
    }

    #[tokio::test]
    async fn worker_can_use_eventloop_io_and_timer_drivers_without_changing_executor() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let vx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(1)
            .build();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut byte = [0];
            socket.read_exact(&mut byte).await.unwrap();
            socket.write_all(&[byte[0] + 1]).await.unwrap();
        });
        vx.deploy_worker(move |_| {
            worker(move |ctx| async move {
                let id = ctx.deployment_id();
                let thread = std::thread::current().id();
                let mut socket = tokio::time::timeout(
                    Duration::from_secs(2),
                    tokio::net::TcpStream::connect(address),
                )
                .await
                .unwrap()
                .unwrap();
                socket.write_all(&[41]).await.unwrap();
                let mut byte = [0];
                socket.read_exact(&mut byte).await.unwrap();
                assert_eq!(byte, [42]);
                ctx.timer(Duration::from_millis(1))?.await?;
                let mut periodic = ctx.periodic(Duration::ZERO, Duration::from_millis(1))?;
                periodic.tick().await?;
                assert_eq!(Context::current().unwrap().deployment_id(), id);
                assert_eq!(std::thread::current().id(), thread);
                assert!(Context::is_on_worker_thread());
                Ok(())
            })
        })
        .await
        .unwrap();
        server.await.unwrap();
        vx.close().await.unwrap();
    }

    #[tokio::test]
    async fn worker_abort_before_first_poll_drops_on_owner_and_finishes_after_drop() {
        let vx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(1)
            .build();
        let (_, ctx) = capture_worker(&vx).await;
        let (entered, started) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let c = ctx.clone();
        let blocker = tokio::spawn(async move {
            c.execute_blocking(move || {
                entered.send(()).ok();
                wait.recv().unwrap();
            })
            .await
        });
        started.await.unwrap();
        let dropped = Arc::new(AtomicUsize::new(0));
        let probe = WorkerDropProbe {
            id: ctx.deployment_id(),
            dropped: dropped.clone(),
            panic: false,
        };
        let task = ctx
            .spawn(async move {
                let _probe = probe;
                panic!("cancelled future was polled");
                #[allow(unreachable_code)]
                Ok(())
            })
            .unwrap();
        let abort = task.abort_handle();
        task.abort();
        assert!(!abort.is_finished());
        release.send(()).unwrap();
        assert!(matches!(task.await, Err(VertxError::TaskCancelled)));
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert!(abort.is_finished());
        blocker.await.unwrap().unwrap();
        vx.close().await.unwrap();
        abort.abort(); // Retaining a cancellation handle must not retain executor threads.
    }

    #[tokio::test]
    async fn worker_destructor_panic_does_not_kill_thread_or_leak_context() {
        let vx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(1)
            .build();
        let (_, ctx) = capture_worker(&vx).await;
        let dropped = Arc::new(AtomicUsize::new(0));
        let probe = WorkerDropProbe {
            id: ctx.deployment_id(),
            dropped: dropped.clone(),
            panic: true,
        };
        let (entered, started) = oneshot::channel();
        let task = ctx
            .spawn(async move {
                let _probe = probe;
                entered.send(()).ok();
                std::future::pending::<Result<()>>().await
            })
            .unwrap();
        started.await.unwrap();
        task.abort();
        assert!(matches!(task.await, Err(VertxError::Panic(_))));
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        ctx.spawn(async {
            assert!(Context::current().is_some());
            Ok(())
        })
        .unwrap()
        .await
        .unwrap();
        assert!(Context::current().is_none());
        vx.close().await.unwrap();
    }

    #[tokio::test]
    async fn future_combinations_preserve_worker_context_and_keep_spawned_tasks_independent() {
        use futures::{FutureExt, future};
        let vx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(1)
            .build();
        let (_, ctx) = capture_worker(&vx).await;
        let owner = ctx.clone();
        ctx.spawn(async move {
            let id = Context::current().unwrap().deployment_id();
            let futures = (0..3u8).map(|n| async move {
                tokio::task::yield_now().await;
                assert_eq!(Context::current().unwrap().deployment_id(), id);
                Ok::<_, VertxError>(n)
            });
            assert_eq!(future::try_join_all(futures).await?, vec![0, 1, 2]);
            let outcomes = future::join_all(vec![
                future::ready(Ok::<_, VertxError>(1)),
                future::ready(Err(VertxError::Other("expected".into()))),
            ])
            .await;
            assert!(outcomes[0].is_ok());
            assert!(outcomes[1].is_err());
            let (value, remaining) = future::select_ok(vec![
                future::ready(Err::<u8, _>(VertxError::Other("first failed".into()))).boxed(),
                async move {
                    tokio::task::yield_now().await;
                    assert_eq!(Context::current().unwrap().deployment_id(), id);
                    Ok(42u8)
                }
                .boxed(),
            ])
            .await?;
            assert_eq!(value, 42);
            drop(remaining);
            assert!(
                future::select_ok(vec![future::ready(Err::<u8, _>(VertxError::Other(
                    "all failed".into()
                )))])
                .await
                .is_err()
            );

            // Fail-fast drops a handle, but the submitted task remains managed.
            let (entered, started) = oneshot::channel();
            let (release, wait) = oneshot::channel();
            let (done, finished) = oneshot::channel();
            let child = owner.spawn(async move {
                entered.send(()).ok();
                wait.await.unwrap();
                done.send(()).ok();
                Ok(())
            })?;
            let abort = child.abort_handle();
            started.await.unwrap();
            let result = future::try_join(
                future::ready(Err::<(), _>(VertxError::Other("fail fast".into()))),
                child,
            )
            .await;
            assert!(result.is_err());
            assert!(!abort.is_finished());
            release.send(()).ok();
            finished.await.unwrap();
            assert_eq!(Context::current().unwrap().deployment_id(), id);
            Ok(())
        })
        .unwrap()
        .await
        .unwrap();
        vx.close().await.unwrap();
    }

    #[tokio::test]
    async fn worker_cancellation_destructor_serializes_with_ordinary_polls() {
        struct DropGate {
            entered: Option<oneshot::Sender<()>>,
            release: std::sync::mpsc::Receiver<()>,
            active: Arc<AtomicUsize>,
            id: Option<vertx_rs::DeploymentId>,
        }
        impl Drop for DropGate {
            fn drop(&mut self) {
                assert_eq!(Context::current().unwrap().deployment_id(), self.id);
                self.active.store(1, Ordering::SeqCst);
                self.entered.take().unwrap().send(()).ok();
                self.release.recv().unwrap();
                self.active.store(0, Ordering::SeqCst);
            }
        }
        let vx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(2)
            .build();
        let (_, ctx) = capture_worker(&vx).await;
        let (_, other) = capture_worker(&vx).await;
        let active = Arc::new(AtomicUsize::new(0));
        let (entered, dropping) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let gate = DropGate {
            entered: Some(entered),
            release: wait,
            active: active.clone(),
            id: ctx.deployment_id(),
        };
        let (entered, running) = oneshot::channel();
        let task = ctx
            .spawn(async move {
                let _gate = gate;
                entered.send(()).ok();
                std::future::pending::<Result<()>>().await
            })
            .unwrap();
        running.await.unwrap();
        task.abort();
        dropping.await.unwrap();
        let same = active.clone();
        let peer = ctx
            .spawn(async move {
                assert_eq!(same.load(Ordering::SeqCst), 0);
                Ok(())
            })
            .unwrap();
        let independent = other
            .spawn(async move {
                assert_eq!(active.load(Ordering::SeqCst), 1);
                Ok(())
            })
            .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(2), independent).await;
        release.send(()).unwrap();
        result.unwrap().unwrap();
        assert!(matches!(task.await, Err(VertxError::TaskCancelled)));
        peer.await.unwrap();
        vx.close().await.unwrap();
    }
}

mod blocking {
    //! Shared fixed pool, ordered admission and non-preemptible work.
    use super::*;

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
    async fn unordered_blocking_obeys_configured_limit_and_can_overlap(
        vx: Vertx,
        #[case] named: bool,
    ) {
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
}

mod eventbus {
    //! Typed routing, delivery, request/reply and bounded queue admission.
    use super::*;

    #[rstest]
    #[tokio::test]
    async fn verticles_communicate_through_their_bound_buses(vx: Vertx) {
        echo(&vx, "upstream").await;
        vx.deploy(|_| {
            local(|ctx| async move {
                let bus = ctx.event_bus();
                bus.consumer::<String>("caller")?.handler(move |env| {
                    let bus = bus.clone();
                    async move {
                        let response: String = bus.request("upstream", env.body().clone()).await?;
                        env.reply(response)
                    }
                })
            })
        })
        .await
        .unwrap();
        assert_eq!(
            vx.event_bus()
                .request::<_, String>("caller", "hello".to_owned())
                .await
                .unwrap(),
            "hello"
        );
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn bound_consumer_after_undeploy_returns_terminated(vx: Vertx) {
        let (id, ctx) = capture(&vx).await;
        vx.undeploy(id).await.unwrap();
        assert!(matches!(
            ctx.event_bus().consumer::<()>("late"),
            Err(VertxError::DeploymentTerminated(_))
        ));
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn publish_delivers_to_all_subscribers(vx: Vertx) {
        let bus = vx.event_bus();
        let mut a = bus.consumer::<u32>("pub").unwrap();
        let mut b = bus.consumer::<u32>("pub").unwrap();
        bus.publish("pub", 7u32).unwrap();
        assert_eq!(a.recv().await.unwrap().unwrap().into_body(), 7);
        assert_eq!(b.recv().await.unwrap().unwrap().into_body(), 7);
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn send_with_no_consumer_errors(vx: Vertx) {
        assert!(matches!(
            vx.event_bus().send("missing", ()).await,
            Err(VertxError::NoConsumer(_))
        ));
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn request_timeout_fires(vx: Vertx) {
        let bus = vx.event_bus();
        let _consumer = bus.consumer::<()>("timeout").unwrap();
        assert!(matches!(
            bus.request_with::<_, ()>(
                "timeout",
                (),
                DeliveryOptions::new().with_timeout(Duration::from_millis(5))
            )
            .await,
            Err(VertxError::Timeout(_))
        ));
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn dropping_consumer_unregisters(vx: Vertx) {
        let consumer = vx.event_bus().consumer::<()>("drop").unwrap();
        drop(consumer);
        assert!(matches!(
            vx.event_bus().try_send("drop", ()),
            Err(VertxError::NoConsumer(_))
        ));
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn consumer_handler_dispatches_envelopes(vx: Vertx) {
        echo(&vx, "handler").await;
        for n in 0..10 {
            assert_eq!(
                vx.event_bus()
                    .request::<_, String>("handler", n.to_string())
                    .await
                    .unwrap(),
                n.to_string()
            );
        }
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn consumer_handler_unregisters_on_undeploy(vx: Vertx) {
        let id = echo(&vx, "handler").await;
        vx.undeploy(id).await.unwrap();
        assert!(matches!(
            vx.event_bus().try_send("handler", "x".to_owned()),
            Err(VertxError::NoConsumer(_))
        ));
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn message_passing_between_handlers(vx: Vertx) {
        vx.deploy(|_| {
            local(|ctx| async move {
                ctx.event_bus()
                    .consumer::<u32>("double")?
                    .handler(|env| async move {
                        let n = *env.body() * 2;
                        env.reply(n)
                    })?;
                let bus = ctx.event_bus();
                bus.consumer::<u32>("forward")?.handler(move |env| {
                    let bus = bus.clone();
                    async move {
                        let n: u32 = bus.request("double", *env.body()).await?;
                        env.reply(n)
                    }
                })
            })
        })
        .await
        .unwrap();
        assert_eq!(
            vx.event_bus()
                .request::<_, u32>("forward", 21u32)
                .await
                .unwrap(),
            42
        );
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn delivery_options_headers_round_trip(vx: Vertx) {
        vx.deploy(|_| {
            local(|ctx| async move {
                ctx.event_bus()
                    .consumer::<()>("headers")?
                    .handler(|env| async move {
                        assert_eq!(
                            env.headers().get_all("tag").collect::<Vec<_>>(),
                            vec!["a", "b"]
                        );
                        env.reply_with(42u32, DeliveryOptions::new().header("reply", "yes"))
                    })
            })
        })
        .await
        .unwrap();
        let reply = vx
            .event_bus()
            .request_message_with::<_, u32>(
                "headers",
                (),
                DeliveryOptions::new().header("tag", "a").header("tag", "b"),
            )
            .await
            .unwrap();
        assert_eq!(*reply.body(), 42);
        assert_eq!(reply.headers().get("reply").unwrap(), "yes");
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn request_timeout_includes_waiting_for_full_queue(vx: Vertx) {
        let bus = vx.event_bus();
        let _consumer = bus.consumer::<u32>("full").unwrap();
        for n in 0..256u32 {
            bus.try_send("full", n).unwrap();
        }
        assert!(matches!(
            bus.try_send("full", 999u32),
            Err(VertxError::QueueFull(_))
        ));
        let result = tokio::time::timeout(
            Duration::from_millis(200),
            bus.request_with::<_, ()>(
                "full",
                999u32,
                DeliveryOptions::new().with_timeout(Duration::from_millis(5)),
            ),
        )
        .await
        .unwrap();
        assert!(matches!(result, Err(VertxError::Timeout(_))));
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn recipient_failure_and_late_reply_are_explicit(vx: Vertx) {
        vx.deploy(|_| {
            local(|ctx| async move {
                ctx.event_bus()
                    .consumer::<()>("business")?
                    .handler(|env| async { env.fail(409, "conflict") })
            })
        })
        .await
        .unwrap();
        assert!(matches!(
            vx.event_bus().request::<_, ()>("business", ()).await,
            Err(VertxError::RecipientFailure { code: 409, .. })
        ));
        let bus = vx.event_bus();
        let mut consumer = bus.consumer::<()>("late").unwrap();
        let b = bus.clone();
        let job = tokio::spawn(async move {
            b.request_with::<_, ()>(
                "late",
                (),
                DeliveryOptions::new().with_timeout(Duration::from_millis(5)),
            )
            .await
        });
        let env = consumer.recv().await.unwrap().unwrap();
        assert!(matches!(job.await.unwrap(), Err(VertxError::Timeout(_))));
        assert!(matches!(env.reply(()), Err(VertxError::ReplyClosed)));
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn type_conflicts_are_rejected_at_registration(vx: Vertx) {
        let bus = vx.event_bus();
        let a = bus.consumer::<u32>("typed").unwrap();
        assert!(matches!(
            bus.consumer::<String>("typed"),
            Err(VertxError::TypeMismatch(_))
        ));
        assert!(matches!(
            bus.try_send("typed", "bad".to_owned()),
            Err(VertxError::TypeMismatch(_))
        ));
        drop(a);
        assert!(bus.consumer::<String>("typed").is_ok());
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn broadcast_drops_only_full_subscriber_and_send_round_robins(vx: Vertx) {
        let bus = vx.event_bus();
        let mut a = bus.consumer::<u32>("pub").unwrap();
        let mut b = bus.consumer::<u32>("pub").unwrap();
        for n in 0..256u32 {
            bus.publish("pub", n).unwrap();
            assert_eq!(b.recv().await.unwrap().unwrap().into_body(), n);
        }
        bus.publish("pub", 999u32).unwrap();
        assert_eq!(b.recv().await.unwrap().unwrap().into_body(), 999);
        for n in 0..256u32 {
            assert_eq!(a.recv().await.unwrap().unwrap().into_body(), n);
        }
        bus.send("pub", 1u32).await.unwrap();
        bus.send("pub", 2u32).await.unwrap();
        assert_eq!(a.recv().await.unwrap().unwrap().into_body(), 1);
        assert_eq!(b.recv().await.unwrap().unwrap().into_body(), 2);
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn closing_unblocks_senders_waiting_for_capacity(vx: Vertx) {
        let bus = vx.event_bus();
        let _c = bus.consumer::<()>("full").unwrap();
        for _ in 0..256 {
            bus.try_send("full", ()).unwrap();
        }
        let b = bus.clone();
        let send = tokio::spawn(async move { b.send("full", ()).await });
        vx.close().await.unwrap();
        assert!(matches!(send.await.unwrap(), Err(VertxError::VertxClosed)));
    }

    #[tokio::test]
    async fn consumer_waits_for_message_future_completion_before_next_message() {
        let vx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(2)
            .build();
        let (entered, started) = oneshot::channel();
        let mut entered = Some(entered);
        let (release, wait) = oneshot::channel();
        let mut wait = Some(wait);
        let (done, finished) = oneshot::channel();
        let mut done = Some(done);
        let order = Arc::new(Mutex::new(Vec::new()));
        let recorded = order.clone();
        vx.deploy(move |_| {
            local(move |ctx| async move {
                ctx.event_bus()
                    .consumer::<u8>("serial-future")?
                    .handler(move |env| {
                        let wait = if *env.body() == 1 { wait.take() } else { None };
                        let entered = entered.take();
                        let done = if *env.body() == 2 { done.take() } else { None };
                        let order = recorded.clone();
                        async move {
                            let value = *env.body();
                            order.lock().unwrap().push(value);
                            if let Some(entered) = entered {
                                entered.send(()).ok();
                            }
                            if let Some(wait) = wait {
                                wait.await.unwrap();
                            }
                            if let Some(done) = done {
                                done.send(()).ok();
                                Ok(())
                            } else {
                                env.reply(value)
                            }
                        }
                    })
            })
        })
        .await
        .unwrap();
        let bus = vx.event_bus();
        let first_bus = bus.clone();
        let first =
            tokio::spawn(async move { first_bus.request::<_, u8>("serial-future", 1u8).await });
        started.await.unwrap();
        // Enqueue the second message, then run another task on the same EventLoop.
        bus.send("serial-future", 2u8).await.unwrap();
        let (_, ctx) = capture(&vx).await;
        ctx.spawn(async { Ok(()) }).unwrap().await.unwrap();
        assert_eq!(*order.lock().unwrap(), vec![1]);
        release.send(()).unwrap();
        assert_eq!(first.await.unwrap().unwrap(), 1);
        finished.await.unwrap();
        assert_eq!(*order.lock().unwrap(), vec![1, 2]);
        vx.close().await.unwrap();
    }

    #[rstest]
    #[case::zero(0)]
    #[case::above_semaphore_limit(usize::MAX)]
    #[tokio::test]
    async fn invalid_consumer_capacity_is_rejected(#[case] capacity: usize, vx: Vertx) {
        assert!(matches!(
            vx.event_bus().consumer_with::<()>(
                "invalid",
                vertx_rs::ConsumerOptions::new().with_capacity(capacity)
            ),
            Err(VertxError::InvalidOptions(_))
        ));
        vx.close().await.unwrap();
    }

    #[tokio::test]
    async fn consumer_capacity_and_waiting_sender_accounting() {
        let vx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(1)
            .diagnostics(vertx_rs::DiagnosticsOptions {
                queue_details: true,
                ..Default::default()
            })
            .build();
        let bus = vx.event_bus();
        let mut consumer = bus
            .consumer_with::<u32>("small", vertx_rs::ConsumerOptions::new().with_capacity(1))
            .unwrap();
        bus.try_send("small", 1u32).unwrap();
        let mut send = Box::pin(bus.send("small", 2u32));
        assert!(futures::poll!(send.as_mut()).is_pending());
        assert!(futures::poll!(send.as_mut()).is_pending());
        let snapshot = vx.diagnostics();
        assert_eq!(
            (
                snapshot.queued_messages,
                snapshot.reserved_slots,
                snapshot.waiting_senders,
                snapshot.queue_full_events
            ),
            (1, 0, 1, 1)
        );
        assert_eq!(snapshot.queues[0].capacity, 1);
        drop(send);
        assert_eq!(vx.diagnostics().waiting_senders, 0);
        assert_eq!(consumer.recv().await.unwrap().unwrap().into_body(), 1);
        assert_eq!(vx.diagnostics().queued_messages, 0);
        drop(consumer);
        assert!(vx.diagnostics().queues.is_empty());
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn publish_does_not_clone_for_a_full_consumer(vx: Vertx) {
        struct CountClone(Arc<AtomicUsize>);
        impl Clone for CountClone {
            fn clone(&self) -> Self {
                self.0.fetch_add(1, Ordering::SeqCst);
                Self(self.0.clone())
            }
        }
        let bus = vx.event_bus();
        let full = bus
            .consumer_with::<CountClone>(
                "copies",
                vertx_rs::ConsumerOptions::new().with_capacity(1),
            )
            .unwrap();
        let clones = Arc::new(AtomicUsize::new(0));
        bus.try_send("copies", CountClone(clones.clone())).unwrap();
        let mut available = bus
            .consumer_with::<CountClone>(
                "copies",
                vertx_rs::ConsumerOptions::new().with_capacity(2),
            )
            .unwrap();
        bus.publish("copies", CountClone(clones.clone())).unwrap();
        assert_eq!(clones.load(Ordering::SeqCst), 1);
        assert!(available.recv().await.unwrap().is_some());
        assert_eq!(vx.diagnostics().broadcast_dropped, 1);
        drop(full);
        drop(available);
        vx.close().await.unwrap();
    }
}

mod shared_state {
    //! Supported ownership patterns for local and cross-Verticle state.
    use super::*;

    #[rstest]
    #[tokio::test]
    async fn rc_refcell_shared_state_across_handlers(vx: Vertx) {
        vx.deploy(|_| {
            local(|ctx| async move {
                let values = Rc::new(RefCell::new(Vec::<u32>::new()));
                let a = values.clone();
                ctx.event_bus()
                    .consumer::<u32>("put")?
                    .handler(move |env| {
                        a.borrow_mut().push(*env.body());
                        async move { env.reply(()) }
                    })?;
                ctx.event_bus().consumer::<()>("get")?.handler(move |env| {
                    let n = values.borrow().len();
                    async move { env.reply(n) }
                })
            })
        })
        .await
        .unwrap();
        vx.event_bus().request::<_, ()>("put", 1u32).await.unwrap();
        assert_eq!(
            vx.event_bus().request::<_, usize>("get", ()).await.unwrap(),
            1
        );
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn rc_cell_shared_scalar_across_handlers(vx: Vertx) {
        vx.deploy(|_| {
            local(|ctx| async move {
                let value = Rc::new(Cell::new(0u32));
                let a = value.clone();
                ctx.event_bus()
                    .consumer::<u32>("put")?
                    .handler(move |env| {
                        a.set(*env.body());
                        async move { env.reply(()) }
                    })?;
                ctx.event_bus().consumer::<()>("get")?.handler(move |env| {
                    let n = value.get();
                    async move { env.reply(n) }
                })
            })
        })
        .await
        .unwrap();
        vx.event_bus().request::<_, ()>("put", 42u32).await.unwrap();
        assert_eq!(
            vx.event_bus().request::<_, u32>("get", ()).await.unwrap(),
            42
        );
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn arc_atomic_counter_shared_across_verticles(vx: Vertx) {
        let count = Arc::new(AtomicUsize::new(0));
        for _ in 0..3 {
            let count = count.clone();
            vx.deploy(move |_| {
                local(move |ctx| async move {
                    ctx.event_bus()
                        .consumer::<()>("count")?
                        .handler(move |env| {
                            let n = count.fetch_add(1, Ordering::SeqCst) + 1;
                            async move { env.reply(n) }
                        })
                })
            })
            .await
            .unwrap();
        }
        for n in 1..=9 {
            assert_eq!(
                vx.event_bus()
                    .request::<_, usize>("count", ())
                    .await
                    .unwrap(),
                n
            );
        }
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn arc_immutable_snapshot_shared_across_verticles(vx: Vertx) {
        let snapshot = Arc::new(vec![1, 2, 3]);
        for _ in 0..2 {
            let snapshot = snapshot.clone();
            vx.deploy(move |_| {
                local(move |ctx| async move {
                    ctx.event_bus()
                        .consumer::<()>("snapshot")?
                        .handler(move |env| {
                            let values = (*snapshot).clone();
                            async move { env.reply(values) }
                        })
                })
            })
            .await
            .unwrap();
        }
        assert_eq!(
            vx.event_bus()
                .request::<_, Vec<i32>>("snapshot", ())
                .await
                .unwrap(),
            vec![1, 2, 3]
        );
        vx.close().await.unwrap();
    }
}

mod timers {
    //! Absolute deadlines, paused time, cancellation and periodic Skip.
    use super::*;

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn vertx_timer_fires_after_delay(vx: Vertx) {
        let timer = vx.timer(Duration::from_millis(100)).unwrap();
        tokio::time::advance(Duration::from_millis(200)).await;
        tokio::time::timeout(Duration::from_millis(1), timer)
            .await
            .unwrap()
            .unwrap();
        tokio::time::resume();
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn vertx_timer_explicit_cancel(vx: Vertx) {
        let timer = vx.timer(Duration::ZERO).unwrap();
        timer.cancel();
        assert!(timer.is_cancelled());
        assert!(matches!(timer.await, Err(VertxError::TimerCancelled)));
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn vertx_periodic_ticks_multiple_times(vx: Vertx) {
        let mut periodic = vx
            .periodic(Duration::from_millis(10), Duration::from_millis(10))
            .unwrap();
        for _ in 0..3 {
            periodic.tick().await.unwrap();
        }
        periodic.cancel();
        assert!(periodic.is_cancelled());
        assert!(matches!(
            periodic.tick().await,
            Err(VertxError::TimerCancelled)
        ));
        tokio::time::resume();
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn bound_timer_cancels_on_undeploy(vx: Vertx) {
        let (id, ctx) = capture(&vx).await;
        let timer = ctx.timer(Duration::from_secs(99)).unwrap();
        let mut periodic = ctx
            .periodic(Duration::ZERO, Duration::from_secs(1))
            .unwrap();
        vx.undeploy(id).await.unwrap();
        assert!(timer.is_cancelled());
        assert!(periodic.is_cancelled());
        assert!(matches!(timer.await, Err(VertxError::TimerCancelled)));
        assert!(matches!(
            periodic.tick().await,
            Err(VertxError::TimerCancelled)
        ));
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn zero_period_is_invalid_and_cancel_wins_over_due_tick(vx: Vertx) {
        assert!(matches!(
            vx.periodic(Duration::ZERO, Duration::ZERO),
            Err(VertxError::InvalidOptions(_))
        ));
        let mut p = vx.periodic(Duration::ZERO, Duration::from_secs(1)).unwrap();
        p.cancel();
        assert!(matches!(p.tick().await, Err(VertxError::TimerCancelled)));
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn periodic_skips_missed_ticks(vx: Vertx) {
        let mut timer = vx
            .periodic(Duration::ZERO, Duration::from_millis(10))
            .unwrap();
        timer.tick().await.unwrap();
        tokio::time::advance(Duration::from_millis(95)).await;
        timer.tick().await.unwrap();
        let before = tokio::time::Instant::now();
        timer.tick().await.unwrap();
        assert!(tokio::time::Instant::now() - before >= Duration::from_millis(5));
        tokio::time::resume();
        vx.close().await.unwrap();
    }

    #[test]
    fn timers_can_be_constructed_without_an_entered_tokio_runtime() {
        let vx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(1)
            .build();
        let timer = vx.timer(Duration::from_secs(1)).unwrap();
        let periodic = vx.periodic(Duration::ZERO, Duration::from_secs(1)).unwrap();
        timer.cancel();
        periodic.cancel();
        assert!(timer.is_cancelled());
        assert!(periodic.is_cancelled());
    }

    #[rstest]
    #[tokio::test]
    async fn completed_timer_does_not_become_cancelled_on_close(vx: Vertx) {
        let mut timer = vx.timer(Duration::ZERO).unwrap();
        (&mut timer).await.unwrap();
        vx.close().await.unwrap();
        timer.cancel();
        assert!(!timer.is_cancelled());
    }
}

mod errors {
    //! Business errors, panic recovery, hooks and retained cleanup failures.
    use super::*;

    #[rstest]
    #[tokio::test]
    async fn exception_handler_receives_handler_errors(vx: Vertx) {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        vx.set_exception_handler(move |e| {
            tx.send(e.to_string()).ok();
        });
        vx.deploy(|_| {
            local(|ctx| async move {
                ctx.event_bus()
                    .consumer::<()>("error")?
                    .handler(|_| async { Err(VertxError::Other("expected".into())) })
            })
        })
        .await
        .unwrap();
        vx.event_bus().send("error", ()).await.unwrap();
        assert!(rx.recv().await.unwrap().contains("expected"));
        vx.close().await.unwrap();
    }

    #[rstest]
    #[case::event_loop_error(false, false)]
    #[case::event_loop_panic(false, true)]
    #[case::worker_error(true, false)]
    #[case::worker_panic(true, true)]
    #[tokio::test]
    async fn task_error_and_handler_panic_preserve_context_and_subscription(
        vx: Vertx,
        #[case] worker_context: bool,
        #[case] panic: bool,
    ) {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        vx.set_exception_handler(move |error| {
            tx.send((
                Context::current().unwrap().deployment_id(),
                error.to_string(),
            ))
            .ok();
        });
        let handler = move |env: vertx_rs::Envelope<bool>| async move {
            if *env.body() {
                assert!(!panic, "test panic");
                return Err(VertxError::Other("expected handler failure".into()));
            }
            env.reply(42u32)
        };
        let id = if worker_context {
            vx.deploy_worker(move |_| {
                worker(move |ctx| async move {
                    ctx.event_bus()
                        .consumer::<bool>("panic")?
                        .handler_worker(handler)
                })
            })
            .await
            .unwrap()
        } else {
            vx.deploy(move |_| {
                local(move |ctx| async move {
                    ctx.event_bus().consumer::<bool>("panic")?.handler(handler)
                })
            })
            .await
            .unwrap()
        };
        assert!(matches!(
            vx.event_bus().request::<_, u32>("panic", true).await,
            Err(VertxError::ReplyClosed)
        ));
        let (owner, message) = rx.recv().await.unwrap();
        assert_eq!(owner, Some(id));
        assert!(message.contains(if panic {
            "test panic"
        } else {
            "expected handler failure"
        }));
        assert_eq!(
            vx.event_bus()
                .request::<_, u32>("panic", false)
                .await
                .unwrap(),
            42
        );
        // A successful subsequent response follows error reporting in this serial loop.
        assert!(
            matches!(
                rx.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "each failed message must report exactly once"
        );
        vx.undeploy(id).await.unwrap();
        let snapshot = vx.diagnostics();
        assert_eq!(snapshot.active_deployments, 0);
        assert!(
            snapshot
                .deployments
                .iter()
                .all(|deployment| deployment.deployment_id != id)
        );
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn context_exception_handler_can_clear_itself_without_deadlock(vx: Vertx) {
        let global = Arc::new(AtomicUsize::new(0));
        let g = global.clone();
        vx.set_exception_handler(move |_| {
            g.fetch_add(1, Ordering::SeqCst);
        });
        let (tx, rx) = oneshot::channel();
        vx.deploy(move |_| {
            local(move |ctx| async move {
                let tx = Mutex::new(Some(tx));
                let own = ctx.clone();
                ctx.set_exception_handler(move |_| {
                    own.clear_exception_handler();
                    if let Some(tx) = tx.lock().unwrap().take() {
                        tx.send(()).ok();
                    }
                });
                ctx.event_bus()
                    .consumer::<()>("fail")?
                    .handler(|_| async { Err(VertxError::Other("error".into())) })
            })
        })
        .await
        .unwrap();
        vx.event_bus().send("fail", ()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(global.load(Ordering::SeqCst), 0);
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn lifecycle_keeps_start_stop_and_drop_errors_and_source(vx: Vertx) {
        struct Broken;
        impl Drop for Broken {
            fn drop(&mut self) {
                panic!("drop failure");
            }
        }
        #[async_trait(?Send)]
        impl Verticle for Broken {
            async fn start(&mut self, _: Context) -> Result<()> {
                Err(VertxError::StartFailed("start failure".into()))
            }
            async fn stop(&mut self, _: Context) -> Result<()> {
                Err(VertxError::Other("stop failure".into()))
            }
        }
        let error = vx.deploy(|_| Broken).await.unwrap_err();
        assert!(
            std::error::Error::source(&error)
                .unwrap()
                .to_string()
                .contains("start failure")
        );
        let VertxError::LifecycleFailed { cause, cleanup } = error else {
            panic!("missing lifecycle errors")
        };
        assert!(matches!(*cause, VertxError::StartFailed(_)));
        assert!(
            matches!(&cleanup[..], [VertxError::Other(s), VertxError::Panic(p)] if s == "stop failure" && p == "drop failure")
        );
        vx.close().await.unwrap();
    }
}

mod diagnostics {
    //! Read-only snapshots, bounded details and lifecycle accounting.
    use super::*;

    #[tokio::test]
    async fn diagnostics_are_bounded_and_counts_follow_actual_task_drop() {
        use vertx_rs::DiagnosticsOptions;
        let vx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(1)
            .diagnostics(DiagnosticsOptions {
                task_details: true,
                max_task_details: 2,
                ..Default::default()
            })
            .build();
        let (_, ctx) = capture(&vx).await;
        let before = vx.diagnostics();
        let ok = ctx.spawn(async { Ok(()) }).unwrap();
        ok.await.unwrap();
        let err = ctx
            .spawn(async { Err(VertxError::Other("task failed".into())) })
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
            handles.push(ctx.spawn(futures::future::pending()).unwrap());
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
            .build();
        let (_, ctx) = capture(&vx).await;
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
        let correct = operation.context_id == ctx.id() && operation.operation == "future_poll";
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
    #[case::enabled(true)]
    #[case::disabled(false)]
    #[tokio::test]
    async fn diagnostics_can_be_disabled_and_consumers_do_not_double_decrement(
        #[case] enabled: bool,
    ) {
        let vx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(1)
            .diagnostics(vertx_rs::DiagnosticsOptions {
                enabled,
                ..Default::default()
            })
            .build();
        let consumer = vx.event_bus().consumer::<()>("diagnostic-count").unwrap();
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
                .build();
            let (_, ctx) = capture(&vx).await;
            let (entered, ready) = oneshot::channel();
            let (release, gate) = std::sync::mpsc::channel();
            let c = ctx.clone();
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
            let c = ctx.clone();
            let (submitted, observed) = oneshot::channel();
            let second = ctx
                .spawn(async move {
                    submitted.send(()).ok();
                    c.execute_blocking(|| ()).await
                })
                .unwrap();
            observed.await.unwrap();
            let c = ctx.clone();
            let (submitted, observed) = oneshot::channel();
            let third = ctx
                .spawn(async move {
                    submitted.send(()).ok();
                    c.execute_blocking_with(|| (), false).await
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
        #[async_trait(?Send)]
        impl Verticle for StopGate {
            async fn start(&mut self, _: Context) -> Result<()> {
                Ok(())
            }
            async fn stop(&mut self, _: Context) -> Result<()> {
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
            .deploy(|_| StopGate {
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
                .any(|d| d.deployment_id == id && d.stage == "stopping")
        );
        assert!(vx.diagnostics().deployments.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn diagnostics_keep_detached_blocking_work_until_its_actual_completion(vx: Vertx) {
        let (_, ctx) = capture(&vx).await;
        let (send, ready) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let c = ctx.clone();
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
        let (_, ctx) = capture(&vx).await;
        let before = vx.diagnostics().failed;
        let calls = Arc::new(AtomicUsize::new(0));
        let hook = calls.clone();
        ctx.set_exception_handler(move |_| {
            hook.fetch_add(1, Ordering::SeqCst);
        });
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
            .build();
        let a = vx.event_bus().consumer::<()>("a").unwrap();
        let b = vx.event_bus().consumer::<()>("b").unwrap();
        vx.event_bus().send("a", ()).await.unwrap();
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
}

mod tracing_propagation {
    //! Per-poll/drop Dispatch and Span propagation across Contexts and threads.
    use super::*;

    #[tokio::test]
    async fn scoped_tracing_follows_interleaved_contexts_without_span_leaks() {
        use tracing::Instrument;
        use tracing::instrument::WithSubscriber;
        let output = TraceOutput::default();
        let dispatch = output.dispatch();
        let vx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(1)
            .build();
        let (_, a) = capture(&vx).with_subscriber(dispatch.clone()).await;
        let (_, b) = capture(&vx).with_subscriber(dispatch.clone()).await;
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let alpha = tracing::dispatcher::with_default(&dispatch, || tracing::info_span!("alpha"));
        let beta = tracing::dispatcher::with_default(&dispatch, || tracing::info_span!("beta"));
        let gate = barrier.clone();
        let aid = a.id();
        let ah = async {
            a.spawn(async move {
                tracing::info!(probe = "alpha", "user probe before wait");
                gate.wait().await;
                assert_eq!(Context::current().unwrap().id(), aid);
                tracing::info!(probe = "alpha", "user probe after wait");
                Ok(())
            })
        }
        .instrument(alpha)
        .with_subscriber(dispatch.clone())
        .await
        .unwrap();
        let bid = b.id();
        let bh = async {
            b.spawn(async move {
                tracing::info!(probe = "beta", "user probe before wait");
                barrier.wait().await;
                assert_eq!(Context::current().unwrap().id(), bid);
                tracing::info!(probe = "beta", "user probe after wait");
                Ok(())
            })
        }
        .instrument(beta)
        .with_subscriber(dispatch.clone())
        .await
        .unwrap();
        ah.await.unwrap();
        bh.await.unwrap();
        vx.close().with_subscriber(dispatch).await.unwrap();
        let text = output.text();
        let lines: Vec<_> = text
            .lines()
            .filter(|line| line.contains("user probe"))
            .collect();
        assert_eq!(lines.len(), 4, "{text}");
        for line in lines {
            if line.contains("probe=\"alpha\"") {
                assert!(
                    line.contains(&format!("context_id={aid}")) && !line.contains("beta:"),
                    "{line}"
                );
            } else {
                assert!(
                    line.contains(&format!("context_id={bid}")) && !line.contains("alpha:"),
                    "{line}"
                );
            }
        }
        assert!(text.contains("task finished after user drop"));
        assert!(Context::current().is_none());
    }

    #[tokio::test]
    async fn diagnostic_logs_separate_admission_queue_timeout_and_user_hook() {
        use tracing::instrument::WithSubscriber;
        let output = TraceOutput::default();
        let dispatch = output.dispatch();
        async {
            let vx = vx();
            let mut consumer = vx.event_bus().consumer::<()>("stages").unwrap();
            for _ in 0..256 {
                vx.event_bus().try_send("stages", ()).unwrap();
            }
            vx.event_bus().publish("stages", ()).unwrap();
            let reply: Result<()> = vx
                .event_bus()
                .request_with(
                    "stages",
                    (),
                    DeliveryOptions::new().with_timeout(Duration::from_millis(5)),
                )
                .await;
            assert!(matches!(reply, Err(VertxError::Timeout(_))));
            assert_eq!(vx.diagnostics().request_timeouts, 1);
            assert_eq!(vx.diagnostics().broadcast_dropped, 1);
            let envelope = consumer.recv().await.unwrap().unwrap();
            drop(envelope);
            vx.event_bus().send("stages", ()).await.unwrap();
            let (_, ctx) = capture(&vx).await;
            let hook = Arc::new(AtomicUsize::new(0));
            let calls = hook.clone();
            ctx.set_exception_handler(move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
            });
            assert!(
                ctx.spawn(async { Err(VertxError::Other("diagnostic-hook-error".into())) })
                    .unwrap()
                    .await
                    .is_err()
            );
            assert_eq!(hook.load(Ordering::SeqCst), 1);
            vx.close().await.unwrap();
        }
        .with_subscriber(dispatch)
        .await;
        let text = output.text();
        assert!(
            text.contains("admission_wait=") && text.contains("queue_residence="),
            "{text}"
        );
        assert!(
            text.lines()
                .any(|line| line.contains("request timed out") && line.contains("request_id="))
        );
        assert_eq!(
            text.lines()
                .filter(|line| line.contains("framework operation failed")
                    && line.contains("diagnostic-hook-error"))
                .count(),
            1
        );
        assert!(text.contains("worker_join") && text.contains("event_loop_join"));
    }

    #[rstest]
    #[tokio::test]
    async fn scoped_tracing_follows_worker_migration_and_restores_context(vx: Vertx) {
        use tracing::Instrument;
        use tracing::instrument::WithSubscriber;
        let output = TraceOutput::default();
        let dispatch = output.dispatch();
        let (send, receive) = oneshot::channel();
        vx.deploy_worker(move |_| {
            worker(move |ctx| async move {
                send.send(ctx).ok();
                Ok(())
            })
        })
        .await
        .unwrap();
        let ctx = receive.await.unwrap();
        let (send, receive) = oneshot::channel();
        let mut send = Some(send);
        let mut first = None;
        let mut delivered = false;
        let context_id = ctx.id();
        let span =
            tracing::dispatcher::with_default(&dispatch, || tracing::info_span!("worker_probe"));
        let task = async {
            ctx.spawn(vertx_rs::pipe_to(
                futures::stream::poll_fn(move |cx| {
                    if delivered {
                        return std::task::Poll::Ready(None);
                    }
                    assert_eq!(Context::current().unwrap().id(), context_id);
                    let current = std::thread::current().id();
                    tracing::info!(thread_id = ?current, "migration probe");
                    if let Some(first) = first {
                        assert_ne!(first, current);
                        delivered = true;
                        std::task::Poll::Ready(Some(Ok(7u32)))
                    } else {
                        first = Some(current);
                        send.take()
                            .unwrap()
                            .send((current, cx.waker().clone()))
                            .ok();
                        std::task::Poll::Pending
                    }
                }),
                {
                    let probe = Arc::new(PipeProbe::default());
                    probe.ready.store(true, Ordering::SeqCst);
                    ProbeSink::new(probe)
                },
            ))
        }
        .instrument(span)
        .with_subscriber(dispatch)
        .await
        .unwrap();
        let (first, wake) = receive.await.unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let (release, wait) = std::sync::mpsc::channel();
        let wait = Arc::new(Mutex::new(wait));
        let (blocked, blocking) = oneshot::channel();
        let blocked = Arc::new(Mutex::new(Some(blocked)));
        let mut jobs = Vec::new();
        for _ in 0..2 {
            let (ctx, barrier, wait, blocked) =
                (ctx.clone(), barrier.clone(), wait.clone(), blocked.clone());
            jobs.push(tokio::spawn(async move {
                ctx.execute_blocking_with(
                    move || {
                        barrier.wait();
                        if std::thread::current().id() == first {
                            blocked.lock().unwrap().take().unwrap().send(()).ok();
                            wait.lock().unwrap().recv().unwrap();
                        }
                    },
                    false,
                )
                .await
            }));
        }
        blocking.await.unwrap();
        wake.wake();
        let result = tokio::time::timeout(Duration::from_secs(2), task).await;
        release.send(()).unwrap();
        result.unwrap().unwrap();
        for job in jobs {
            job.await.unwrap().unwrap();
        }
        vx.close().await.unwrap();
        let text = output.text();
        let lines: Vec<_> = text
            .lines()
            .filter(|line| line.contains("migration probe"))
            .collect();
        assert_eq!(lines.len(), 2, "{text}");
        for line in lines {
            assert!(
                line.contains("worker_probe:")
                    && line.contains(&format!("context_id={context_id}")),
                "{line}"
            );
        }
        assert!(Context::current().is_none());
    }

    #[rstest]
    #[tokio::test]
    async fn handler_destructor_uses_message_dispatch_even_when_consumer_has_another_subscriber(
        vx: Vertx,
    ) {
        use tracing::instrument::WithSubscriber;
        struct DropProbe(vertx_rs::ContextId);
        impl Drop for DropProbe {
            fn drop(&mut self) {
                assert_eq!(Context::current().unwrap().id(), self.0);
                tracing::info!("message destructor probe");
            }
        }
        let receiver = TraceOutput::default();
        let sender = TraceOutput::default();
        let (send, ready) = oneshot::channel();
        vx.deploy(move |_| {
            local(move |ctx| async move {
                let mut send = Some(send);
                let id = ctx.id();
                ctx.event_bus()
                    .consumer::<()>("drop-trace")?
                    .handler(move |_| {
                        let send = send.take();
                        async move {
                            let _probe = DropProbe(id);
                            send.unwrap().send(()).ok();
                            futures::future::pending::<()>().await;
                            Ok(())
                        }
                    })
            })
        })
        .with_subscriber(receiver.dispatch())
        .await
        .unwrap();
        vx.event_bus()
            .send("drop-trace", ())
            .with_subscriber(sender.dispatch())
            .await
            .unwrap();
        ready.await.unwrap();
        vx.close().await.unwrap();
        assert!(sender.text().contains("message destructor probe"));
        assert!(!receiver.text().contains("message destructor probe"));
    }

    #[rstest]
    #[tokio::test]
    async fn message_span_names_receiving_context_separately_from_sender_ancestry(vx: Vertx) {
        use tracing::instrument::WithSubscriber;
        let output = TraceOutput::default();
        let dispatch = output.dispatch();
        let (send, receive) = oneshot::channel();
        vx.deploy(move |_| {
            local(move |ctx| async move {
                let id = ctx.id();
                send.send(id).ok();
                ctx.event_bus()
                    .consumer::<()>("trace-hop")?
                    .handler(move |env| async move {
                        assert_eq!(Context::current().unwrap().id(), id);
                        tracing::info!("receiver execution probe");
                        env.reply(())
                    })
            })
        })
        .with_subscriber(dispatch.clone())
        .await
        .unwrap();
        let receiver_id = receive.await.unwrap();
        let (_, source) = capture(&vx).with_subscriber(dispatch.clone()).await;
        assert_ne!(source.id(), receiver_id);
        let bus = source.event_bus();
        let task =
            async { source.spawn(async move { bus.request::<_, ()>("trace-hop", ()).await }) }
                .with_subscriber(dispatch)
                .await
                .unwrap();
        task.await.unwrap();
        vx.close().await.unwrap();
        let text = output.text();
        let line = text
            .lines()
            .find(|line| line.contains("receiver execution probe"))
            .unwrap();
        let handler_fields = line
            .split("message_handler{")
            .nth(1)
            .unwrap()
            .split('}')
            .next()
            .unwrap();
        assert!(
            handler_fields.contains(&format!("context_id={receiver_id}")),
            "{line}"
        );
        assert!(handler_fields.contains("task_id=task#"), "{line}");
    }
}

mod streams {
    //! Pull consumption, readiness reservations, backpressure and pipe ending policies.
    use super::*;

    #[rstest]
    #[tokio::test]
    async fn message_sink_reserves_once_and_only_closes_its_adapter(vx: Vertx) {
        use futures::{Sink, SinkExt};
        let bus = vx.event_bus();
        let mut consumer = bus
            .consumer_with::<u32>("sink", vertx_rs::ConsumerOptions::new().with_capacity(1))
            .unwrap();
        let mut sink = bus
            .sink_with::<u32>("sink", DeliveryOptions::new().header("source", "sink"))
            .unwrap();
        for _ in 0..2 {
            futures::future::poll_fn(|cx| std::pin::Pin::new(&mut sink).poll_ready(cx))
                .await
                .unwrap();
        }
        assert_eq!(vx.diagnostics().reserved_slots, 1);
        assert!(matches!(
            bus.try_send("sink", 0u32),
            Err(VertxError::QueueFull(_))
        ));
        std::pin::Pin::new(&mut sink).start_send(7).unwrap();
        sink.flush().await.unwrap();
        let envelope = consumer.recv().await.unwrap().unwrap();
        assert_eq!(
            envelope.headers().get("source").map(String::as_str),
            Some("sink")
        );
        assert_eq!(envelope.into_body(), 7);
        futures::future::poll_fn(|cx| std::pin::Pin::new(&mut sink).poll_ready(cx))
            .await
            .unwrap();
        sink.close().await.unwrap();
        assert_eq!(vx.diagnostics().reserved_slots, 0);
        bus.send("sink", 8u32).await.unwrap();
        assert_eq!(consumer.recv().await.unwrap().unwrap().into_body(), 8);
        assert!(sink.send(9).await.is_err());
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn message_sink_waits_on_its_selected_consumer_and_drop_releases_wait(vx: Vertx) {
        use futures::{Sink, SinkExt};
        let bus = vx.event_bus();
        let options = vertx_rs::ConsumerOptions::new().with_capacity(1);
        let mut a = bus.consumer_with::<u32>("sticky", options.clone()).unwrap();
        let _b = bus.consumer_with::<u32>("sticky", options.clone()).unwrap();
        bus.try_send("sticky", 1u32).unwrap();
        bus.try_send("sticky", 2u32).unwrap();
        let mut sink = bus.sink::<u32>("sticky").unwrap();
        for _ in 0..2 {
            assert!(
                futures::poll!(futures::future::poll_fn(
                    |cx| std::pin::Pin::new(&mut sink).poll_ready(cx)
                ))
                .is_pending()
            );
        }
        let _available = bus.consumer_with::<u32>("sticky", options).unwrap();
        assert!(
            futures::poll!(futures::future::poll_fn(
                |cx| std::pin::Pin::new(&mut sink).poll_ready(cx)
            ))
            .is_pending()
        );
        assert_eq!(vx.diagnostics().waiting_senders, 1);
        a.recv().await.unwrap().unwrap();
        sink.send(3).await.unwrap();
        // Adding a consumer changes subsequent round-robin indices. A is still
        // full here, so dropping the next pending send must release its waiter.
        assert!(
            futures::poll!(futures::future::poll_fn(
                |cx| std::pin::Pin::new(&mut sink).poll_ready(cx)
            ))
            .is_pending()
        );
        drop(sink);
        assert_eq!(a.recv().await.unwrap().unwrap().into_body(), 3);
        assert_eq!(vx.diagnostics().waiting_senders, 0);
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn stream_and_sink_observe_scope_termination_with_retained_consumer(vx: Vertx) {
        use futures::{Sink, StreamExt};
        let (id, ctx) = capture(&vx).await;
        let mut consumer = ctx.event_bus().consumer::<u32>("retained").unwrap();
        let mut sink = vx.event_bus().sink::<u32>("retained").unwrap();
        futures::future::poll_fn(|cx| std::pin::Pin::new(&mut sink).poll_ready(cx))
            .await
            .unwrap();
        vx.undeploy(id).await.unwrap();
        assert!(matches!(
            std::pin::Pin::new(&mut sink).start_send(1),
            Err(VertxError::ChannelClosed(_))
        ));
        assert!(
            matches!(consumer.next().await, Some(Err(VertxError::DeploymentTerminated(i))) if i == id)
        );
        assert!(consumer.next().await.is_none());
        assert!(consumer.recv().await.unwrap().is_none());
        assert_eq!(vx.diagnostics().reserved_slots, 0);
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn consumer_stream_supports_non_unpin_messages_and_global_cancel_wins(vx: Vertx) {
        use futures::{SinkExt, StreamExt};
        struct PinnedMessage {
            value: u32,
            _pin: std::marker::PhantomPinned,
        }
        let bus = vx.event_bus();
        let mut consumer = bus.consumer::<PinnedMessage>("pinned").unwrap();
        let mut sink = bus.sink::<PinnedMessage>("pinned").unwrap();
        sink.send(PinnedMessage {
            value: 42,
            _pin: std::marker::PhantomPinned,
        })
        .await
        .unwrap();
        assert_eq!(consumer.next().await.unwrap().unwrap().body().value, 42);
        sink.send(PinnedMessage {
            value: 43,
            _pin: std::marker::PhantomPinned,
        })
        .await
        .unwrap();
        vx.close().await.unwrap();
        assert!(matches!(
            consumer.next().await,
            Some(Err(VertxError::VertxClosed))
        ));
        assert!(consumer.next().await.is_none());
    }

    #[tokio::test]
    async fn pipe_applies_backpressure_without_prefetch_and_flushes_before_close() {
        let probe = Arc::new(PipeProbe::default());
        let reads = Arc::new(AtomicUsize::new(0));
        let count = reads.clone();
        let source = futures::stream::poll_fn(move |_| {
            let n = count.fetch_add(1, Ordering::SeqCst);
            std::task::Poll::Ready((n < 3).then_some(Ok(n as u32)))
        });
        let mut pipe = Box::pin(vertx_rs::pipe_to(source, ProbeSink::new(probe.clone())));
        for _ in 0..3 {
            assert!(futures::poll!(pipe.as_mut()).is_pending());
        }
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        probe.ready.store(true, Ordering::SeqCst);
        probe.wake.lock().unwrap().take().unwrap().wake();
        pipe.await.unwrap();
        assert_eq!(*probe.values.lock().unwrap(), vec![0, 1, 2]);
        assert_eq!(*probe.calls.lock().unwrap(), vec!["flush", "close"]);
    }

    #[tokio::test]
    async fn pipe_waits_for_flush_and_close_completion() {
        let probe = Arc::new(PipeProbe::default());
        let mut sink = ProbeSink::new(probe.clone());
        let (flush, flushed) = oneshot::channel();
        let (close, closed) = oneshot::channel();
        sink.flush_gate = Some(flushed);
        sink.close_gate = Some(closed);
        let mut pipe = Box::pin(vertx_rs::pipe_to(futures::stream::empty(), sink));
        assert!(futures::poll!(pipe.as_mut()).is_pending());
        assert!(probe.calls.lock().unwrap().is_empty());
        flush.send(()).unwrap();
        assert!(futures::poll!(pipe.as_mut()).is_pending());
        assert_eq!(*probe.calls.lock().unwrap(), vec!["flush"]);
        close.send(()).unwrap();
        pipe.await.unwrap();
        assert_eq!(*probe.calls.lock().unwrap(), vec!["flush", "close"]);
    }

    #[rstest]
    #[case::success(None, false)]
    #[case::close_error(None, true)]
    #[case::read_error(Some("read"), false)]
    #[case::read_and_close_errors(Some("read"), true)]
    #[case::readiness_error(Some("ready"), false)]
    #[case::readiness_and_close_errors(Some("ready"), true)]
    #[case::write_error(Some("write"), false)]
    #[case::write_and_close_errors(Some("write"), true)]
    #[case::flush_error(Some("flush"), false)]
    #[case::flush_and_close_errors(Some("flush"), true)]
    #[tokio::test]
    async fn pipe_preserves_primary_and_cleanup_failures(
        #[case] failure: Option<&'static str>,
        #[case] close_failure: bool,
    ) {
        let probe = Arc::new(PipeProbe::default());
        probe.ready.store(true, Ordering::SeqCst);
        let mut sink = ProbeSink::new(probe.clone());
        sink.fail = failure.filter(|stage| *stage != "read");
        sink.fail_close = close_failure;
        let item = if failure == Some("read") {
            Err(VertxError::Other("read".into()))
        } else {
            Ok(1)
        };
        let result = vertx_rs::pipe_to(futures::stream::iter([item]), sink).await;
        match (failure, close_failure, result) {
            (None, false, Ok(())) => {}
            (None, true, Err(VertxError::Other(error))) => assert_eq!(error, "close"),
            (Some(stage), false, Err(VertxError::Other(error))) => assert_eq!(error, stage),
            (Some(stage), true, Err(VertxError::LifecycleFailed { cause, cleanup })) => {
                assert!(matches!(*cause, VertxError::Other(error) if error == stage));
                assert!(matches!(&cleanup[..], [VertxError::Other(error)] if error == "close"));
            }
            (_, _, unexpected) => panic!("unexpected pipe outcome: {unexpected:?}"),
        }
        assert_eq!(probe.calls.lock().unwrap().last(), Some(&"close"));
    }

    #[rstest]
    #[case::eof(false)]
    #[case::read_error(true)]
    #[tokio::test]
    async fn pipe_ending_policy(
        #[case] read_error: bool,
        #[values(false, true)] end_on_success: bool,
        #[values(false, true)] end_on_failure: bool,
    ) {
        let probe = Arc::new(PipeProbe::default());
        probe.ready.store(true, Ordering::SeqCst);
        let item = if read_error {
            Err(VertxError::Other("read".into()))
        } else {
            Ok(1)
        };
        let result = vertx_rs::pipe_to_with(
            futures::stream::iter([item]),
            ProbeSink::new(probe.clone()),
            vertx_rs::PipeOptions {
                end_on_success,
                end_on_failure,
            },
        )
        .await;
        assert_eq!(result.is_err(), read_error);
        let should_close = if read_error {
            end_on_failure
        } else {
            end_on_success
        };
        let calls = probe.calls.lock().unwrap();
        assert_eq!(calls.contains(&"flush"), !read_error);
        assert_eq!(calls.contains(&"close"), should_close);
    }

    #[tokio::test]
    async fn pipe_can_reuse_sink_when_automatic_ending_is_disabled() {
        use futures::SinkExt;
        let probe = Arc::new(PipeProbe::default());
        probe.ready.store(true, Ordering::SeqCst);
        let mut sink = ProbeSink::new(probe.clone());
        let options = vertx_rs::PipeOptions {
            end_on_success: false,
            end_on_failure: false,
        };
        vertx_rs::pipe_to_with(futures::stream::empty(), &mut sink, options)
            .await
            .unwrap();
        assert_eq!(*probe.calls.lock().unwrap(), vec!["flush"]);
        sink.send(2).await.unwrap();
        let result = vertx_rs::pipe_to_with(
            futures::stream::iter([Err(VertxError::Other("read".into()))]),
            &mut sink,
            options,
        )
        .await;
        assert!(result.is_err());
        assert_eq!(*probe.calls.lock().unwrap(), vec!["flush", "flush"]);
    }

    #[tokio::test]
    async fn pipe_yields_for_always_ready_streams_and_sinks() {
        let probe = Arc::new(PipeProbe::default());
        probe.ready.store(true, Ordering::SeqCst);
        let source = futures::stream::iter((0..130u32).map(Ok));
        let mut pipe = Box::pin(vertx_rs::pipe_to(source, ProbeSink::new(probe.clone())));
        assert!(futures::poll!(pipe.as_mut()).is_pending());
        assert_eq!(probe.values.lock().unwrap().len(), 64);
        pipe.await.unwrap();
        assert_eq!(probe.values.lock().unwrap().len(), 130);
    }

    #[tokio::test]
    async fn pipe_local_futures_keep_context_and_spans_across_interleaved_awaits() {
        use futures::{SinkExt, StreamExt};
        use tracing::Instrument;
        use tracing::instrument::WithSubscriber;
        let output = TraceOutput::default();
        let dispatch = output.dispatch();
        let vx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(1)
            .build();
        let (_, a) = capture(&vx).with_subscriber(dispatch.clone()).await;
        let (_, b) = capture(&vx).with_subscriber(dispatch.clone()).await;
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let mut tasks = Vec::new();
        for ctx in [a, b] {
            let span = tracing::dispatcher::with_default(
                &dispatch,
                || tracing::info_span!("local_pipe", owner = %ctx.id()),
            );
            let gate = barrier.clone();
            let target = ctx.clone();
            tasks.push(
                async {
                    ctx.spawn(async move {
                        let child = {
                            let id = target.id();
                            let thread = std::thread::current().id();
                            let local = Rc::new(Cell::new(0));
                            let source_state = local.clone();
                            let source = futures::stream::iter([1u32, 2]).then(move |value| {
                                let (gate, state) = (gate.clone(), source_state.clone());
                                async move {
                                    gate.wait().await;
                                    assert_eq!(Context::current().unwrap().id(), id);
                                    assert_eq!(std::thread::current().id(), thread);
                                    state.set(state.get() + 1);
                                    tracing::info!(probe_context = %id, "local pipe probe");
                                    Ok(value)
                                }
                            });
                            let probe = Arc::new(PipeProbe::default());
                            probe.ready.store(true, Ordering::SeqCst);
                            let sink_state = local.clone();
                            let sink = ProbeSink::new(probe.clone()).with(move |value| {
                                let state = sink_state.clone();
                                async move {
                                    assert_eq!(Context::current().unwrap().id(), id);
                                    state.set(state.get() + 1);
                                    Ok(value)
                                }
                            });
                            target.spawn_local(async move {
                                vertx_rs::pipe_to(source, sink).await?;
                                assert_eq!(local.get(), 4);
                                assert_eq!(*probe.values.lock().unwrap(), vec![1, 2]);
                                Ok(())
                            })?
                        };
                        child.await
                    })
                }
                .instrument(span)
                .with_subscriber(dispatch.clone())
                .await
                .unwrap(),
            );
        }
        for task in tasks {
            task.await.unwrap();
        }
        vx.close().await.unwrap();
        let text = output.text();
        let lines: Vec<_> = text
            .lines()
            .filter(|line| line.contains("local pipe probe"))
            .collect();
        assert_eq!(lines.len(), 4, "{text}");
        for line in lines {
            let id = line.split("probe_context=").nth(1).unwrap();
            assert!(line.contains(&format!("context_id={id}")), "{line}");
        }
    }

    #[rstest]
    #[tokio::test]
    async fn pipe_abort_and_timeout_release_queue_waiters_and_drop_on_context(vx: Vertx) {
        use futures::StreamExt;
        struct DropContext {
            expected: vertx_rs::ContextId,
            dropped: Arc<AtomicUsize>,
        }
        impl Drop for DropContext {
            fn drop(&mut self) {
                assert_eq!(Context::current().unwrap().id(), self.expected);
                self.dropped.fetch_add(1, Ordering::SeqCst);
            }
        }
        let (_, ctx) = capture(&vx).await;
        let bus = vx.event_bus();
        let mut consumer = bus
            .consumer_with::<u32>(
                "blocked-pipe",
                vertx_rs::ConsumerOptions::new().with_capacity(1),
            )
            .unwrap();
        bus.try_send("blocked-pipe", 0u32).unwrap();
        let (send, receive) = oneshot::channel();
        let mut send = Some(send);
        let dropped = Arc::new(AtomicUsize::new(0));
        let probe = DropContext {
            expected: ctx.id(),
            dropped: dropped.clone(),
        };
        let source = futures::stream::iter([Ok(1u32)]).inspect(move |_| {
            let _ = &probe;
            send.take().unwrap().send(()).ok();
        });
        let sink = bus.sink("blocked-pipe").unwrap();
        let task = ctx.spawn(vertx_rs::pipe_to(source, sink)).unwrap();
        receive.await.unwrap();
        // This round trip runs after the sender's first synchronous poll.
        ctx.spawn(async { Ok(()) }).unwrap().await.unwrap();
        assert_eq!(vx.diagnostics().waiting_senders, 1);
        task.abort();
        assert!(matches!(task.await, Err(VertxError::TaskCancelled)));
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(vx.diagnostics().waiting_senders, 0);
        let result = tokio::time::timeout(
            Duration::from_millis(5),
            vertx_rs::pipe_to(
                futures::stream::iter([Ok(2u32)]),
                bus.sink("blocked-pipe").unwrap(),
            ),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(vx.diagnostics().waiting_senders, 0);
        assert_eq!(consumer.recv().await.unwrap().unwrap().into_body(), 0);
        vx.close().await.unwrap();
    }

    #[rstest]
    #[case::own_scope_cancelled(true)]
    #[case::foreign_source_failed(false)]
    #[tokio::test]
    async fn pipe_distinguishes_own_shutdown_from_foreign_source_failure(
        #[case] same_scope: bool,
        vx: Vertx,
    ) {
        use futures::TryStreamExt;
        let (owner_id, owner) = capture(&vx).await;
        let (source_id, source_ctx) = if same_scope {
            (owner_id, owner.clone())
        } else {
            capture(&vx).await
        };
        let consumer = source_ctx.event_bus().consumer::<u32>("pipe-stop").unwrap();
        let probe = Arc::new(PipeProbe::default());
        probe.ready.store(true, Ordering::SeqCst);
        let calls = Arc::new(AtomicUsize::new(0));
        let hook = calls.clone();
        owner.set_exception_handler(move |_| {
            hook.fetch_add(1, Ordering::SeqCst);
        });
        let before = vx.diagnostics();
        let task = owner
            .spawn(vertx_rs::pipe_to(
                consumer.map_ok(|env| env.into_body()),
                ProbeSink::new(probe.clone()),
            ))
            .unwrap();
        owner.spawn(async { Ok(()) }).unwrap().await.unwrap();
        vx.undeploy(source_id).await.unwrap();
        assert!(matches!(task.await, Err(VertxError::DeploymentTerminated(id)) if id == source_id));
        assert_eq!(calls.load(Ordering::SeqCst), usize::from(!same_scope));
        assert_eq!(*probe.calls.lock().unwrap(), vec!["close"]);
        let after = vx.diagnostics();
        assert_eq!(after.failed - before.failed, u64::from(!same_scope));
        assert_eq!(after.cancelled - before.cancelled, u64::from(same_scope));
        vx.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn message_sink_global_close_releases_cached_and_pending_reservations(vx: Vertx) {
        use futures::{Sink, SinkExt};
        let bus = vx.event_bus();
        let _consumer = bus
            .consumer_with::<()>(
                "closing-sink",
                vertx_rs::ConsumerOptions::new().with_capacity(1),
            )
            .unwrap();
        let mut reserved = bus.sink::<()>("closing-sink").unwrap();
        futures::future::poll_fn(|cx| std::pin::Pin::new(&mut reserved).poll_ready(cx))
            .await
            .unwrap();
        let mut waiting = bus.sink::<()>("closing-sink").unwrap();
        assert!(
            futures::poll!(futures::future::poll_fn(|cx| std::pin::Pin::new(
                &mut waiting
            )
            .poll_ready(cx)))
            .is_pending()
        );
        vx.close().await.unwrap();
        assert!(matches!(
            std::pin::Pin::new(&mut reserved).start_send(()),
            Err(VertxError::VertxClosed)
        ));
        assert!(matches!(
            waiting.send(()).await,
            Err(VertxError::VertxClosed)
        ));
        assert_eq!(
            (
                vx.diagnostics().waiting_senders,
                vx.diagnostics().reserved_slots
            ),
            (0, 0)
        );
    }
}
