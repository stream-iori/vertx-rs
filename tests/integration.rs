//! Contract tests for the Vert.x 5-style execution core.
use async_trait::async_trait;
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

fn vx() -> Vertx {
    Vertx::builder()
        .event_loop_threads(2)
        .worker_threads(2)
        .shutdown_grace(Duration::from_millis(500))
        .build()
}
struct Local<F>(Option<F>);
fn local<F, Fut>(f: F) -> Local<F>
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
struct Worker<F>(Option<F>);
fn worker<F, Fut>(f: F) -> Worker<F>
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
async fn capture(vx: &Vertx) -> (vertx_rs::DeploymentId, Context) {
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
async fn echo(vx: &Vertx, address: &'static str) -> vertx_rs::DeploymentId {
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
#[tokio::test]
async fn deploy_runs_factory_then_start_then_undeploy_stops() {
    let vx = vx();
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
#[tokio::test]
async fn undeploy_unknown_id_errors() {
    let vx = vx();
    let (id, _) = capture(&vx).await;
    vx.undeploy(id).await.unwrap();
    assert!(matches!(
        vx.undeploy(id).await,
        Err(VertxError::DeploymentNotFound(_))
    ));
    vx.close().await.unwrap();
}
#[tokio::test]
async fn undeploy_aborts_misbehaving_child() {
    let vx = vx();
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
#[tokio::test]
async fn verticle_pinned_to_single_thread() {
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
    let vx = vx();
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
#[tokio::test]
async fn verticle_can_hold_non_send_state_directly() {
    let vx = vx();
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
async fn undeploy_unregisters_bound_consumers() {
    let vx = vx();
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
#[tokio::test]
async fn verticles_communicate_through_their_bound_buses() {
    let vx = vx();
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
#[tokio::test]
async fn worker_verticle_processes_messages() {
    let vx = vx();
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
async fn execute_blocking_from_event_loop_verticle() {
    let vx = vx();
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
#[tokio::test]
async fn context_current_is_none_outside_vertx() {
    assert!(Context::current().is_none());
    assert!(!Context::is_on_vertx_thread());
}
#[tokio::test]
async fn context_introspection_event_loop_full() {
    let vx = vx();
    let (id, ctx) = capture(&vx).await;
    assert_eq!(ctx.threading_model(), ThreadingModel::EventLoop);
    assert_eq!(ctx.deployment_id(), Some(id));
    assert!(ctx.owner().is_some());
    assert!(ctx.event_loop_id().is_some());
    vx.close().await.unwrap();
}
#[tokio::test]
async fn context_introspection_worker_full() {
    let vx = vx();
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
#[tokio::test]
async fn external_thread_can_hop_into_event_loop_via_run_on_context() {
    let vx = vx();
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
#[tokio::test]
async fn run_on_context_after_undeploy_returns_terminated() {
    let vx = vx();
    let (id, ctx) = capture(&vx).await;
    vx.undeploy(id).await.unwrap();
    assert!(matches!(
        ctx.run_on_context(async { Ok(()) }),
        Err(VertxError::DeploymentTerminated(_))
    ));
    vx.close().await.unwrap();
}
#[tokio::test]
async fn execute_blocking_after_undeploy_returns_terminated() {
    let vx = vx();
    let (id, ctx) = capture(&vx).await;
    vx.undeploy(id).await.unwrap();
    assert!(matches!(
        ctx.execute_blocking(|| 1).await,
        Err(VertxError::DeploymentTerminated(_))
    ));
    vx.close().await.unwrap();
}
#[tokio::test]
async fn bound_consumer_after_undeploy_returns_terminated() {
    let vx = vx();
    let (id, ctx) = capture(&vx).await;
    vx.undeploy(id).await.unwrap();
    assert!(matches!(
        ctx.event_bus().consumer::<()>("late"),
        Err(VertxError::DeploymentTerminated(_))
    ));
    vx.close().await.unwrap();
}
#[tokio::test(start_paused = true)]
async fn vertx_timer_fires_after_delay() {
    let vx = vx();
    let timer = vx.timer(Duration::from_millis(100)).unwrap();
    tokio::time::advance(Duration::from_millis(200)).await;
    tokio::time::timeout(Duration::from_millis(1), timer)
        .await
        .unwrap()
        .unwrap();
    tokio::time::resume();
    vx.close().await.unwrap();
}
#[tokio::test]
async fn vertx_timer_explicit_cancel() {
    let vx = vx();
    let timer = vx.timer(Duration::ZERO).unwrap();
    timer.cancel();
    assert!(timer.is_cancelled());
    assert!(matches!(timer.await, Err(VertxError::TimerCancelled)));
    vx.close().await.unwrap();
}
#[tokio::test(start_paused = true)]
async fn vertx_periodic_ticks_multiple_times() {
    let vx = vx();
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
#[tokio::test]
async fn bound_timer_cancels_on_undeploy() {
    let vx = vx();
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
#[tokio::test]
async fn publish_delivers_to_all_subscribers() {
    let vx = vx();
    let bus = vx.event_bus();
    let mut a = bus.consumer::<u32>("pub").unwrap();
    let mut b = bus.consumer::<u32>("pub").unwrap();
    bus.publish("pub", 7u32).unwrap();
    assert_eq!(a.recv().await.unwrap().into_body(), 7);
    assert_eq!(b.recv().await.unwrap().into_body(), 7);
    vx.close().await.unwrap();
}
#[tokio::test]
async fn send_with_no_consumer_errors() {
    let vx = vx();
    assert!(matches!(
        vx.event_bus().send("missing", ()).await,
        Err(VertxError::NoConsumer(_))
    ));
    vx.close().await.unwrap();
}
#[tokio::test]
async fn request_timeout_fires() {
    let vx = vx();
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
#[tokio::test]
async fn dropping_consumer_unregisters() {
    let vx = vx();
    let consumer = vx.event_bus().consumer::<()>("drop").unwrap();
    drop(consumer);
    assert!(matches!(
        vx.event_bus().try_send("drop", ()),
        Err(VertxError::NoConsumer(_))
    ));
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
#[tokio::test]
async fn consumer_handler_dispatches_envelopes() {
    let vx = vx();
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
#[tokio::test]
async fn consumer_handler_unregisters_on_undeploy() {
    let vx = vx();
    let id = echo(&vx, "handler").await;
    vx.undeploy(id).await.unwrap();
    assert!(matches!(
        vx.event_bus().try_send("handler", "x".to_owned()),
        Err(VertxError::NoConsumer(_))
    ));
    vx.close().await.unwrap();
}
#[tokio::test]
async fn rc_refcell_shared_state_across_handlers() {
    let vx = vx();
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
#[tokio::test]
async fn rc_cell_shared_scalar_across_handlers() {
    let vx = vx();
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
#[tokio::test]
async fn message_passing_between_handlers() {
    let vx = vx();
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
#[tokio::test]
async fn arc_atomic_counter_shared_across_verticles() {
    let vx = vx();
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
#[tokio::test]
async fn arc_immutable_snapshot_shared_across_verticles() {
    let vx = vx();
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
#[tokio::test]
async fn delivery_options_headers_round_trip() {
    let vx = vx();
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
#[tokio::test]
async fn exception_handler_receives_handler_errors() {
    let vx = vx();
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
#[tokio::test]
async fn deploy_with_instances_and_typed_config() {
    let vx = vx();
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

// Regression coverage for Context isolation, lifecycle races and cancellation.
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
#[tokio::test]
async fn raw_tokio_spawn_has_no_logical_context() {
    let vx = vx();
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
async fn execute_blocking_is_fifo_by_default() {
    let vx = vx();
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
#[tokio::test]
async fn unordered_blocking_obeys_configured_limit_and_can_overlap() {
    let vx = vx();
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let m = maximum.clone();
    vx.deploy(move |_| {
        local(move |ctx| async move {
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let jobs = (0..8).map(|_| {
                let (a, m, b) = (active.clone(), m.clone(), barrier.clone());
                ctx.execute_blocking_with(
                    move || {
                        let n = a.fetch_add(1, Ordering::SeqCst) + 1;
                        m.fetch_max(n, Ordering::SeqCst);
                        b.wait();
                        a.fetch_sub(1, Ordering::SeqCst);
                    },
                    false,
                )
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
async fn task_error_and_handler_panic_preserve_context_and_subscription() {
    let vx = vx();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    vx.set_exception_handler(move |e| {
        tx.send((Context::current().unwrap().deployment_id(), e.to_string()))
            .ok();
    });
    let id = vx
        .deploy(|_| {
            local(|ctx| async move {
                ctx.event_bus()
                    .consumer::<bool>("panic")?
                    .handler(|env| async move {
                        if *env.body() {
                            panic!("test panic");
                        }
                        env.reply(42u32)
                    })
            })
        })
        .await
        .unwrap();
    assert!(matches!(
        vx.event_bus().request::<_, u32>("panic", true).await,
        Err(VertxError::ReplyClosed)
    ));
    let (owner, message) = rx.recv().await.unwrap();
    assert_eq!(owner, Some(id));
    assert!(message.contains("test panic"));
    assert_eq!(
        vx.event_bus()
            .request::<_, u32>("panic", false)
            .await
            .unwrap(),
        42
    );
    vx.close().await.unwrap();
}
#[tokio::test]
async fn context_exception_handler_can_clear_itself_without_deadlock() {
    let vx = vx();
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
#[tokio::test]
async fn request_timeout_includes_waiting_for_full_queue() {
    let vx = vx();
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
#[tokio::test]
async fn recipient_failure_and_late_reply_are_explicit() {
    let vx = vx();
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
    let env = consumer.recv().await.unwrap();
    assert!(matches!(job.await.unwrap(), Err(VertxError::Timeout(_))));
    assert!(matches!(env.reply(()), Err(VertxError::ReplyClosed)));
    vx.close().await.unwrap();
}
#[tokio::test]
async fn type_conflicts_are_rejected_at_registration() {
    let vx = vx();
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
#[tokio::test]
async fn broadcast_drops_only_full_subscriber_and_send_round_robins() {
    let vx = vx();
    let bus = vx.event_bus();
    let mut a = bus.consumer::<u32>("pub").unwrap();
    let mut b = bus.consumer::<u32>("pub").unwrap();
    for n in 0..256u32 {
        bus.publish("pub", n).unwrap();
        assert_eq!(b.recv().await.unwrap().into_body(), n);
    }
    bus.publish("pub", 999u32).unwrap();
    assert_eq!(b.recv().await.unwrap().into_body(), 999);
    for n in 0..256u32 {
        assert_eq!(a.recv().await.unwrap().into_body(), n);
    }
    bus.send("pub", 1u32).await.unwrap();
    bus.send("pub", 2u32).await.unwrap();
    assert_eq!(a.recv().await.unwrap().into_body(), 1);
    assert_eq!(b.recv().await.unwrap().into_body(), 2);
    vx.close().await.unwrap();
}
#[tokio::test]
async fn start_failure_and_factory_panic_clean_resources() {
    let vx = vx();
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
#[tokio::test]
async fn cancellation_of_pending_deploy_cleans_startup() {
    let vx = vx();
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
#[tokio::test]
async fn multi_instance_failure_rolls_back_successful_instances() {
    let vx = vx();
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
#[tokio::test]
async fn undeploy_waits_for_inflight_handler_to_be_dropped() {
    let vx = vx();
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
async fn stop_can_execute_blocking_cleanup_during_global_close() {
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
    let vx = vx();
    vx.deploy(|_| Cleanup).await.unwrap();
    vx.close().await.unwrap();
}
#[tokio::test]
async fn close_is_idempotent_cancels_global_resources_and_rejects_work() {
    let vx = vx();
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
    assert!(external.recv().await.is_none());
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
#[tokio::test]
async fn own_context_waiting_shutdown_is_rejected_and_request_stop_works() {
    let vx = vx();
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
#[tokio::test]
async fn zero_period_is_invalid_and_cancel_wins_over_due_tick() {
    let vx = vx();
    assert!(matches!(
        vx.periodic(Duration::ZERO, Duration::ZERO),
        Err(VertxError::InvalidOptions(_))
    ));
    let mut p = vx.periodic(Duration::ZERO, Duration::from_secs(1)).unwrap();
    p.cancel();
    assert!(matches!(p.tick().await, Err(VertxError::TimerCancelled)));
    vx.close().await.unwrap();
}
#[tokio::test]
async fn closing_unblocks_senders_waiting_for_capacity() {
    let vx = vx();
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
async fn close_during_start_cancels_deploy_without_cleanup_error() {
    let vx = vx();
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
#[tokio::test]
async fn stop_error_is_returned_and_resources_still_close() {
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
    let vx = vx();
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
#[tokio::test]
async fn concurrent_undeploy_runs_stop_exactly_once() {
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
    let vx = vx();
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
#[tokio::test]
async fn cancelled_close_waiter_does_not_cancel_cleanup() {
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
    let vx = vx();
    let (tx, rx) = oneshot::channel();
    vx.deploy(|_| Gate(Some(rx))).await.unwrap();
    let mut close = Box::pin(vx.close());
    assert!(futures::poll!(close.as_mut()).is_pending());
    drop(close);
    tx.send(()).unwrap();
    vx.close().await.unwrap();
    assert!(vx.is_closed());
}

#[tokio::test]
async fn worker_failed_start_waits_for_its_resource_cleanup() {
    let vx = vx();
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
#[tokio::test(start_paused = true)]
async fn periodic_skips_missed_ticks() {
    let vx = vx();
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
#[tokio::test]
async fn completed_timer_does_not_become_cancelled_on_close() {
    let vx = vx();
    let mut timer = vx.timer(Duration::ZERO).unwrap();
    (&mut timer).await.unwrap();
    vx.close().await.unwrap();
    timer.cancel();
    assert!(!timer.is_cancelled());
}
#[tokio::test]
async fn retained_stop_context_cannot_admit_cleanup_from_external_tasks() {
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
    let vx = vx();
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
async fn cancelling_multi_instance_deploy_rolls_back_prior_instances() {
    let vx = vx();
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

async fn capture_worker(vx: &Vertx) -> (vertx_rs::DeploymentId, Context) {
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

struct WorkerDropProbe {
    id: Option<vertx_rs::DeploymentId>,
    dropped: Arc<AtomicUsize>,
    panic: bool,
}
impl Drop for WorkerDropProbe {
    fn drop(&mut self) {
        assert!(Context::is_on_worker_thread());
        assert_eq!(Context::current().unwrap().deployment_id(), self.id);
        self.dropped.fetch_add(1, Ordering::SeqCst);
        assert!(!self.panic, "worker destructor panic");
    }
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
    let first = tokio::spawn(async move { first_bus.request::<_, u8>("serial-future", 1u8).await });
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

#[tokio::test]
async fn future_combinations_preserve_worker_context_and_keep_spawned_tasks_independent() {
    use futures::{future, FutureExt};
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
