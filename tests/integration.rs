//! Integration tests for vertx-rs (owned-self / run model).

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use vertx_rs::{Consumer, Context, DeliveryOptions, EventBus, Result, Verticle, Vertx, VertxError, WorkerVerticle};

// ---------------------------------------------------------------------------
// Lifecycle: deploy spawns run() after factory; undeploy cancels it
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct Counters {
    started: Arc<AtomicUsize>,
    stopped: Arc<AtomicUsize>,
}

struct LifecycleV {
    counters: Counters,
}

#[async_trait(?Send)]
impl Verticle for LifecycleV {
    async fn run(self, ctx: Context) -> Result<()> {
        self.counters.started.fetch_add(1, Ordering::SeqCst);
        ctx.cancellation_token().cancelled().await;
        self.counters.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn deploy_runs_factory_then_run_then_undeploy_cancels() {
    let vx = Vertx::new();
    let c = Counters::default();
    let c2 = c.clone();
    let id = vx
        .deploy(move |_ctx| LifecycleV { counters: c2 })
        .await
        .unwrap();
    // Give the spawned task a moment to enter run().
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(c.started.load(Ordering::SeqCst), 1);
    assert_eq!(c.stopped.load(Ordering::SeqCst), 0);

    vx.undeploy(id).await.unwrap();
    assert_eq!(c.stopped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn undeploy_unknown_id_errors() {
    let vx = Vertx::new();
    let id = vx
        .deploy(|_| LifecycleV { counters: Counters::default() })
        .await
        .unwrap();
    vx.undeploy(id).await.unwrap();
    let err = vx.undeploy(id).await.unwrap_err();
    assert!(matches!(err, VertxError::DeploymentNotFound(_)));
}

#[tokio::test]
async fn undeploy_aborts_misbehaving_verticle() {
    // Verticle that never listens to cancellation — must be aborted.
    struct Stubborn;
    #[async_trait(?Send)]
    impl Verticle for Stubborn {
        async fn run(self, _ctx: Context) -> Result<()> {
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    let vx = Vertx::new();
    let id = vx.deploy(|_| Stubborn).await.unwrap();
    // undeploy with internal grace period; should eventually return.
    let res = tokio::time::timeout(Duration::from_secs(3), vx.undeploy(id)).await;
    assert!(res.is_ok(), "undeploy must not hang");
}

// ---------------------------------------------------------------------------
// EventLoop affinity & !Send state held by owned value (no Rc/RefCell)
// ---------------------------------------------------------------------------

struct AffinityV {
    consumer: Consumer<u32>,
    samples: Arc<std::sync::Mutex<Vec<std::thread::ThreadId>>>,
}

#[async_trait(?Send)]
impl Verticle for AffinityV {
    async fn run(mut self, ctx: Context) -> Result<()> {
        self.samples
            .lock()
            .unwrap()
            .push(std::thread::current().id());
        let cancel = ctx.cancellation_token();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                msg = self.consumer.recv() => {
                    let Some(env) = msg else { return Ok(()) };
                    self.samples.lock().unwrap().push(std::thread::current().id());
                    let body = env.body().clone();
                    let _ = env.reply(body);
                }
            }
        }
    }
}

#[tokio::test]
async fn verticle_pinned_to_single_thread() {
    let vx = Vertx::new();
    let samples: Arc<std::sync::Mutex<Vec<_>>> = Arc::new(Default::default());
    let s2 = Arc::clone(&samples);
    vx.deploy(move |ctx| AffinityV {
        consumer: ctx.event_bus().consumer::<u32>("affinity").unwrap(),
        samples: s2,
    })
    .await
    .unwrap();

    for i in 0..20u32 {
        let _: u32 = vx
            .event_bus()
            .request("affinity", i)
            .await
            .unwrap();
    }

    let s = samples.lock().unwrap();
    let first = s[0];
    assert!(s.iter().all(|t| *t == first), "moved across threads: {s:?}");
    assert!(s.len() >= 21);
}

/// Counter verticle holding owned `Cell<u64>` (no Rc/RefCell needed).
struct CounterV {
    consumer: Consumer<()>,
    count: Cell<u64>,
}

#[async_trait(?Send)]
impl Verticle for CounterV {
    async fn run(mut self, ctx: Context) -> Result<()> {
        let cancel = ctx.cancellation_token();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                msg = self.consumer.recv() => {
                    let Some(env) = msg else { return Ok(()) };
                    self.count.set(self.count.get() + 1);
                    let _ = env.reply(self.count.get());
                }
            }
        }
    }
}

#[tokio::test]
async fn verticle_can_hold_non_send_state_directly() {
    let vx = Vertx::new();
    vx.deploy(|ctx| CounterV {
        consumer: ctx.event_bus().consumer::<()>("inc").unwrap(),
        count: Cell::new(0),
    })
    .await
    .unwrap();
    let n1: u64 = vx.event_bus().request("inc", ()).await.unwrap();
    let n2: u64 = vx.event_bus().request("inc", ()).await.unwrap();
    let n3: u64 = vx.event_bus().request("inc", ()).await.unwrap();
    assert_eq!((n1, n2, n3), (1, 2, 3));
}

// ---------------------------------------------------------------------------
// EventBus: bound consumer auto-cleanup, inter-verticle communication
// ---------------------------------------------------------------------------

struct EchoV {
    consumer: Consumer<String>,
}

#[async_trait(?Send)]
impl Verticle for EchoV {
    async fn run(mut self, ctx: Context) -> Result<()> {
        let cancel = ctx.cancellation_token();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                msg = self.consumer.recv() => {
                    let Some(env) = msg else { return Ok(()) };
                    let body = env.body().clone();
                    let _ = env.reply(format!("echo:{body}"));
                }
            }
        }
    }
}

#[tokio::test]
async fn undeploy_unregisters_bound_consumers() {
    let vx = Vertx::new();
    let id = vx
        .deploy(|ctx| EchoV {
            consumer: ctx.event_bus().consumer::<String>("echo").unwrap(),
        })
        .await
        .unwrap();
    let r: String = vx
        .event_bus()
        .request("echo", "hi".to_string())
        .await
        .unwrap();
    assert_eq!(r, "echo:hi");

    vx.undeploy(id).await.unwrap();
    let err = vx.event_bus().send("echo", "x".to_string()).await.unwrap_err();
    assert!(matches!(err, VertxError::NoConsumer(_)));
}

// Inter-verticle calls via bound buses.
struct UpperV {
    consumer: Consumer<String>,
}
#[async_trait(?Send)]
impl Verticle for UpperV {
    async fn run(mut self, ctx: Context) -> Result<()> {
        let cancel = ctx.cancellation_token();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                msg = self.consumer.recv() => {
                    let Some(env) = msg else { return Ok(()) };
                    let body = env.body().clone();
                    let _ = env.reply(body.to_uppercase());
                }
            }
        }
    }
}
struct CallerV {
    consumer: Consumer<String>,
    bus: EventBus,
}
#[async_trait(?Send)]
impl Verticle for CallerV {
    async fn run(mut self, ctx: Context) -> Result<()> {
        let cancel = ctx.cancellation_token();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                msg = self.consumer.recv() => {
                    let Some(env) = msg else { return Ok(()) };
                    let body = env.body().clone();
                    let upper: String = self.bus
                        .request("upper", body)
                        .await
                        .unwrap();
                    let _ = env.reply(upper);
                }
            }
        }
    }
}

#[tokio::test]
async fn verticles_communicate_through_their_bound_buses() {
    let vx = Vertx::new();
    vx.deploy(|ctx| UpperV {
        consumer: ctx.event_bus().consumer::<String>("upper").unwrap(),
    })
    .await
    .unwrap();
    vx.deploy(|ctx| CallerV {
        consumer: ctx.event_bus().consumer::<String>("caller").unwrap(),
        bus: ctx.event_bus(),
    })
    .await
    .unwrap();
    let r: String = vx
        .event_bus()
        .request("caller", "hello".to_string())
        .await
        .unwrap();
    assert_eq!(r, "HELLO");
}

// ---------------------------------------------------------------------------
// WorkerVerticle + execute_blocking
// ---------------------------------------------------------------------------

struct EchoWorker {
    consumer: Consumer<String>,
}
#[async_trait]
impl WorkerVerticle for EchoWorker {
    async fn run(mut self, _ctx: Context) -> Result<()> {
        while let Some(env) = self.consumer.recv().await {
            let body = env.body().clone();
            let _ = env.reply(format!("worker:{body}"));
        }
        Ok(())
    }
}

#[tokio::test]
async fn worker_verticle_processes_messages() {
    let vx = Vertx::new();
    vx.deploy_worker(EchoWorker {
        consumer: vx.event_bus().consumer::<String>("worker-echo").unwrap(),
    })
    .await
    .unwrap();
    let r: String = vx
        .event_bus()
        .request("worker-echo", "hi".to_string())
        .await
        .unwrap();
    assert_eq!(r, "worker:hi");
}

struct ComputeV {
    consumer: Consumer<u64>,
}
#[async_trait(?Send)]
impl Verticle for ComputeV {
    async fn run(mut self, ctx: Context) -> Result<()> {
        let cancel = ctx.cancellation_token();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                msg = self.consumer.recv() => {
                    let Some(env) = msg else { return Ok(()) };
                    let n = *env.body();
                    let out = ctx
                        .execute_blocking(move || {
                            std::thread::sleep(Duration::from_millis(5));
                            n * n
                        })
                        .await
                        .unwrap();
                    let _ = env.reply(out);
                }
            }
        }
    }
}

#[tokio::test]
async fn execute_blocking_from_event_loop_verticle() {
    let vx = Vertx::new();
    vx.deploy(|ctx| ComputeV {
        consumer: ctx.event_bus().consumer::<u64>("compute").unwrap(),
    })
    .await
    .unwrap();
    let n: u64 = vx.event_bus().request("compute", 7u64).await.unwrap();
    assert_eq!(n, 49);
}

// ---------------------------------------------------------------------------
// Context introspection (Vert.x parity)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn context_current_is_none_outside_vertx() {
    assert!(Context::current().is_none());
    assert!(!Context::is_on_event_loop_thread());
    assert!(!Context::is_on_worker_thread());
    assert!(!Context::is_on_vertx_thread());
}

#[derive(Clone)]
struct ProbeOut {
    threading_model: vertx_rs::ThreadingModel,
    is_on_el: bool,
    is_on_worker: bool,
    is_on_vertx: bool,
    deployment_id: Option<vertx_rs::DeploymentId>,
    owner_present: bool,
}

struct ProbeV {
    out: Arc<std::sync::Mutex<Option<ProbeOut>>>,
}
#[async_trait(?Send)]
impl Verticle for ProbeV {
    async fn run(self, ctx: Context) -> Result<()> {
        *self.out.lock().unwrap() = Some(ProbeOut {
            threading_model: ctx.threading_model(),
            is_on_el: Context::is_on_event_loop_thread(),
            is_on_worker: Context::is_on_worker_thread(),
            is_on_vertx: Context::is_on_vertx_thread(),
            deployment_id: ctx.deployment_id(),
            owner_present: ctx.owner().is_some(),
        });
        ctx.cancellation_token().cancelled().await;
        Ok(())
    }
}

#[tokio::test]
async fn context_introspection_event_loop_full() {
    let vx = Vertx::new();
    let out: Arc<std::sync::Mutex<Option<ProbeOut>>> = Arc::new(Default::default());
    let out2 = Arc::clone(&out);
    let dep = vx.deploy(move |_| ProbeV { out: out2 }).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let r = out.lock().unwrap().clone().unwrap();
    assert_eq!(r.threading_model, vertx_rs::ThreadingModel::EventLoop);
    assert!(r.is_on_el);
    assert!(!r.is_on_worker);
    assert!(r.is_on_vertx);
    assert_eq!(r.deployment_id, Some(dep));
    assert!(r.owner_present);
}

struct WorkerProbeV {
    out: Arc<std::sync::Mutex<Option<ProbeOut>>>,
}
#[async_trait]
impl WorkerVerticle for WorkerProbeV {
    async fn run(self, ctx: Context) -> Result<()> {
        *self.out.lock().unwrap() = Some(ProbeOut {
            threading_model: ctx.threading_model(),
            is_on_el: Context::is_on_event_loop_thread(),
            is_on_worker: Context::is_on_worker_thread(),
            is_on_vertx: Context::is_on_vertx_thread(),
            deployment_id: ctx.deployment_id(),
            owner_present: ctx.owner().is_some(),
        });
        ctx.cancellation_token().cancelled().await;
        Ok(())
    }
}

#[tokio::test]
async fn context_introspection_worker_full() {
    let vx = Vertx::new();
    let out: Arc<std::sync::Mutex<Option<ProbeOut>>> = Arc::new(Default::default());
    let dep = vx
        .deploy_worker(WorkerProbeV { out: Arc::clone(&out) })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let r = out.lock().unwrap().clone().unwrap();
    assert_eq!(r.threading_model, vertx_rs::ThreadingModel::Worker);
    assert!(!r.is_on_el);
    assert!(r.is_on_worker);
    assert!(r.is_on_vertx);
    assert_eq!(r.deployment_id, Some(dep));
    assert!(r.owner_present);
}

// ---------------------------------------------------------------------------
// run_on_context: fire-and-forget hop into an EventLoop
// ---------------------------------------------------------------------------

/// Verticle that broadcasts its Context for the test to grab.
struct CtxBroker {
    slot: Arc<std::sync::Mutex<Option<Context>>>,
}
#[async_trait(?Send)]
impl Verticle for CtxBroker {
    async fn run(self, ctx: Context) -> Result<()> {
        *self.slot.lock().unwrap() = Some(ctx.clone());
        ctx.cancellation_token().cancelled().await;
        Ok(())
    }
}

#[tokio::test]
async fn external_thread_can_hop_into_event_loop_via_run_on_context() {
    let vx = Vertx::new();
    let slot: Arc<std::sync::Mutex<Option<Context>>> = Arc::new(Default::default());
    let s2 = Arc::clone(&slot);
    vx.deploy(move |_| CtxBroker { slot: s2 }).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert!(Context::current().is_none());
    let el_ctx = slot.lock().unwrap().clone().unwrap();
    let expected = el_ctx.event_loop_id().unwrap();

    let (tx, rx) = tokio::sync::oneshot::channel();
    el_ctx
        .run_on_context(move || {
            let id = Context::current().and_then(|c| c.event_loop_id()).unwrap();
            let _ = tx.send(id);
        })
        .unwrap();
    let observed = rx.await.unwrap();
    assert_eq!(observed, expected);
}

#[tokio::test]
async fn run_on_context_after_undeploy_returns_terminated() {
    let vx = Vertx::new();
    let slot: Arc<std::sync::Mutex<Option<Context>>> = Arc::new(Default::default());
    let s2 = Arc::clone(&slot);
    let id = vx.deploy(move |_| CtxBroker { slot: s2 }).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let captured = slot.lock().unwrap().clone().unwrap();
    assert!(captured.is_alive());

    vx.undeploy(id).await.unwrap();
    assert!(!captured.is_alive());
    let res = captured.run_on_context(|| {});
    assert!(matches!(res, Err(VertxError::DeploymentTerminated(_))));
}

#[tokio::test]
async fn execute_blocking_after_undeploy_returns_terminated() {
    let vx = Vertx::new();
    let slot: Arc<std::sync::Mutex<Option<Context>>> = Arc::new(Default::default());
    let s2 = Arc::clone(&slot);
    let id = vx.deploy(move |_| CtxBroker { slot: s2 }).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let captured = slot.lock().unwrap().clone().unwrap();
    vx.undeploy(id).await.unwrap();
    let res: Result<u32> = captured.execute_blocking(|| 1u32).await;
    assert!(matches!(res, Err(VertxError::DeploymentTerminated(_))));
}

#[tokio::test]
async fn bound_consumer_after_undeploy_returns_terminated() {
    let vx = Vertx::new();
    let slot: Arc<std::sync::Mutex<Option<Context>>> = Arc::new(Default::default());
    let s2 = Arc::clone(&slot);
    let id = vx.deploy(move |_| CtxBroker { slot: s2 }).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let captured = slot.lock().unwrap().clone().unwrap();
    let bound_bus = captured.event_bus();
    vx.undeploy(id).await.unwrap();
    let res: Result<Consumer<String>> = bound_bus.consumer("anywhere");
    assert!(matches!(res, Err(VertxError::DeploymentTerminated(_))));
}

// ---------------------------------------------------------------------------
// Timer / Periodic
// ---------------------------------------------------------------------------

#[tokio::test]
async fn vertx_timer_fires_after_delay() {
    let vx = Vertx::new();
    let start = std::time::Instant::now();
    vx.timer(Duration::from_millis(50)).await.unwrap();
    assert!(start.elapsed() >= Duration::from_millis(45));
}

#[tokio::test]
async fn vertx_timer_explicit_cancel() {
    let vx = Vertx::new();
    let timer = vx.timer(Duration::from_secs(10));
    timer.cancel();
    let res = timer.await;
    assert!(matches!(res, Err(VertxError::TimerCancelled)));
}

#[tokio::test]
async fn vertx_periodic_ticks_multiple_times() {
    let vx = Vertx::new();
    let mut p = vx.periodic(Duration::from_millis(10), Duration::from_millis(10));
    for _ in 0..3 {
        p.tick().await.unwrap();
    }
    p.cancel();
    let res = p.tick().await;
    assert!(matches!(res, Err(VertxError::TimerCancelled)));
}

// Bound timer dies with the verticle.
struct TimerHolderV {
    timer: vertx_rs::Timer,
    fired: Arc<AtomicUsize>,
    cancelled: Arc<AtomicUsize>,
}
#[async_trait(?Send)]
impl Verticle for TimerHolderV {
    async fn run(self, _ctx: Context) -> Result<()> {
        match self.timer.await {
            Ok(()) => {
                self.fired.fetch_add(1, Ordering::SeqCst);
            }
            Err(VertxError::TimerCancelled) => {
                self.cancelled.fetch_add(1, Ordering::SeqCst);
            }
            Err(_) => {}
        }
        Ok(())
    }
}

#[tokio::test]
async fn bound_timer_cancels_on_undeploy() {
    let vx = Vertx::new();
    let fired = Arc::new(AtomicUsize::new(0));
    let cancelled = Arc::new(AtomicUsize::new(0));
    let f = Arc::clone(&fired);
    let c = Arc::clone(&cancelled);
    let id = vx
        .deploy(move |ctx| TimerHolderV {
            timer: ctx.timer(Duration::from_secs(10)),
            fired: f,
            cancelled: c,
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(fired.load(Ordering::SeqCst), 0);
    vx.undeploy(id).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(fired.load(Ordering::SeqCst), 0);
    assert_eq!(cancelled.load(Ordering::SeqCst), 1);
}

// ---------------------------------------------------------------------------
// EventBus core
// ---------------------------------------------------------------------------

#[tokio::test]
async fn publish_delivers_to_all_subscribers() {
    let vx = Vertx::new();
    let bus = vx.event_bus();
    let mut c1 = bus.consumer::<String>("news").unwrap();
    let mut c2 = bus.consumer::<String>("news").unwrap();
    bus.publish("news", "breaking".to_string()).unwrap();
    assert_eq!(c1.recv().await.unwrap().body(), "breaking");
    assert_eq!(c2.recv().await.unwrap().body(), "breaking");
}

#[tokio::test]
async fn send_with_no_consumer_errors() {
    let vx = Vertx::new();
    let err = vx.event_bus().send("ghost", 1u32).await.unwrap_err();
    assert!(matches!(err, VertxError::NoConsumer(_)));
}

#[tokio::test]
async fn request_timeout_fires() {
    let vx = Vertx::new();
    let bus = vx.event_bus();
    let _c = bus.consumer::<String>("holdme").unwrap();
    let res: std::result::Result<String, _> = bus
        .request_with("holdme", "x".to_string(), DeliveryOptions::new().with_timeout(Duration::from_millis(30)))
        .await;
    assert!(matches!(res, Err(VertxError::Timeout(_))));
}

#[tokio::test]
async fn dropping_consumer_unregisters() {
    let vx = Vertx::new();
    let bus = vx.event_bus();
    {
        let _c = bus.consumer::<u32>("temp").unwrap();
        bus.send("temp", 1u32).await.unwrap();
    }
    let err = bus.send("temp", 2u32).await.unwrap_err();
    assert!(matches!(err, VertxError::NoConsumer(_)));
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

#[tokio::test]
async fn default_worker_pool_size_is_20() {
    let vx = Vertx::new();
    assert_eq!(vx.worker_thread_count(), 20);
}

#[tokio::test]
async fn builder_overrides() {
    let vx = Vertx::builder()
        .event_loop_threads(2)
        .worker_threads(4)
        .build();
    assert_eq!(vx.event_loop_count(), 2);
    assert_eq!(vx.worker_thread_count(), 4);
}

// ---------------------------------------------------------------------------
// Vert.x-style handler API: Consumer::handler(fn) spawns its own loop.
// User Verticle::run just waits for cancellation (or is a no-op start hook).
// ---------------------------------------------------------------------------

struct HandlerPingerV {
    consumer: Consumer<String>,
}

#[async_trait(?Send)]
impl Verticle for HandlerPingerV {
    async fn run(self, ctx: Context) -> Result<()> {
        // Single-handler state: just `move` the counter into the FnMut.
        // No Rc, no Cell, no interior mutability — zero overhead, compile-time safe.
        let mut count: u64 = 0;
        self.consumer.handler(move |env| {
            count += 1;
            let n = count;
            async move {
                env.reply(format!("hpong#{n}"))?;
                Ok(())
            }
        });
        ctx.cancellation_token().cancelled().await;
        Ok(())
    }
}

#[tokio::test]
async fn consumer_handler_dispatches_envelopes() {
    let vx = Vertx::new();
    vx.deploy(|ctx| HandlerPingerV {
        consumer: ctx.event_bus().consumer::<String>("hping").unwrap(),
    })
    .await
    .unwrap();

    let bus = vx.event_bus();
    let r1: String = bus
        .request_with("hping", "x".to_string(), DeliveryOptions::new().with_timeout(Duration::from_secs(2)))
        .await
        .unwrap();
    let r2: String = bus
        .request_with("hping", "x".to_string(), DeliveryOptions::new().with_timeout(Duration::from_secs(2)))
        .await
        .unwrap();
    assert_eq!(r1, "hpong#1");
    assert_eq!(r2, "hpong#2");
}

struct HandlerCancelV {
    consumer: Consumer<String>,
}
#[async_trait(?Send)]
impl Verticle for HandlerCancelV {
    async fn run(self, ctx: Context) -> Result<()> {
        self.consumer.handler(|env| async move {
            env.reply(1u64).ok();
            Ok(())
        });
        ctx.cancellation_token().cancelled().await;
        Ok(())
    }
}

#[tokio::test]
async fn consumer_handler_unregisters_on_undeploy() {
    let vx = Vertx::new();
    let dep = vx
        .deploy(|ctx| HandlerCancelV {
            consumer: ctx.event_bus().consumer::<String>("hping2").unwrap(),
        })
        .await
        .unwrap();

    vx.event_bus()
        .request_with::<_, u64>("hping2", "x".to_string(), DeliveryOptions::new().with_timeout(Duration::from_secs(2)))
        .await
        .unwrap();

    vx.undeploy(dep).await.unwrap();
    let res = vx
        .event_bus()
        .request_with::<_, u64>("hping2", "x".to_string(), DeliveryOptions::new().with_timeout(Duration::from_millis(100)))
        .await;
    assert!(res.is_err());
}

// ---------------------------------------------------------------------------
// Reference pattern: when multiple handlers share complex mutable state,
// `Rc<RefCell<T>>` is the right tool. Discipline: keep borrow scope tiny,
// never hold a borrow across `.await`.
// ---------------------------------------------------------------------------

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

struct StatsV {
    incr_consumer: Consumer<String>,
    get_consumer: Consumer<String>,
    counts: Rc<RefCell<HashMap<String, u64>>>,
}

#[async_trait(?Send)]
impl Verticle for StatsV {
    async fn run(self, ctx: Context) -> Result<()> {
        let counts_incr = self.counts.clone();
        self.incr_consumer.handler(move |env| {
            // Borrow scope is exactly one statement — no `.await` between
            // `borrow_mut()` and drop, so no panic risk.
            let n = {
                let mut m = counts_incr.borrow_mut();
                let n = m.entry(env.body().clone()).or_insert(0);
                *n += 1;
                *n
            };
            async move { env.reply(n)?; Ok(()) }
        });

        let counts_get = self.counts.clone();
        self.get_consumer.handler(move |env| {
            let n = counts_get.borrow().get(env.body()).copied().unwrap_or(0);
            async move { env.reply(n)?; Ok(()) }
        });

        ctx.cancellation_token().cancelled().await;
        Ok(())
    }
}

#[tokio::test]
async fn rc_refcell_shared_state_across_handlers() {
    let vx = Vertx::new();
    vx.deploy(|ctx| {
        let bus = ctx.event_bus();
        StatsV {
            incr_consumer: bus.consumer::<String>("stats.incr").unwrap(),
            get_consumer: bus.consumer::<String>("stats.get").unwrap(),
            counts: Rc::new(RefCell::new(HashMap::new())),
        }
    })
    .await
    .unwrap();

    let bus = vx.event_bus();
    let _: u64 = bus.request("stats.incr", "a".to_string()).await.unwrap();
    let _: u64 = bus.request("stats.incr", "a".to_string()).await.unwrap();
    let _: u64 = bus.request("stats.incr", "b".to_string()).await.unwrap();
    let a: u64 = bus.request("stats.get", "a".to_string()).await.unwrap();
    let b: u64 = bus.request("stats.get", "b".to_string()).await.unwrap();
    let c: u64 = bus.request("stats.get", "c".to_string()).await.unwrap();
    assert_eq!((a, b, c), (2, 1, 0));
}

// ---------------------------------------------------------------------------
// Pattern: multiple handlers sharing a Copy scalar via Rc<Cell<T>>.
// No borrow guard → no possible panic, lighter than Rc<RefCell>.
// ---------------------------------------------------------------------------

struct CounterPairV {
    incr_consumer: Consumer<()>,
    read_consumer: Consumer<()>,
    count: Rc<Cell<u64>>,
}

#[async_trait(?Send)]
impl Verticle for CounterPairV {
    async fn run(self, ctx: Context) -> Result<()> {
        let c_incr = self.count.clone();
        self.incr_consumer.handler(move |env| {
            let n = c_incr.get() + 1;
            c_incr.set(n);
            async move { env.reply(n)?; Ok(()) }
        });

        let c_read = self.count.clone();
        self.read_consumer.handler(move |env| {
            let n = c_read.get();
            async move { env.reply(n)?; Ok(()) }
        });

        ctx.cancellation_token().cancelled().await;
        Ok(())
    }
}

#[tokio::test]
async fn rc_cell_shared_scalar_across_handlers() {
    let vx = Vertx::new();
    vx.deploy(|ctx| {
        let bus = ctx.event_bus();
        CounterPairV {
            incr_consumer: bus.consumer::<()>("ctr.incr").unwrap(),
            read_consumer: bus.consumer::<()>("ctr.read").unwrap(),
            count: Rc::new(Cell::new(0)),
        }
    })
    .await
    .unwrap();

    let bus = vx.event_bus();
    let _: u64 = bus.request("ctr.incr", ()).await.unwrap();
    let _: u64 = bus.request("ctr.incr", ()).await.unwrap();
    let _: u64 = bus.request("ctr.incr", ()).await.unwrap();
    let n: u64 = bus.request("ctr.read", ()).await.unwrap();
    assert_eq!(n, 3);
}

// ---------------------------------------------------------------------------
// Pattern: handler coordination via an internal EventBus address.
// No shared mutability — each handler owns its own slice of state and
// communicates by sending messages. Actor-style.
// ---------------------------------------------------------------------------

struct AdderV {
    add_consumer: Consumer<u64>,
}

#[async_trait(?Send)]
impl Verticle for AdderV {
    async fn run(self, ctx: Context) -> Result<()> {
        // `total` is owned solely by the accumulator handler. The frontend
        // handler delegates by sending an "adder.apply" message and waiting
        // for a reply — no Rc, no RefCell, no shared state at all.
        let bus = ctx.event_bus();
        let internal_bus = bus.clone();
        let frontend = bus.consumer::<u64>("adder.add").unwrap();
        frontend.handler(move |env| {
            let internal_bus = internal_bus.clone();
            async move {
                let body = *env.body();
                let total: u64 = internal_bus.request("adder.apply", body).await?;
                env.reply(total)?;
                Ok(())
            }
        });

        let mut total: u64 = 0;
        self.add_consumer.handler(move |env| {
            total += *env.body();
            let snapshot = total;
            async move { env.reply(snapshot)?; Ok(()) }
        });

        ctx.cancellation_token().cancelled().await;
        Ok(())
    }
}

#[tokio::test]
async fn message_passing_between_handlers() {
    let vx = Vertx::new();
    vx.deploy(|ctx| AdderV {
        add_consumer: ctx.event_bus().consumer::<u64>("adder.apply").unwrap(),
    })
    .await
    .unwrap();

    let bus = vx.event_bus();
    let r1: u64 = bus.request("adder.add", 10u64).await.unwrap();
    let r2: u64 = bus.request("adder.add", 5u64).await.unwrap();
    let r3: u64 = bus.request("adder.add", 7u64).await.unwrap();
    assert_eq!((r1, r2, r3), (10, 15, 22));
}

// ---------------------------------------------------------------------------
// SharedData escape hatch: Vert.x `Counter` ≈ `Arc<AtomicU64>` for cross-
// verticle scalar counts. No framework wrapper needed.
// ---------------------------------------------------------------------------

use std::sync::atomic::AtomicU64;

struct CtrIncrV {
    consumer: Consumer<u64>,
    counter: Arc<AtomicU64>,
}
#[async_trait(?Send)]
impl Verticle for CtrIncrV {
    async fn run(self, ctx: Context) -> Result<()> {
        let c = self.counter.clone();
        self.consumer.handler(move |env| {
            let n = c.fetch_add(*env.body(), Ordering::Relaxed) + *env.body();
            async move { env.reply(n)?; Ok(()) }
        });
        ctx.cancellation_token().cancelled().await;
        Ok(())
    }
}

struct CtrReadV {
    consumer: Consumer<()>,
    counter: Arc<AtomicU64>,
}
#[async_trait(?Send)]
impl Verticle for CtrReadV {
    async fn run(self, ctx: Context) -> Result<()> {
        let c = self.counter.clone();
        self.consumer.handler(move |env| {
            let n = c.load(Ordering::Relaxed);
            async move { env.reply(n)?; Ok(()) }
        });
        ctx.cancellation_token().cancelled().await;
        Ok(())
    }
}

#[tokio::test]
async fn arc_atomic_counter_shared_across_verticles() {
    let vx = Vertx::new();
    let counter = Arc::new(AtomicU64::new(0));

    let c1 = counter.clone();
    vx.deploy(move |ctx| CtrIncrV {
        consumer: ctx.event_bus().consumer::<u64>("ctr.add").unwrap(),
        counter: c1,
    })
    .await
    .unwrap();

    let c2 = counter.clone();
    vx.deploy(move |ctx| CtrReadV {
        consumer: ctx.event_bus().consumer::<()>("ctr.get").unwrap(),
        counter: c2,
    })
    .await
    .unwrap();

    let bus = vx.event_bus();
    let _: u64 = bus.request("ctr.add", 3u64).await.unwrap();
    let _: u64 = bus.request("ctr.add", 4u64).await.unwrap();
    let n: u64 = bus.request("ctr.get", ()).await.unwrap();
    assert_eq!(n, 7);
}

// ---------------------------------------------------------------------------
// SharedData escape hatch: Vert.x `LocalMap` (immutable Shareable values)
// ≈ `Arc<T>` — share a read-only snapshot across verticles.
// ---------------------------------------------------------------------------

/// Immutable routing table built once at deploy time, shared by reference.
struct RouteTable {
    routes: HashMap<String, String>,
}

struct RouterV {
    consumer: Consumer<String>,
    table: Arc<RouteTable>,
}
#[async_trait(?Send)]
impl Verticle for RouterV {
    async fn run(self, ctx: Context) -> Result<()> {
        let t = self.table.clone();
        self.consumer.handler(move |env| {
            // Zero-copy read: just an Arc clone, then map lookup. No EventBus
            // round-trip for the hot path.
            let route = t.routes.get(env.body()).cloned().unwrap_or_default();
            async move { env.reply(route)?; Ok(()) }
        });
        ctx.cancellation_token().cancelled().await;
        Ok(())
    }
}

#[tokio::test]
async fn arc_immutable_snapshot_shared_across_verticles() {
    let vx = Vertx::new();
    let table = Arc::new(RouteTable {
        routes: [("a", "north"), ("b", "south")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    });

    let t1 = table.clone();
    vx.deploy(move |ctx| RouterV {
        consumer: ctx.event_bus().consumer::<String>("route.r1").unwrap(),
        table: t1,
    })
    .await
    .unwrap();
    let t2 = table.clone();
    vx.deploy(move |ctx| RouterV {
        consumer: ctx.event_bus().consumer::<String>("route.r2").unwrap(),
        table: t2,
    })
    .await
    .unwrap();

    let bus = vx.event_bus();
    let r1: String = bus.request("route.r1", "a".to_string()).await.unwrap();
    let r2: String = bus.request("route.r2", "b".to_string()).await.unwrap();
    let r3: String = bus.request("route.r1", "missing".to_string()).await.unwrap();
    assert_eq!((r1.as_str(), r2.as_str(), r3.as_str()), ("north", "south", ""));
}
