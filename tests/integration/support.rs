use rstest::fixture;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use vertx_rs::{Context, Result, Verticle, Vertx, VertxError, WorkerVerticle};

#[fixture]
pub(super) fn vx() -> Vertx {
    Vertx::builder()
        .event_loop_threads(2)
        .worker_threads(2)
        .shutdown_grace(Duration::from_millis(500))
        .build()
        .unwrap()
}

/// Gates a test body on running in a child that the parent can stop on deadlock.
pub(super) fn in_test_subprocess(name: &str) -> bool {
    const CHILD_TEST: &str = "VERTX_RS_CHILD_TEST";
    if std::env::var(CHILD_TEST).as_deref() == Ok(name) {
        return true;
    }
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture"])
        .env(CHILD_TEST, name)
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "child test {name} failed: {status}");
            return false;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            child.wait().unwrap();
            panic!("child test {name} did not complete in five seconds");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

pub(super) struct Local<F>(Option<F>);

pub(super) fn local<F, Fut>(f: F) -> Local<F>
where
    F: FnOnce(Context) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    Local(Some(f))
}
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
    pub(super) id: vertx_rs::DeploymentId,
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
