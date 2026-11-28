//! close 超时不取消后台清理；stop 可在共享 Worker 池执行 blocking 清理。
//! cargo run --example graceful_shutdown
//! 首次 ShutdownTimeout 是预期结果；不用 sleep 猜测清理进度。
mod support;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::oneshot;
use vertx_rs::{Context, Result, Verticle, Vertx, VertxError};

struct Service {
    task_started: Option<oneshot::Sender<()>>,
    cancelled: Option<oneshot::Sender<()>>,
    blocking_started: Option<oneshot::Sender<()>>,
    release: Option<std::sync::mpsc::Receiver<()>>,
    cleaned: Option<oneshot::Sender<()>>,
    stop_count: Arc<AtomicUsize>,
}
impl Verticle for Service {
    async fn start(&mut self, ctx: Context) -> Result<()> {
        let cancelled = self.cancelled.take().expect("start called once");
        let task_started = self.task_started.take().expect("start called once");
        let token = ctx.cancellation_token();
        ctx.spawn_local(async move {
            task_started.send(()).map_err(|_| VertxError::ReplyClosed)?;
            token.cancelled().await;
            cancelled.send(()).map_err(|_| VertxError::ReplyClosed)?;
            Ok(())
        })?;
        Ok(())
    }
    async fn stop(&mut self, ctx: Context) -> Result<()> {
        self.stop_count.fetch_add(1, Ordering::SeqCst);
        let release = self.release.take().expect("stop called once");
        let started = self.blocking_started.take().expect("stop called once");
        let value = ctx
            .execute_blocking(move || {
                assert!(Context::is_on_worker_thread());
                started.send(()).map_err(|_| VertxError::ReplyClosed)?;
                // 若外层业务超时或失败，发送端 Drop 也会解除这个等待。
                release
                    .recv()
                    .map_err(|_| VertxError::Other("cleanup gate closed".into()))?;
                Ok::<_, VertxError>(42)
            })
            .await??;
        assert_eq!(value, 42);
        self.cleaned
            .take()
            .expect("stop called once")
            .send(())
            .map_err(|_| VertxError::ReplyClosed)?;
        Ok(())
    }
}
async fn run(vx: &Vertx) -> Result<()> {
    let (task_started, task_ready) = oneshot::channel();
    let (cancelled, saw_cancel) = oneshot::channel();
    let (blocking_started, started) = oneshot::channel();
    let (release, gate) = std::sync::mpsc::channel();
    let (cleaned, finished) = oneshot::channel();
    let count = Arc::new(AtomicUsize::new(0));
    let stops = count.clone();
    vx.deploy(move |_| Service {
        task_started: Some(task_started),
        cancelled: Some(cancelled),
        blocking_started: Some(blocking_started),
        release: Some(gate),
        cleaned: Some(cleaned),
        stop_count: stops,
    })
    .await?;
    task_ready.await.map_err(|_| VertxError::ReplyClosed)?;
    let first = vx.close().await;
    assert!(matches!(first, Err(VertxError::ShutdownTimeout(_))));
    saw_cancel.await.map_err(|_| VertxError::ReplyClosed)?;
    started.await.map_err(|_| VertxError::ReplyClosed)?;
    assert!(vx.is_closed());
    assert!(matches!(
        vx.event_bus().send("new-work", ()).await,
        Err(VertxError::VertxClosed)
    ));
    println!("close timed out; cooperative task exited; blocking cleanup is still running");
    release.send(()).map_err(|_| VertxError::ReplyClosed)?;
    finished.await.map_err(|_| VertxError::ReplyClosed)?;
    vx.close().await?;
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let snapshot = vx.diagnostics();
    assert_eq!(snapshot.close_stage, "closed");
    assert_eq!(snapshot.active_tasks, 0);
    assert_eq!(snapshot.active_deployments, 0);
    assert_eq!(snapshot.active_consumers, 0);
    assert_eq!(snapshot.blocking_waiting + snapshot.blocking_running, 0);
    println!("cleanup completed once; subsequent close joined all executor threads");
    Ok(())
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    support::init_tracing();
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(2)
        .shutdown_grace(Duration::from_millis(250))
        .build()?;
    let outcome = support::bounded(run(&vx)).await;
    support::finish(&vx, outcome).await
}
