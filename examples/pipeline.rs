//! Source → 两个 Hasher EventLoop → Worker Sink；所有同步计算共用两个 OS Worker。
//!
//! 运行：RUST_LOG=vertx_rs=debug cargo run --example pipeline
//! LOG_FORMAT=json 输出结构化日志；预期：五个校验和，close 后 active_tasks=0。
//! Source 使用 pipe_to，Sink 使用 Stream::take 定义有限传输；小容量队列向上游传递背压。
//! ordered 只保证同来源 Context 的 blocking FIFO，不保证全流水线输出顺序。
//! handler 等待上一条处理结束后才消费下一条；增加消费者才会增加消息并行度。
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use std::time::Duration;
use tokio::sync::oneshot;
use vertx_rs::{
    pipe_to, ConsumerOptions, Context, DiagnosticsOptions, Result, Verticle, Vertx, VertxError,
    WorkerVerticle,
};
struct Source;
#[async_trait(?Send)]
impl Verticle for Source {
    async fn start(&mut self, ctx: Context) -> Result<()> {
        let bus = ctx.event_bus();
        // Finite source: EOF flushes and closes only this address-bound adapter.
        let source = futures::stream::iter((1..=5u64).map(Ok));
        let handle = ctx.spawn_local(pipe_to(source, bus.sink("hash")?))?;
        println!("source task={}", handle.id()); // 丢弃句柄只分离等待者，部署继续管理任务。
        Ok(())
    }
}
// 模拟批量校验工作，避免把微不足道的乘法转交线程池。
fn checksum(seed: u64) -> u64 {
    (0..100_000u64).fold(seed, |hash, value| {
        hash.rotate_left(5).wrapping_add(value ^ seed)
    })
}
struct Hasher;
#[async_trait(?Send)]
impl Verticle for Hasher {
    async fn start(&mut self, ctx: Context) -> Result<()> {
        ctx.event_bus()
            .consumer_with::<u64>("hash", ConsumerOptions::new().with_capacity(1))?
            .handler(move |env| {
                let ctx = ctx.clone();
                async move {
                    let seed = env.into_body();
                    // 两个 Future 同时等待：有序与无序 blocking 都进同一个固定 Worker 池。
                    let (ordered, unordered) = futures::try_join!(
                        ctx.execute_blocking(move || checksum(seed)),
                        ctx.execute_blocking_with(move || checksum(seed), false),
                    )?;
                    assert_eq!(ordered, unordered);
                    ctx.event_bus().send("sink", (seed, ordered)).await
                }
            })
    }
}
struct Sink {
    done: Option<oneshot::Sender<Vec<(u64, u64)>>>,
}
#[async_trait]
impl WorkerVerticle for Sink {
    async fn start(&mut self, ctx: Context) -> Result<()> {
        let done = self.done.take().expect("start is called once");
        let consumer = ctx
            .event_bus()
            .consumer_with::<(u64, u64)>("sink", ConsumerOptions::new().with_capacity(1))?;
        ctx.spawn(async move {
            // EventBus has no natural EOF: take(5) defines this operation's end.
            // Only one-way bodies are collected; headers/reply are not forwarded.
            let results = consumer
                .take(5)
                .map_ok(|env| env.into_body())
                .try_collect::<Vec<_>>()
                .await?;
            done.send(results).ok();
            Ok(())
        })?;
        Ok(())
    }
}
async fn run(vx: &Vertx) -> Result<()> {
    let (tx, rx) = oneshot::channel();
    vx.deploy_worker(|_| Sink { done: Some(tx) }).await?;
    vx.deploy(|_| Hasher).await?;
    vx.deploy(|_| Hasher).await?;
    vx.deploy(|_| Source).await?;
    print_snapshot("running", vx);
    let mut results = tokio::time::timeout(Duration::from_secs(2), rx)
        .await
        .map_err(|_| VertxError::Timeout(Duration::from_secs(2)))?
        .map_err(|_| VertxError::ReplyClosed)?;
    results.sort();
    for &(seed, hash) in &results {
        assert_eq!(checksum(seed), hash);
    }
    println!("sink got: {results:?}");
    Ok(())
}
fn print_snapshot(label: &str, vx: &Vertx) {
    let snapshot = vx.diagnostics();
    println!(
        "{label}: deployments={}, tasks={}, ready={}, blocking(wait/run)={}/{}, close={}",
        snapshot.active_deployments,
        snapshot.active_tasks,
        snapshot.ready_tasks,
        snapshot.blocking_waiting,
        snapshot.blocking_running,
        snapshot.close_stage
    );
    println!(
        "  queues: queued={}, reserved={}, waiting={}, full_events={}",
        snapshot.queued_messages,
        snapshot.reserved_slots,
        snapshot.waiting_senders,
        snapshot.queue_full_events
    );
    for queue in snapshot.queues {
        println!(
            "  {} / consumer#{}: {}/{}, reserved={}, waiting={}",
            queue.address,
            queue.consumer_id,
            queue.queued,
            queue.capacity,
            queue.reserved,
            queue.waiting_senders
        );
    }
    for task in snapshot.tasks {
        println!(
            "  {} {} {:?}, elapsed={:?}",
            task.task_id, task.context_id, task.stage, task.elapsed
        );
    }
    for thread in snapshot.threads {
        println!(
            "  {:?}: {} {}, running={:?}",
            thread.thread_id,
            thread.task_id,
            thread.operation,
            thread.started_at.elapsed()
        );
    }
}
fn init_tracing() {
    let filter =
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into());
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    if std::env::var("LOG_FORMAT").as_deref() == Ok("json") {
        builder.json().init();
    } else {
        builder.init();
    }
}
async fn finish(vx: &Vertx, outcome: Result<()>) -> Result<()> {
    match (outcome, vx.close().await) {
        (Ok(()), close) => close,
        (Err(cause), Ok(())) => Err(cause),
        (Err(cause), Err(cleanup)) => Err(VertxError::LifecycleFailed {
            cause: Box::new(cause),
            cleanup: vec![cleanup],
        }),
    }
}
#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let vx = Vertx::builder()
        .event_loop_threads(2)
        .worker_threads(2)
        .diagnostics(DiagnosticsOptions {
            task_details: true,
            queue_details: true,
            max_queue_details: 32,
            max_task_details: 32,
            ..Default::default()
        })
        .build();
    let outcome = tokio::time::timeout(Duration::from_secs(5), run(&vx))
        .await
        .unwrap_or_else(|_| Err(VertxError::Timeout(Duration::from_secs(5))));
    let result = finish(&vx, outcome).await;
    print_snapshot("closed", &vx);
    result
}
