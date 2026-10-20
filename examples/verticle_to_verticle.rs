//! main → Caller → Upper / Reject；通过真实服务展示三种 Future 组合。
//!
//! 运行：RUST_LOG=vertx_rs=debug cargo run --example verticle_to_verticle
//! LOG_FORMAT=json 输出 JSON；预期：HELLO、VERTX、RUST，以及 join_all 的业务失败。
//! try_join_all 遇错提前返回，join_all 保留每个结果，select_ok 等第一个成功。
//! 组合器丢弃普通 Future 会停止继续 poll；不会撤回已入队消息。
//! 已 spawn 的任务独立运行，丢弃 TaskHandle 不会取消，需 abort 或 undeploy。
//! 未获选服务的迟到回复可能返回 ReplyClosed；服务显式处理这个预期结果。
use async_trait::async_trait;
use futures::{
    future::{join_all, select_ok, try_join_all},
    FutureExt,
};
use std::time::Duration;
use vertx_rs::{Context, DeliveryOptions, Result, Verticle, Vertx, VertxError};
fn allow_late_reply(result: Result<()>) -> Result<()> {
    match result {
        Err(VertxError::ReplyClosed) => {
            tracing::debug!("request waiter detached before reply");
            Ok(())
        }
        result => result,
    }
}
struct Upper;
#[async_trait(?Send)]
impl Verticle for Upper {
    async fn start(&mut self, ctx: Context) -> Result<()> {
        ctx.event_bus()
            .consumer::<String>("upper")?
            .handler(|env| async move {
                let response = env.body().to_uppercase();
                allow_late_reply(env.reply(response))
            })
    }
}
struct Reject;
#[async_trait(?Send)]
impl Verticle for Reject {
    async fn start(&mut self, ctx: Context) -> Result<()> {
        ctx.event_bus()
            .consumer::<String>("reject")?
            .handler(|env| async move { allow_late_reply(env.fail(503, "service unavailable")) })
    }
}
struct Caller;
#[async_trait(?Send)]
impl Verticle for Caller {
    async fn start(&mut self, ctx: Context) -> Result<()> {
        let bus = ctx.event_bus();
        bus.consumer::<String>("caller")?.handler(move |env| {
            let bus = bus.clone();
            async move {
                let candidates = ["reject", "upper"].into_iter().map(|address| {
                    bus.request_with::<_, String>(
                        address,
                        env.body().clone(),
                        DeliveryOptions::new().with_timeout(Duration::from_secs(1)),
                    )
                    .boxed()
                });
                let (response, remaining) = select_ok(candidates).await?;
                drop(remaining);
                env.reply(response)
            }
        })
    }
}
async fn run(vx: &Vertx) -> Result<()> {
    vx.deploy(|_| Upper).await?;
    vx.deploy(|_| Reject).await?;
    vx.deploy(|_| Caller).await?;
    let bus = vx.event_bus();
    let responses = try_join_all(["hello", "vertx", "rust"].into_iter().map(|word| {
        bus.request_with::<_, String>(
            "caller",
            word.to_owned(),
            DeliveryOptions::new().with_timeout(Duration::from_secs(1)),
        )
    }))
    .await?;
    println!("all successful: {responses:?}");
    let outcomes = join_all(["upper", "reject"].into_iter().map(|address| {
        bus.request_with::<_, String>(
            address,
            "example".to_owned(),
            DeliveryOptions::new().with_timeout(Duration::from_secs(1)),
        )
    }))
    .await;
    assert!(matches!(
        &outcomes[1],
        Err(VertxError::RecipientFailure { code: 503, .. })
    ));
    println!("all finished: {outcomes:?}");
    Ok(())
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
        .build();
    let outcome = tokio::time::timeout(Duration::from_secs(5), run(&vx))
        .await
        .unwrap_or_else(|_| Err(VertxError::Timeout(Duration::from_secs(5))));
    finish(&vx, outcome).await
}
