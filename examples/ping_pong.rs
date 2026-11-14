//! 请求、headers、业务失败与超时；Rc<Cell> 说明 !Send 状态的线程亲和性。
//!
//! main --request--> ping / 固定 EventLoop --reply--> main
//!
//! 运行：RUST_LOG=vertx_rs=debug cargo run --example ping_pong
//! LOG_FORMAT=json 可输出 JSON 日志；预期：pong#1、recipient_failure、timeout、stop。
//! 超时不会撤回已入队消息；同步慢操作必须放进 execute_blocking。
use async_trait::async_trait;
use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;
use vertx_rs::{Context, DeliveryOptions, Result, Verticle, Vertx, VertxError};
struct Pinger {
    count: Rc<Cell<u64>>,
}
#[async_trait(?Send)]
impl Verticle for Pinger {
    async fn start(&mut self, ctx: Context) -> Result<()> {
        let count = self.count.clone();
        let thread = std::thread::current().id();
        println!("start {} on {thread:?}", ctx.id());
        ctx.event_bus()
            .consumer::<String>("ping")?
            .handler(move |env| {
                let (ctx, count) = (ctx.clone(), count.clone());
                async move {
                    assert_eq!(std::thread::current().id(), thread);
                    assert_eq!(Context::current().unwrap().id(), ctx.id());
                    if env.body() == "fail" {
                        return env.fail(400, "invalid ping");
                    }
                    if env.body() == "slow" {
                        ctx.timer(Duration::from_millis(40))?.await?;
                        // 请求方已超时，迟到回复是可预期的 ReplyClosed。
                        assert!(matches!(
                            env.reply("late".to_owned()),
                            Err(VertxError::ReplyClosed)
                        ));
                        return Ok(());
                    }
                    assert_eq!(
                        env.headers().get("caller").map(String::as_str),
                        Some("example")
                    );
                    count.set(count.get() + 1);
                    env.reply_with(
                        format!("pong#{}", count.get()),
                        DeliveryOptions::new().header("served-by", ctx.id().to_string()),
                    )
                }
            })
    }
    async fn stop(&mut self, ctx: Context) -> Result<()> {
        println!(
            "stop {} on {:?}, count={}",
            ctx.id(),
            std::thread::current().id(),
            self.count.get()
        );
        Ok(())
    }
}
async fn run(vx: &Vertx) -> Result<()> {
    vx.deploy(|_| Pinger {
        count: Rc::new(Cell::new(0)),
    })
    .await?;
    let bus = vx.event_bus();
    let reply = bus
        .request_message_with::<_, String>(
            "ping",
            "hello".to_owned(),
            DeliveryOptions::new()
                .header("caller", "example")
                .with_timeout(Duration::from_secs(1)),
        )
        .await?;
    println!(
        "{}; served-by={:?}",
        reply.body(),
        reply.headers().get("served-by")
    );
    let failure: Result<String> = bus.request("ping", "fail".to_owned()).await;
    assert!(matches!(
        failure,
        Err(VertxError::RecipientFailure { code: 400, .. })
    ));
    println!("recipient_failure");
    let timeout: Result<String> = bus
        .request_with(
            "ping",
            "slow".to_owned(),
            DeliveryOptions::new().with_timeout(Duration::from_millis(5)),
        )
        .await;
    assert!(matches!(timeout, Err(VertxError::Timeout(_))));
    println!("timeout");
    // 下一次请求排在 slow 后面，确保迟到回复路径实际运行。
    let _: String = bus
        .request_with(
            "ping",
            "hello".to_owned(),
            DeliveryOptions::new()
                .header("caller", "example")
                .with_timeout(Duration::from_secs(1)),
        )
        .await?;
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
