//! 请求、headers、业务失败与超时；Rc<Cell> 说明 !Send 状态的线程亲和性。
//!
//! main --request--> ping / 固定 EventLoop --reply--> main
//!
//! 运行：RUST_LOG=vertx_rs=debug cargo run --example ping_pong
//! LOG_FORMAT=json 可输出 JSON 日志；预期：pong#1、recipient_failure、timeout、stop。
//! 超时不会撤回已入队消息；同步慢操作必须放进 execute_blocking。
//! 三次周期心跳广播给两个订阅者；定时器取消不会撤销 Pinger 部署。
mod support;
use futures::{StreamExt, TryStreamExt};
use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;
use vertx_rs::{Context, DeliveryOptions, RequestOptions, Result, Verticle, Vertx, VertxError};
struct Pinger {
    count: Rc<Cell<u64>>,
}
impl Verticle for Pinger {
    async fn start(&mut self, ctx: Context) -> Result<()> {
        let count = self.count.clone();
        let thread = std::thread::current().id();
        println!("start {} on {thread:?}", ctx.id());
        let heartbeat_ctx = ctx.clone();
        ctx.spawn_local(async move {
            let mut timer = heartbeat_ctx.periodic(Duration::ZERO, Duration::from_millis(10))?;
            for tick in 1..=3u32 {
                timer.tick().await?;
                heartbeat_ctx.event_bus().publish("heartbeat", tick)?;
            }
            timer.cancel();
            assert!(timer.is_cancelled());
            println!("heartbeat timer cancelled after 3 ticks");
            Ok(())
        })?;
        ctx.event_bus()
            .consumer::<String>("ping")?
            .handler(async move |env| {
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
                assert_eq!(env.headers().get("caller"), Some("example"));
                count.set(count.get() + 1);
                env.reply_with(
                    format!("pong#{}", count.get()),
                    DeliveryOptions::new().header("served-by", ctx.id().to_string()),
                )
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
    // 先订阅再启动生产者。publish 不等待空位；此处容量足以容纳全部三条。
    let bus = vx.event_bus();
    let monitor = bus.consumer::<u32>("heartbeat")?;
    let audit = bus.consumer::<u32>("heartbeat")?;
    vx.deploy(|_| Pinger {
        count: Rc::new(Cell::new(0)),
    })
    .await?;
    let (monitor_ticks, audit_ticks): (Vec<u32>, Vec<u32>) = futures::try_join!(
        monitor.take(3).map_ok(|env| env.into_body()).try_collect(),
        audit.take(3).map_ok(|env| env.into_body()).try_collect(),
    )?;
    assert_eq!(monitor_ticks, [1, 2, 3]);
    assert_eq!(monitor_ticks, audit_ticks);
    println!("heartbeat broadcast: monitor={monitor_ticks:?}, audit={audit_ticks:?}");
    // 心跳任务已结束，但 start 返回后的部署仍然可以处理请求。
    let reply = bus
        .request_message_with::<_, String>(
            "ping",
            "hello".to_owned(),
            RequestOptions::new()
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
            RequestOptions::new().with_timeout(Duration::from_millis(5)),
        )
        .await;
    assert!(matches!(timeout, Err(VertxError::Timeout(_))));
    println!("timeout");
    // 下一次请求排在 slow 后面，确保迟到回复路径实际运行。
    let _: String = bus
        .request_with(
            "ping",
            "hello".to_owned(),
            RequestOptions::new()
                .header("caller", "example")
                .with_timeout(Duration::from_secs(1)),
        )
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    support::init_tracing();
    let vx = Vertx::builder()
        .event_loop_threads(2)
        .worker_threads(2)
        .build()?;
    let outcome = support::bounded(run(&vx)).await;
    support::finish(&vx, outcome).await
}
