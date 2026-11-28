//! 业务拒绝、普通 Err、panic 的区别；deployment hook 优先于全局 hook。
//! cargo run --example error_handling
//! 预期会出现 panic hook 输出和错误日志，但消费者继续运行，程序最终退出成功。
mod support;
use tokio::sync::mpsc;
use vertx_rs::{ExecutionInfo, Result, Verticle, Vertx, VertxError, VertxHandle};

#[derive(Clone, Copy)]
enum Command {
    Reject,
    Error,
    Panic,
    Health,
}
struct Service;
impl Verticle for Service {
    async fn start(&mut self, ctx: VertxHandle) -> Result<()> {
        ctx.event_bus()?
            .consumer::<Command>("service")?
            .handler(async |env| match env.body() {
                Command::Reject => env.fail(409, "business rejected"),
                Command::Error => Err(VertxError::Other("handler error".into())),
                Command::Panic => panic!("expected handler panic"),
                Command::Health => env.reply("healthy".to_owned()),
            })?;
        Ok(())
    }
}
async fn run(vx: &Vertx) -> Result<()> {
    let (events, mut received) = mpsc::unbounded_channel();
    let global = events.clone();
    vx.set_exception_handler(move |error| {
        assert!(ExecutionInfo::current().is_some());
        let _ = global.send(("global", error.kind()));
    });
    let deployment = vx.deploy(|| Service).await?;
    deployment.set_exception_handler(move |error| {
        assert!(ExecutionInfo::current().is_some());
        let _ = events.send(("deployment", error.kind()));
    })?;
    let bus = vx.event_bus()?;
    let rejection: Result<String> = bus.request("service", Command::Reject).await;
    assert!(matches!(
        rejection,
        Err(VertxError::RecipientFailure { code: 409, .. })
    ));
    let healthy: String = bus.request("service", Command::Health).await?;
    assert_eq!(healthy, "healthy");
    assert!(matches!(
        received.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    println!("business rejection: RecipientFailure; no exception hook");

    let error: Result<String> = bus.request("service", Command::Error).await;
    assert!(matches!(error, Err(VertxError::ReplyClosed)));
    assert_eq!(received.recv().await, Some(("deployment", "other")));
    println!("handler error: deployment hook");
    deployment.clear_exception_handler()?;
    let panic: Result<String> = bus.request("service", Command::Panic).await;
    assert!(matches!(panic, Err(VertxError::ReplyClosed)));
    assert_eq!(received.recv().await, Some(("global", "panic")));
    println!("handler panic: global hook");
    let healthy: String = bus.request("service", Command::Health).await?;
    assert_eq!(healthy, "healthy");
    assert!(matches!(
        received.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    println!("consumer recovered; each exception reported once");
    Ok(())
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    support::init_tracing();
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(2)
        .build()?;
    let outcome = support::bounded(run(&vx)).await;
    support::finish(&vx, outcome).await
}
