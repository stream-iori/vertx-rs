//! main → Caller → Upper / Reject；通过真实服务展示三种 Future 组合。
//!
//! 运行：RUST_LOG=vertx_rs=debug cargo run --example verticle_to_verticle
//! LOG_FORMAT=json 输出 JSON；预期：HELLO、VERTX、RUST，以及 join_all 的业务失败。
//! try_join_all 遇错提前返回，join_all 保留每个结果，select_ok 等第一个成功。
//! 组合器丢弃普通 Future 会停止继续 poll；不会撤回已入队消息。
//! 实际演示丢弃 TaskHandle 后继续运行、abort 后等待析构、undeploy 后路由移除。
//! 未获选服务的迟到回复可能返回 ReplyClosed；服务显式处理这个预期结果。
mod support;
use futures::FutureExt;
use futures::future::{join_all, select_ok, try_join_all};
use std::time::Duration;
use tokio::sync::oneshot;
use vertx_rs::{Context, RequestOptions, Result, Verticle, Vertx, VertxError};
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
impl Verticle for Reject {
    async fn start(&mut self, ctx: Context) -> Result<()> {
        ctx.event_bus()
            .consumer::<String>("reject")?
            .handler(|env| async move { allow_late_reply(env.fail(503, "service unavailable")) })
    }
}
struct Caller {
    context: Option<oneshot::Sender<Context>>,
}
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
                        RequestOptions::new().with_timeout(Duration::from_secs(1)),
                    )
                    .boxed()
                });
                let (response, remaining) = select_ok(candidates).await?;
                drop(remaining);
                env.reply(response)
            }
        })?;
        self.context
            .take()
            .expect("start is called once")
            .send(ctx)
            .map_err(|_| VertxError::ReplyClosed)?;
        Ok(())
    }
}

// 用 channel 控制任务进度，避免靠 sleep 猜测任务是否启动或析构。
struct DropNotice(Option<oneshot::Sender<()>>);
impl Drop for DropNotice {
    fn drop(&mut self) {
        if let Some(done) = self.0.take() {
            let _ = done.send(());
        }
    }
}
async fn task_lifecycle(ctx: &Context) -> Result<()> {
    let (release, wait) = oneshot::channel();
    let (done, finished) = oneshot::channel();
    let id = ctx.id();
    let detached = ctx.spawn(async move {
        wait.await.map_err(|_| VertxError::ReplyClosed)?;
        assert_eq!(Context::current().unwrap().id(), id);
        done.send(()).map_err(|_| VertxError::ReplyClosed)?;
        Ok(())
    })?;
    drop(detached);
    release.send(()).map_err(|_| VertxError::ReplyClosed)?;
    finished.await.map_err(|_| VertxError::ReplyClosed)?;
    println!("detached task still completed");

    let (entered, started) = oneshot::channel();
    let (dropped, destroyed) = oneshot::channel();
    let task = ctx.spawn(async move {
        let _notice = DropNotice(Some(dropped));
        entered.send(()).map_err(|_| VertxError::ReplyClosed)?;
        std::future::pending::<Result<()>>().await
    })?;
    started.await.map_err(|_| VertxError::ReplyClosed)?;
    task.abort();
    match task.await {
        Err(VertxError::TaskCancelled) => {}
        result => {
            return Err(VertxError::Other(format!(
                "expected cancellation, got {result:?}"
            )));
        }
    }
    destroyed.await.map_err(|_| VertxError::ReplyClosed)?;
    println!("aborted task destroyed before completion was observed");
    Ok(())
}
async fn run(vx: &Vertx) -> Result<()> {
    vx.deploy(|_| Upper).await?;
    vx.deploy(|_| Reject).await?;
    let (ready, context) = oneshot::channel();
    let caller = vx
        .deploy(|_| Caller {
            context: Some(ready),
        })
        .await?;
    let ctx = context.await.map_err(|_| VertxError::ReplyClosed)?;
    task_lifecycle(&ctx).await?;
    // 单个任务结束或取消都不会撤销 Caller，下面仍能处理业务请求。
    let bus = vx.event_bus();
    let responses = try_join_all(["hello", "vertx", "rust"].into_iter().map(|word| {
        bus.request_with::<_, String>(
            "caller",
            word.to_owned(),
            RequestOptions::new().with_timeout(Duration::from_secs(1)),
        )
    }))
    .await?;
    assert_eq!(responses, ["HELLO", "VERTX", "RUST"]);
    println!("all successful: {responses:?}");
    let outcomes = join_all(["upper", "reject"].into_iter().map(|address| {
        bus.request_with::<_, String>(
            address,
            "example".to_owned(),
            RequestOptions::new().with_timeout(Duration::from_secs(1)),
        )
    }))
    .await;
    assert!(matches!(
        &outcomes[1],
        Err(VertxError::RecipientFailure { code: 503, .. })
    ));
    println!("all finished: {outcomes:?}");
    vx.undeploy(caller).await?;
    let after_stop: Result<String> = bus.request("caller", "after stop".to_owned()).await;
    assert!(matches!(after_stop, Err(VertxError::NoConsumer(_))));
    println!("caller undeployed; its route was removed");
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
