//! handler 拆出 ReplyHandle 并交给受管任务，稍后附带响应 headers 回复。
//! cargo run --example deferred_reply
//! handler 本身仍然串行；并发来自应用主动 spawn，输入固定为三个请求。
mod support;
use async_trait::async_trait;
use vertx_rs::{Context, DeliveryOptions, Result, Verticle, Vertx, VertxError};

fn transform(body: &str) -> String {
    let hash = body.bytes().cycle().take(100_000).fold(0u64, |hash, byte| {
        hash.rotate_left(5).wrapping_add(u64::from(byte))
    });
    format!("{}:{hash:016x}", body.to_uppercase())
}
struct Transformer;
#[async_trait(?Send)]
impl Verticle for Transformer {
    async fn start(&mut self, ctx: Context) -> Result<()> {
        ctx.event_bus()
            .consumer::<String>("transform")?
            .handler(move |env| {
                let ctx = ctx.clone();
                async move {
                    let (body, headers, reply) = env.into_parts();
                    let reply = reply
                        .ok_or_else(|| VertxError::Other("transform requires a request".into()))?;
                    // Headers/reply 不会自动转发，应用明确选择需要返回的关联字段。
                    let correlation = headers
                        .get("correlation-id")
                        .cloned()
                        .ok_or_else(|| VertxError::Other("missing correlation-id".into()))?;
                    let task_ctx = ctx.clone();
                    ctx.spawn_local(async move {
                        let response = task_ctx
                            .execute_blocking_unordered(move || transform(&body))
                            .await?;
                        reply.reply_with(
                            response,
                            DeliveryOptions::new().header("correlation-id", correlation),
                        )
                    })?;
                    Ok(())
                }
            })
    }
}
async fn run(vx: &Vertx) -> Result<()> {
    vx.deploy(|_| Transformer).await?;
    let bus = vx.event_bus();
    let requests = ["alpha", "beta", "gamma"]
        .into_iter()
        .enumerate()
        .map(|(id, word)| {
            let bus = &bus;
            async move {
                let correlation = format!("job-{id}");
                let response = bus
                    .request_message_with::<_, String>(
                        "transform",
                        word.to_owned(),
                        DeliveryOptions::new().header("correlation-id", correlation.clone()),
                    )
                    .await?;
                assert_eq!(response.body(), &transform(word));
                assert_eq!(response.headers().get("correlation-id"), Some(&correlation));
                println!("{correlation}: {}", response.body());
                Ok::<_, VertxError>(())
            }
        });
    futures::future::try_join_all(requests).await?;
    Ok(())
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    support::init_tracing();
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(2)
        .try_build()?;
    let outcome = support::bounded(run(&vx)).await;
    support::finish(&vx, outcome).await
}
