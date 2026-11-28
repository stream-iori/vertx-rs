//! handler 拆出 TypedReplyHandle 并交给受管任务，稍后附带响应 headers 回复。
//! cargo run --example deferred_reply
//! handler 本身仍然串行；并发来自应用主动 spawn，输入固定为三个请求。
mod support;
use vertx_rs::{
    DeliveryOptions, RequestAddress, RequestOptions, Result, Verticle, Vertx, VertxError,
    VertxHandle,
};

const TRANSFORM: RequestAddress<String, String> = RequestAddress::new("transform");

fn transform(body: &str) -> String {
    let hash = body.bytes().cycle().take(100_000).fold(0u64, |hash, byte| {
        hash.rotate_left(5).wrapping_add(u64::from(byte))
    });
    format!("{}:{hash:016x}", body.to_uppercase())
}
struct Transformer;
impl Verticle for Transformer {
    async fn start(&mut self, vertx: VertxHandle) -> Result<()> {
        TRANSFORM
            .consumer(&vertx.event_bus()?)?
            .handler(async move |env| {
                let (body, headers, reply) = env.into_parts();
                // Headers/reply 不会自动转发，应用明确选择需要返回的关联字段。
                let correlation = headers
                    .get("correlation-id")
                    .map(str::to_owned)
                    .ok_or_else(|| VertxError::Other("missing correlation-id".into()))?;
                let task_vertx = vertx.clone();
                vertx.spawn_local(async move {
                    let response = task_vertx
                        .execute_blocking_unordered(move || transform(&body))
                        .await?;
                    reply.reply_with(
                        response,
                        DeliveryOptions::new().header("correlation-id", correlation),
                    )
                })?;
                Ok(())
            })?;
        Ok(())
    }
}
async fn run(vx: &Vertx) -> Result<()> {
    vx.deploy(|| Transformer).await?;
    let bus = vx.event_bus()?;
    let requests = ["alpha", "beta", "gamma"]
        .into_iter()
        .enumerate()
        .map(|(id, word)| {
            let bus = &bus;
            async move {
                let correlation = format!("job-{id}");
                let response = TRANSFORM
                    .request_message_with(
                        bus,
                        word.to_owned(),
                        RequestOptions::new().header("correlation-id", correlation.clone()),
                    )
                    .await?;
                assert_eq!(response.body(), &transform(word));
                assert_eq!(
                    response.headers().get("correlation-id"),
                    Some(correlation.as_str())
                );
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
        .build()?;
    let outcome = support::bounded(run(&vx)).await;
    support::finish(&vx, outcome).await
}
