//! Two deployments communicating through their own Context-bound EventBus.
use async_trait::async_trait;
use futures::FutureExt;
use vertx_rs::{Context, Result, Verticle, Vertx};
struct Upper;
#[async_trait(?Send)]
impl Verticle for Upper {
    async fn start(&mut self, ctx: Context) -> Result<()> {
        ctx.event_bus()
            .consumer::<String>("upper")?
            .handler(|env| async move {
                let response = env.body().to_uppercase();
                env.reply(response)
            })
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
                // any: the first success wins, rather than the first completion.
                // Dropping remaining request futures does not retract queued messages.
                let candidates = ["unavailable-upper", "upper"].into_iter().map(|address| {
                    bus.request::<_, String>(address, env.body().clone())
                        .boxed()
                });
                let (response, remaining) = futures::future::select_ok(candidates).await?;
                drop(remaining);
                env.reply(response)
            }
        })
    }
}
#[tokio::main]
async fn main() -> Result<()> {
    let vx = Vertx::builder().event_loop_threads(2).build();
    vx.deploy(|_| Upper).await?;
    vx.deploy(|_| Caller).await?;
    for word in ["hello", "vertx", "rust"] {
        let response: String = vx.event_bus().request("caller", word.to_owned()).await?;
        println!("{word} -> {response}");
    }
    vx.close().await
}
