//! Two deployments communicating through their own Context-bound EventBus.
use async_trait::async_trait;
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
                let response: String = bus.request("upper", env.body().clone()).await?;
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
