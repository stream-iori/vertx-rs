//! Two Verticles communicating through their own deployment-bound EventBus
//! handles, showing the owned-self / `run(self, ctx)` shape.
//!
//! Run with: `cargo run --example verticle_to_verticle`

use async_trait::async_trait;
use vertx_rs::{Consumer, Context, EventBus, Result, Verticle, Vertx};

/// Provider: turns a string upper-case on request.
struct UpperV {
    consumer: Consumer<String>,
}

#[async_trait(?Send)]
impl Verticle for UpperV {
    async fn run(mut self, ctx: Context) -> Result<()> {
        let cancel = ctx.cancellation_token();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                msg = self.consumer.recv() => {
                    let Some(env) = msg else { return Ok(()) };
                    let body = env.body().clone();
                    let _ = env.reply(body.to_uppercase());
                }
            }
        }
    }
}

/// Caller: delegates to UpperV via the bound EventBus.
struct CallerV {
    consumer: Consumer<String>,
    bus: EventBus,
}

#[async_trait(?Send)]
impl Verticle for CallerV {
    async fn run(mut self, ctx: Context) -> Result<()> {
        let cancel = ctx.cancellation_token();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                msg = self.consumer.recv() => {
                    let Some(env) = msg else { return Ok(()) };
                    let body = env.body().clone();
                    let upper: String = self.bus
                        .request("upper", body)
                        .await?;
                    let _ = env.reply(upper);
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let vx = Vertx::new();
    vx.deploy(|ctx| UpperV {
        consumer: ctx.event_bus().consumer::<String>("upper").unwrap(),
    })
    .await?;
    vx.deploy(|ctx| CallerV {
        consumer: ctx.event_bus().consumer::<String>("caller").unwrap(),
        bus: ctx.event_bus(),
    })
    .await?;

    for word in ["hello", "vertx", "rust"] {
        let r: String = vx
            .event_bus()
            .request("caller", word.to_string())
            .await?;
        println!("{word} -> {r}");
    }
    Ok(())
}
