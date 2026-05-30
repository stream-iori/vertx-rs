//! Ping-pong over the local EventBus, demonstrating an EventLoop-affine
//! Verticle with owned `!Send` state (`Cell<u64>` — no `Rc<RefCell>` needed).
//!
//! Run with: `cargo run --example ping_pong`

use std::cell::Cell;

use async_trait::async_trait;
use vertx_rs::{Consumer, Context, Result, Verticle, Vertx};

struct Pinger {
    consumer: Consumer<String>,
    count: Cell<u64>,
}

#[async_trait(?Send)]
impl Verticle for Pinger {
    async fn run(mut self, ctx: Context) -> Result<()> {
        let cancel = ctx.cancellation_token();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                msg = self.consumer.recv() => {
                    let Some(env) = msg else { return Ok(()) };
                    self.count.set(self.count.get() + 1);
                    println!("got #{}: {}", self.count.get(), env.body());
                    let _ = env.reply(format!("pong#{}", self.count.get()));
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let vx = Vertx::new();

    // factory runs synchronously on the target EventLoop, registers the
    // consumer, returns the owned verticle. After deploy() returns, the
    // consumer is registered and run() is taking over.
    vx.deploy(|ctx| Pinger {
        consumer: ctx.event_bus().consumer::<String>("ping").unwrap(),
        count: Cell::new(0),
    })
    .await?;

    let bus = vx.event_bus();
    for _ in 0..3 {
        let r: String = bus
            .request("ping", "hello".to_string())
            .await?;
        println!("reply: {r}");
    }
    Ok(())
}
