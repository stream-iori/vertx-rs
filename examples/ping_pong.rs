//! Request/reply with non-Send state and asynchronous start/stop.
use async_trait::async_trait;
use std::{cell::Cell, rc::Rc};
use vertx_rs::{Context, Result, Verticle, Vertx};
struct Pinger {
    count: Rc<Cell<u64>>,
}
#[async_trait(?Send)]
impl Verticle for Pinger {
    async fn start(&mut self, ctx: Context) -> Result<()> {
        let count = self.count.clone();
        ctx.event_bus()
            .consumer::<String>("ping")?
            .handler(move |env| {
                count.set(count.get() + 1);
                let reply = format!("pong#{}", count.get());
                async move { env.reply(reply) }
            })
    }
    async fn stop(&mut self, _ctx: Context) -> Result<()> {
        println!("stopped after {} messages", self.count.get());
        Ok(())
    }
}
#[tokio::main]
async fn main() -> Result<()> {
    let vx = Vertx::builder().event_loop_threads(2).build();
    vx.deploy(|_| Pinger {
        count: Rc::new(Cell::new(0)),
    })
    .await?;
    for _ in 0..3 {
        let reply: String = vx.event_bus().request("ping", "hello".to_owned()).await?;
        println!("{reply}");
    }
    vx.close().await
}
