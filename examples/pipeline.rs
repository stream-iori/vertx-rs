//! Pipeline: Source -> Doubler×2 -> Sink, demonstrating `Context::execute_blocking`
//! mid-flow and a mix of `Verticle` (EventLoop) + `WorkerVerticle`.
//!
//! Run with: `cargo run --example pipeline`

use async_trait::async_trait;
use tokio::sync::oneshot;
use vertx_rs::{Consumer, Context, EventBus, Result, Verticle, Vertx, WorkerVerticle};

struct Source {
    items: Vec<i64>,
    bus: EventBus,
}

#[async_trait(?Send)]
impl Verticle for Source {
    async fn run(self, _ctx: Context) -> Result<()> {
        for v in self.items {
            self.bus.send("stage1", v).await?;
        }
        Ok(())
    }
}

struct Doubler {
    label: &'static str,
    consumer: Consumer<i64>,
    bus: EventBus,
}

#[async_trait(?Send)]
impl Verticle for Doubler {
    async fn run(mut self, ctx: Context) -> Result<()> {
        let cancel = ctx.cancellation_token();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                msg = self.consumer.recv() => {
                    let Some(env) = msg else { return Ok(()) };
                    let n = *env.body();
                    let doubled = ctx.execute_blocking(move || n * 2).await?;
                    println!("[{}] {} -> {}", self.label, n, doubled);
                    self.bus.send("stage2", doubled).await?;
                }
            }
        }
    }
}

struct Sink {
    expected: usize,
    done: oneshot::Sender<Vec<i64>>,
    consumer: Consumer<i64>,
}

#[async_trait]
impl WorkerVerticle for Sink {
    async fn run(mut self, _ctx: Context) -> Result<()> {
        let mut buf = Vec::with_capacity(self.expected);
        while let Some(env) = self.consumer.recv().await {
            buf.push(*env.body());
            if buf.len() == self.expected {
                let _ = self.done.send(buf);
                return Ok(());
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let vx = Vertx::new();

    let (tx, rx) = oneshot::channel();
    vx.deploy_worker(Sink {
        expected: 5,
        done: tx,
        consumer: vx.event_bus().consumer::<i64>("stage2").unwrap(),
    })
    .await?;
    vx.deploy(|ctx| Doubler {
        label: "A",
        consumer: ctx.event_bus().consumer::<i64>("stage1").unwrap(),
        bus: ctx.event_bus(),
    })
    .await?;
    vx.deploy(|ctx| Doubler {
        label: "B",
        consumer: ctx.event_bus().consumer::<i64>("stage1").unwrap(),
        bus: ctx.event_bus(),
    })
    .await?;
    vx.deploy(|ctx| Source {
        items: vec![1, 2, 3, 4, 5],
        bus: ctx.event_bus(),
    })
    .await?;

    let mut results = rx.await.unwrap();
    results.sort();
    println!("sink got: {results:?}");
    assert_eq!(results, vec![2, 4, 6, 8, 10]);
    Ok(())
}
