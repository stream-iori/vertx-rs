//! Source -> Doubler x 2 -> Worker Sink, using managed tasks and bounded queues.
use async_trait::async_trait;
use tokio::sync::oneshot;
use vertx_rs::{Context, Result, Verticle, Vertx, WorkerVerticle};
struct Source;
#[async_trait(?Send)]
impl Verticle for Source {
    async fn start(&mut self, ctx: Context) -> Result<()> {
        let bus = ctx.event_bus();
        ctx.spawn_local(async move {
            for value in 1..=5i64 {
                bus.send("stage1", value).await?;
            }
            Ok(())
        })?;
        Ok(())
    }
}
struct Doubler;
#[async_trait(?Send)]
impl Verticle for Doubler {
    async fn start(&mut self, ctx: Context) -> Result<()> {
        ctx.event_bus()
            .consumer::<i64>("stage1")?
            .handler(move |env| {
                let ctx = ctx.clone();
                async move {
                    let value = *env.body();
                    let doubled = ctx.execute_blocking(move || value * 2).await?;
                    ctx.event_bus().send("stage2", doubled).await
                }
            })
    }
}
struct Sink {
    done: Option<oneshot::Sender<Vec<i64>>>,
}
#[async_trait]
impl WorkerVerticle for Sink {
    async fn start(&mut self, ctx: Context) -> Result<()> {
        let done = self.done.take().unwrap();
        let mut consumer = ctx.event_bus().consumer::<i64>("stage2")?;
        ctx.spawn(async move {
            let mut results = Vec::new();
            while let Some(env) = consumer.recv().await {
                results.push(env.into_body());
                if results.len() == 5 {
                    let _ = done.send(results);
                    return Ok(());
                }
            }
            Ok(())
        })?;
        Ok(())
    }
}
#[tokio::main]
async fn main() -> Result<()> {
    let vx = Vertx::builder().event_loop_threads(2).build();
    let (tx, rx) = oneshot::channel();
    vx.deploy_worker(|_| Sink { done: Some(tx) }).await?;
    vx.deploy(|_| Doubler).await?;
    vx.deploy(|_| Doubler).await?;
    vx.deploy(|_| Source).await?;
    let mut results = rx.await.unwrap();
    results.sort();
    assert_eq!(results, vec![2, 4, 6, 8, 10]);
    println!("sink got: {results:?}");
    vx.close().await
}
