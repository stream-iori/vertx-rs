//! 批量部署第二个实例失败，等待失败实例及前序实例清理。
//! cargo run --example deployment_rollback
//! 预期出现启动失败日志，但正确回滚后程序退出成功。
mod support;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use vertx_rs::{Context, DeploymentOptions, Result, Verticle, Vertx, VertxError};

struct Config {
    fail_at: usize,
}
struct Instance {
    index: usize,
    fail: bool,
    stopped: Arc<Mutex<Vec<usize>>>,
}
impl Verticle for Instance {
    async fn start(&mut self, ctx: Context) -> Result<()> {
        ctx.event_bus()
            .consumer::<()>("batch")?
            .handler(async |_| Ok(()))?;
        if self.fail {
            return Err(VertxError::StartFailed(format!(
                "instance {} rejected configuration",
                self.index
            )));
        }
        Ok(())
    }
    async fn stop(&mut self, _: Context) -> Result<()> {
        self.stopped.lock().unwrap().push(self.index);
        Ok(())
    }
}
async fn run(vx: &Vertx) -> Result<()> {
    let created = Arc::new(AtomicUsize::new(0));
    let stopped = Arc::new(Mutex::new(Vec::new()));
    let (count, stops) = (created.clone(), stopped.clone());
    let result = vx
        .deploy_with(
            move |_, config| {
                let index = count.fetch_add(1, Ordering::SeqCst);
                Instance {
                    index,
                    fail: index == config.fail_at,
                    stopped: stops.clone(),
                }
            },
            DeploymentOptions::new()
                .instances(NonZeroUsize::new(3).unwrap())
                .config(Config { fail_at: 1 }),
        )
        .await;
    let error = result.expect_err("second instance must fail");
    let VertxError::DeploymentFailed { cause, rollback } = &error else {
        return Err(error);
    };
    assert!(
        matches!(cause.as_ref(), VertxError::StartFailed(message) if message.contains("instance 1"))
    );
    assert!(rollback.is_empty(), "rollback itself must succeed");
    assert!(std::error::Error::source(&error).is_some());
    assert_eq!(created.load(Ordering::SeqCst), 2);
    let mut stopped_indices = stopped.lock().unwrap().clone();
    stopped_indices.sort();
    assert_eq!(stopped_indices, [0, 1]);
    assert!(matches!(
        vx.event_bus().try_send("batch", ()),
        Err(VertxError::NoConsumer(_))
    ));
    println!("startup failed as expected; created=2, stopped=[0, 1], routes removed");
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
