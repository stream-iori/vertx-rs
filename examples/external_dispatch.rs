//! 外部 OS 线程提交任务到已有 Context；逻辑模型不描述提交方线程。
//! cargo run --example external_dispatch
mod support;
use async_trait::async_trait;
use std::thread::{JoinHandle, ThreadId};
use tokio::sync::oneshot;
use vertx_rs::{Context, Result, ThreadingModel, Verticle, Vertx, VertxError};

struct Service {
    ready: Option<oneshot::Sender<(Context, ThreadId)>>,
}
#[async_trait(?Send)]
impl Verticle for Service {
    async fn start(&mut self, ctx: Context) -> Result<()> {
        self.ready
            .take()
            .expect("start called once")
            .send((ctx, std::thread::current().id()))
            .map_err(|_| VertxError::ReplyClosed)
    }
}
async fn run(vx: &Vertx, external: &mut Option<JoinHandle<()>>) -> Result<()> {
    let (ready, context) = oneshot::channel();
    let deployment = vx.deploy(|_| Service { ready: Some(ready) }).await?;
    let (ctx, owner_thread) = context.await.map_err(|_| VertxError::ReplyClosed)?;
    let (submitted, task) = oneshot::channel();
    let target = ctx.clone();
    *external = Some(std::thread::spawn(move || {
        // 将提交阶段错误送回异步调用方；主线程最后仍会 join 并检查线程 panic。
        assert!(Context::current().is_none());
        assert!(!Context::is_on_vertx_thread());
        assert_eq!(target.threading_model(), ThreadingModel::EventLoop);
        let id = target.id();
        let task = target.run_on_context(async move {
            assert_eq!(Context::current().unwrap().id(), id);
            assert_eq!(std::thread::current().id(), owner_thread);
            assert!(Context::is_on_event_loop_thread());
            println!("external submission ran on {owner_thread:?} in {id}");
            Ok(())
        });
        let _ = submitted.send(task);
    }));
    let task = task.await.map_err(|_| VertxError::ReplyClosed)??;
    task.await?;
    assert!(Context::current().is_none());
    vx.undeploy(deployment).await?;
    assert!(matches!(
        ctx.run_on_context(async { Ok(()) }),
        Err(VertxError::DeploymentTerminated(_))
    ));
    println!("submission after undeploy: DeploymentTerminated");
    Ok(())
}
fn main() -> Result<()> {
    support::init_tracing();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| VertxError::Other(format!("caller runtime: {error}")))?;
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(2)
        .try_build()?;
    let mut external = None;
    let outcome = runtime.block_on(async {
        let outcome = support::bounded(run(&vx, &mut external)).await;
        support::finish(&vx, outcome).await
    });
    // 外部线程只提交任务，不等待任务完成；在异步 runtime 外回收它。
    let joined = external.map_or(Ok(()), |thread| {
        thread
            .join()
            .map_err(|_| VertxError::Join("external thread panicked".into()))
    });
    match (outcome, joined) {
        (Ok(()), joined) => joined,
        (Err(cause), Ok(())) => Err(cause),
        (Err(cause), Err(cleanup)) => Err(VertxError::LifecycleFailed {
            cause: Box::new(cause),
            cleanup: vec![cleanup],
        }),
    }
}
