//! 外部 OS 线程可以派发实例级任务，也可以指定部署。
//! cargo run --example external_dispatch
mod support;
use std::thread::{JoinHandle, ThreadId};
use tokio::sync::oneshot;
use vertx_rs::{ExecutionInfo, Result, Verticle, Vertx, VertxError, VertxHandle};

struct Service {
    ready: Option<oneshot::Sender<(ExecutionInfo, ThreadId)>>,
}
impl Verticle for Service {
    async fn start(&mut self, vertx: VertxHandle) -> Result<()> {
        self.ready
            .take()
            .expect("start called once")
            .send((
                vertx.current_execution().expect("managed start"),
                std::thread::current().id(),
            ))
            .map_err(|_| VertxError::ReplyClosed)
    }
}
async fn run(vx: &Vertx, external: &mut Option<JoinHandle<()>>) -> Result<()> {
    let (ready, context) = oneshot::channel();
    let deployment = vx.deploy(|| Service { ready: Some(ready) }).await?;
    let (execution, owner_thread) = context.await.map_err(|_| VertxError::ReplyClosed)?;
    let (submitted, tasks) = oneshot::channel();
    let target = deployment.clone();
    let instance = vx.handle();
    *external = Some(std::thread::spawn(move || {
        assert!(instance.current_execution().is_none());
        let instance_task = instance.spawn(async {
            let current = ExecutionInfo::current().expect("managed task");
            assert_eq!(current.deployment_id, None);
            Ok(current.context_id)
        });
        let deployment_task = target.spawn(async move {
            let current = ExecutionInfo::current().expect("managed task");
            assert_eq!(current, execution);
            assert_eq!(std::thread::current().id(), owner_thread);
            Ok(current.context_id)
        });
        let _ = submitted.send((instance_task, deployment_task));
    }));
    let (instance_task, deployment_task) = tasks.await.map_err(|_| VertxError::ReplyClosed)?;
    let (instance_id, deployment_context_id) = tokio::try_join!(instance_task?, deployment_task?)?;
    assert_ne!(instance_id, deployment_context_id);
    println!("instance context {instance_id}, deployment context {deployment_context_id}");
    deployment.stop().await?;
    assert!(matches!(
        deployment.spawn(async { Ok(()) }),
        Err(VertxError::DeploymentTerminated(_))
    ));
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
        .build()?;
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
