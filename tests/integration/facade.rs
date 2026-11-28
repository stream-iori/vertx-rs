use super::support::{capture, local, report_on_thread_exit, vx};
use rstest::rstest;
use std::time::Duration;
use tokio::sync::oneshot;
use vertx_rs::{DiagnosticsOptions, ExecutionInfo, ExecutorThread, Result, Vertx, VertxError};

#[rstest]
#[tokio::test]
async fn blocking_future_uses_the_scope_where_it_was_created(vx: Vertx) {
    let (deployment, execution) = capture(&vx).await;
    let external = vx.execute_blocking(|| ExecutionInfo::current().unwrap());
    let handle = vx.handle();
    let (root, from_deployment) = deployment
        .spawn(async move {
            let root = external.await?;
            let deferred = handle.execute_blocking(|| ExecutionInfo::current().unwrap());
            Ok((root, deferred))
        })
        .unwrap()
        .await
        .unwrap();

    assert_eq!(root.deployment_id, None);
    assert_eq!(root.thread, ExecutorThread::Worker);
    let blocking = from_deployment.await.unwrap();
    assert_eq!(blocking.context_id, execution.context_id);
    assert_eq!(blocking.deployment_id, Some(deployment.id()));
    assert_eq!(blocking.threading_model, execution.threading_model);
    assert_eq!(blocking.thread, ExecutorThread::Worker);
    deployment.stop().await.unwrap();
    vx.close().await.unwrap();
}

#[tokio::test]
async fn calls_to_another_instance_use_the_destination_lifetime() {
    let origin = vx();
    let target = vx();
    let handle = target.handle();
    let (deployment, _) = capture(&origin).await;
    let (timer, info) = deployment
        .spawn(async move {
            assert!(handle.current_execution().is_none());
            assert!(handle.current_deployment().is_none());
            let target = handle.clone();
            let received = handle
                .spawn(
                    async move { Ok(target.current_execution().expect("destination execution")) },
                )?
                .await?;
            Ok((handle.timer(Duration::from_secs(60))?, received))
        })
        .unwrap()
        .await
        .unwrap();

    assert!(matches!(info.thread, ExecutorThread::EventLoop(_)));
    assert_eq!(info.deployment_id, None);
    origin.close().await.unwrap();
    assert!(!timer.is_cancelled());
    target.close().await.unwrap();
    assert!(timer.is_cancelled());
}

#[tokio::test]
async fn close_drains_instance_tasks_and_consumers_independently_of_deployments() {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .diagnostics(DiagnosticsOptions {
            task_details: true,
            ..Default::default()
        })
        .build()
        .unwrap();
    let (deployment, _) = capture(&vx).await;
    let (ready, registered) = oneshot::channel();
    let handle = vx.handle();
    let task = vx
        .spawn(async move {
            let consumer = handle.event_bus()?.consumer::<u32>("instance")?;
            let info = handle.current_execution().unwrap();
            ready.send((consumer, info)).ok();
            std::future::pending::<Result<()>>().await
        })
        .unwrap();
    let (mut consumer, info) = registered.await.unwrap();
    assert_eq!(info.deployment_id, None);
    let snapshot = vx.diagnostics();
    assert!(
        snapshot
            .tasks
            .iter()
            .any(|record| record.task_id == task.id()
                && record.context_id == info.context_id
                && record.deployment_id.is_none())
    );

    deployment.stop().await.unwrap();
    vx.event_bus().unwrap().try_send("instance", 42u32).unwrap();
    assert_eq!(consumer.recv().await.unwrap().unwrap().into_body(), 42);
    vx.close().await.unwrap();
    assert!(matches!(task.await, Err(VertxError::TaskCancelled)));
    assert!(matches!(
        consumer.recv().await,
        Err(VertxError::VertxClosed)
    ));
    let snapshot = vx.diagnostics();
    assert_eq!(snapshot.active_tasks, 0);
    assert_eq!(snapshot.active_consumers, 0);
    assert!(snapshot.threads.is_empty());
}

#[tokio::test]
async fn retaining_weak_handles_does_not_keep_pending_runtime_tasks_alive() {
    struct Dropped(Option<oneshot::Sender<()>>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.take().unwrap().send(()).ok();
        }
    }

    let vx = vx();
    let (ready, started) = oneshot::channel();
    let (destroyed, observed) = oneshot::channel();
    let handle = vx.handle();
    let captured = handle.clone();
    let task = vx
        .spawn(async move {
            let _guard = Dropped(Some(destroyed));
            ready.send(()).ok();
            std::future::pending::<Result<()>>().await?;
            assert!(captured.current_execution().is_some());
            Ok(())
        })
        .unwrap();
    started.await.unwrap();

    let retained = vx.deploy(|| local(|_| async { Ok(()) })).await.unwrap();
    drop(vx);
    assert!(matches!(handle.event_bus(), Err(VertxError::VertxClosed)));
    tokio::time::timeout(Duration::from_secs(3), observed)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(task.await, Err(VertxError::TaskCancelled)));
    tokio::time::timeout(Duration::from_secs(3), retained.wait_stopped())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn retained_consumer_allows_event_loop_to_exit_after_last_owner_drops() {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build()
        .unwrap();
    let (report, exited) = oneshot::channel();
    let handle = vx.handle();
    let mut consumer = vx
        .spawn(async move {
            report_on_thread_exit(report);
            handle.event_bus()?.consumer::<()>("retained")
        })
        .unwrap()
        .await
        .unwrap();

    drop(vx);
    tokio::time::timeout(Duration::from_secs(3), exited)
        .await
        .expect("retained Consumer held the EventLoop alive")
        .unwrap();
    assert!(matches!(
        consumer.recv().await,
        Err(VertxError::VertxClosed)
    ));
    assert!(consumer.recv().await.unwrap().is_none());
}
