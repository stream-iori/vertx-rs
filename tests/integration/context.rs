//! Logical context isolation, physical thread affinity and managed dispatch.

use super::support::{capture, local, vx, worker};
use rstest::rstest;
use std::cell::Cell;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use vertx_rs::{
    ExecutionInfo, ExecutorThread, Result, ThreadingModel, Verticle, Vertx, VertxError, VertxHandle,
};

#[rstest]
#[tokio::test]
async fn raw_tokio_task_can_dispatch_to_its_own_event_loop(vx: Vertx) {
    let (ctx, execution) = capture(&vx).await;
    let target = ctx.clone();
    ctx.spawn(async move {
        let thread = std::thread::current().id();
        let id = execution.context_id;
        tokio::spawn(async move {
            assert!(ExecutionInfo::current().is_none());
            assert_eq!(std::thread::current().id(), thread);
            target
                .spawn(async move {
                    assert_eq!(ExecutionInfo::current().unwrap().context_id, id);
                    assert_eq!(std::thread::current().id(), thread);
                    Ok(())
                })?
                .await
        })
        .await
        .unwrap()
    })
    .unwrap()
    .await
    .unwrap();
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn verticle_pinned_to_single_thread(vx: Vertx) {
    struct Affinity {
        samples: Arc<Mutex<Vec<std::thread::ThreadId>>>,
        non_send: Rc<Cell<u64>>,
    }
    impl Drop for Affinity {
        fn drop(&mut self) {
            self.samples
                .lock()
                .unwrap()
                .push(std::thread::current().id());
            assert!(ExecutionInfo::current().is_some());
        }
    }
    impl Verticle for Affinity {
        async fn start(&mut self, ctx: VertxHandle) -> Result<()> {
            self.samples
                .lock()
                .unwrap()
                .push(std::thread::current().id());
            let vertx = ctx.clone();
            let (samples, count) = (self.samples.clone(), self.non_send.clone());
            ctx.event_bus()
                .unwrap()
                .consumer::<()>("affinity")?
                .handler(async move |env| {
                    samples.lock().unwrap().push(std::thread::current().id());
                    tokio::task::yield_now().await;
                    vertx.execute_blocking(|| 42).await?;
                    samples.lock().unwrap().push(std::thread::current().id());
                    count.set(count.get() + 1);
                    env.reply(count.get())
                })?;
            Ok(())
        }
        async fn stop(&mut self, _: VertxHandle) -> Result<()> {
            tokio::task::yield_now().await;
            self.samples
                .lock()
                .unwrap()
                .push(std::thread::current().id());
            Ok(())
        }
    }
    let samples = Arc::new(Mutex::new(Vec::new()));
    let s = samples.clone();
    let id = vx
        .deploy(move || {
            s.lock().unwrap().push(std::thread::current().id());
            Affinity {
                samples: s,
                non_send: Rc::new(Cell::new(0)),
            }
        })
        .await
        .unwrap();
    for i in 1..=20 {
        assert_eq!(
            vx.event_bus()
                .unwrap()
                .request::<_, u64>("affinity", ())
                .await
                .unwrap(),
            i
        );
    }
    id.stop().await.unwrap();
    {
        let s = samples.lock().unwrap();
        assert!(s.len() >= 44);
        assert!(s.iter().all(|t| *t == s[0]));
    }
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn verticle_can_hold_non_send_state_directly(vx: Vertx) {
    vx.deploy(|| {
        local(|ctx| async move {
            let count = Cell::new(0u64);
            ctx.event_bus()
                .unwrap()
                .consumer::<()>("inc")?
                .handler(async move |env| {
                    count.set(count.get() + 1);
                    let n = count.get();
                    env.reply(n)
                })?;
            Ok(())
        })
    })
    .await
    .unwrap();
    for n in 1..=3 {
        assert_eq!(
            vx.event_bus()
                .unwrap()
                .request::<_, u64>("inc", ())
                .await
                .unwrap(),
            n
        );
    }
    vx.close().await.unwrap();
}

#[tokio::test]
async fn context_current_is_none_outside_vertx() {
    assert!(ExecutionInfo::current().is_none());
}

#[rstest]
#[tokio::test]
async fn context_introspection_event_loop_full(vx: Vertx) {
    let (deployment, info) = capture(&vx).await;
    assert_eq!(info.threading_model, ThreadingModel::EventLoop);
    assert_eq!(info.deployment_id, Some(deployment.id()));
    assert_eq!(
        info.thread,
        ExecutorThread::EventLoop(info.event_loop_id.unwrap())
    );
    assert!(vx.current_execution().is_none());
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn context_introspection_worker_full(vx: Vertx) {
    vx.deploy_worker(|| {
        worker(|ctx| async move {
            let current = ctx.current_execution().unwrap();
            assert_eq!(current.threading_model, ThreadingModel::Worker);
            assert!(current.event_loop_id.is_none());
            assert!(matches!(
                ExecutionInfo::current().map(|info| info.thread),
                Some(ExecutorThread::Worker)
            ));
            let id = ctx.current_execution().unwrap().deployment_id;
            tokio::task::yield_now().await;
            assert_eq!(ExecutionInfo::current().unwrap().deployment_id, id);
            Ok(())
        })
    })
    .await
    .unwrap();
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn external_thread_can_spawn_on_an_existing_event_loop(vx: Vertx) {
    let (ctx, _) = capture(&vx).await;
    let (tx, rx) = oneshot::channel();
    let id = Some(ctx.id());
    std::thread::spawn(move || {
        ctx.spawn(async move {
            assert!(matches!(
                ExecutionInfo::current().map(|info| info.thread),
                Some(ExecutorThread::EventLoop(_))
            ));
            tx.send(ExecutionInfo::current().unwrap().deployment_id)
                .ok();
            Ok(())
        })
        .unwrap()
    })
    .join()
    .unwrap();
    assert_eq!(rx.await.unwrap(), id);
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn spawn_after_undeploy_returns_terminated(vx: Vertx) {
    let (ctx, _) = capture(&vx).await;
    ctx.stop().await.unwrap();
    assert!(matches!(
        ctx.spawn(async { Ok(()) }),
        Err(VertxError::DeploymentTerminated(_))
    ));
    vx.close().await.unwrap();
}

#[tokio::test]
async fn two_contexts_on_one_event_loop_never_leak_identity() {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(2)
        .build()
        .unwrap();
    for address in ["a", "b"] {
        vx.deploy(move || {
            local(move |ctx| async move {
                let id = ctx.current_deployment().unwrap().id();
                ctx.event_bus()
                    .unwrap()
                    .consumer::<()>(address)?
                    .handler(async move |env| {
                        for _ in 0..100 {
                            tokio::task::yield_now().await;
                            assert_eq!(ExecutionInfo::current().unwrap().deployment_id, Some(id));
                        }
                        env.reply(id)
                    })?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }
    let bus = vx.event_bus().unwrap();
    let (a, b) = tokio::join!(
        bus.request::<_, vertx_rs::DeploymentId>("a", ()),
        bus.request::<_, vertx_rs::DeploymentId>("b", ())
    );
    assert_ne!(a.unwrap(), b.unwrap());
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn raw_tokio_spawn_has_no_logical_context(vx: Vertx) {
    vx.deploy(|| {
        local(|ctx| async move {
            let own = ctx.current_execution().unwrap().deployment_id;
            let thread = std::thread::current().id();
            tokio::spawn(async move {
                assert!(ExecutionInfo::current().is_none());
                assert_eq!(std::thread::current().id(), thread);
            })
            .await?;
            assert_eq!(ExecutionInfo::current().unwrap().deployment_id, own);
            Ok(())
        })
    })
    .await
    .unwrap();
    vx.close().await.unwrap();
}

#[tokio::test]
async fn local_spawn_and_consumer_binding_on_one_event_loop() {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(2)
        .build()
        .unwrap();
    let (first, _) = capture(&vx).await;
    let bus = vx.event_bus().unwrap();
    let consumer = first
        .spawn(async move { bus.consumer::<()>("first") })
        .unwrap()
        .await
        .unwrap();
    vx.deploy(move || {
        local(move |ctx| async move {
            assert!(matches!(
                consumer.handler(async |_| Ok(())),
                Err(VertxError::ContextMismatch)
            ));
            let current = ctx.current_execution().unwrap();
            let state = Rc::new(Cell::new(0));
            let result = ctx
                .spawn_local(async move {
                    tokio::task::yield_now().await;
                    assert_eq!(ExecutionInfo::current(), Some(current));
                    state.set(42);
                    Ok(state)
                })?
                .await?;
            assert_eq!(result.get(), 42);
            Ok(())
        })
    })
    .await
    .unwrap();
    assert!(matches!(
        vx.spawn_local(async { Ok(()) }),
        Err(VertxError::ContextMismatch)
    ));
    vx.close().await.unwrap();
}

#[tokio::test]
async fn spawn_local_rejects_another_vertx_instance() {
    let a = vx();
    let b = vx();
    let ctx = a.handle();
    b.deploy(move || {
        local(move |_| async move {
            assert!(matches!(
                ctx.spawn_local(async { Ok(()) }),
                Err(VertxError::ContextMismatch)
            ));
            Ok(())
        })
    })
    .await
    .unwrap();
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn same_eventloop_send_spawn_switches_context_without_running_inline() {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build()
        .unwrap();
    let (first, _) = capture(&vx).await;
    let (second, _) = capture(&vx).await;
    let first_id = Some(first.id());
    let second_id = Some(second.id());
    let ran = Arc::new(AtomicUsize::new(0));
    let observed = ran.clone();
    first
        .spawn(async move {
            let thread = std::thread::current().id();
            assert_eq!(ExecutionInfo::current().unwrap().deployment_id, first_id);
            let task = second.spawn(async move {
                assert_eq!(std::thread::current().id(), thread);
                assert_eq!(ExecutionInfo::current().unwrap().deployment_id, second_id);
                ran.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
                assert_eq!(ExecutionInfo::current().unwrap().deployment_id, second_id);
                Ok(())
            })?;
            assert_eq!(observed.load(Ordering::SeqCst), 0);
            assert_eq!(ExecutionInfo::current().unwrap().deployment_id, first_id);
            task.await?;
            assert_eq!(observed.load(Ordering::SeqCst), 1);
            assert_eq!(ExecutionInfo::current().unwrap().deployment_id, first_id);
            Ok(())
        })
        .unwrap()
        .await
        .unwrap();
    vx.close().await.unwrap();
}

#[tokio::test]
async fn queued_cross_thread_abort_preserves_target_context_during_destruction() {
    struct DropCheck {
        id: vertx_rs::DeploymentId,
        dropped: Arc<AtomicUsize>,
    }
    impl Drop for DropCheck {
        fn drop(&mut self) {
            assert!(matches!(
                ExecutionInfo::current().map(|info| info.thread),
                Some(ExecutorThread::EventLoop(_))
            ));
            assert_eq!(
                ExecutionInfo::current().unwrap().deployment_id,
                Some(self.id)
            );
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build()
        .unwrap();
    let (ctx, _) = capture(&vx).await;
    let (entered, started) = oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let blocker = ctx
        .spawn(async move {
            entered.send(()).ok();
            wait.recv().unwrap();
            Ok(())
        })
        .unwrap();
    started.await.unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let guard = DropCheck {
        id: ctx.id(),
        dropped: dropped.clone(),
    };
    let task = ctx
        .spawn(async move {
            let _guard = guard;
            panic!("queued cancelled task was polled");
            #[allow(unreachable_code)]
            Ok(())
        })
        .unwrap();
    let abort = task.abort_handle();
    abort.abort();
    assert!(!abort.is_finished());
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    release.send(()).unwrap();
    assert!(matches!(task.await, Err(VertxError::TaskCancelled)));
    assert!(abort.is_finished());
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    blocker.await.unwrap();
    assert!(ExecutionInfo::current().is_none());
    vx.close().await.unwrap();
}
