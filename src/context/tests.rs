use super::{Context, ContextId, TaskResult, ThreadIdentity, enter_thread, thread_identity};
use crate::deployment::DeploymentScope;
use crate::{DeploymentId, EventLoopId, Result, TaskId, Vertx, VertxError};
use std::future::Future;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use tokio_util::sync::CancellationToken;

#[rstest::rstest]
#[case::normal_return(false)]
#[case::unwind(true)]
fn thread_identity_is_restored_on_exit(#[case] unwind: bool) {
    assert!(thread_identity().is_none());
    let event_loop = ThreadIdentity::EventLoop(1, EventLoopId(0));
    let worker = ThreadIdentity::Worker(2);
    {
        let _event_loop = enter_thread(event_loop);
        let result = std::panic::catch_unwind(|| {
            let _worker = enter_thread(worker);
            assert!(thread_identity() == Some(worker));
            assert!(!unwind, "test unwind");
        });
        assert_eq!(result.is_err(), unwind);
        assert!(thread_identity() == Some(event_loop));
    }
    assert!(thread_identity().is_none());
}

#[tokio::test]
async fn context_restored_when_send_future_moves_between_os_threads() {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(2)
        .build()
        .unwrap();
    let scope = Arc::new(DeploymentScope::new(
        DeploymentId::next(),
        vx.inner.lifetime.cancel.child_token(),
    ));
    let ctx = Context::new_worker(&vx.inner, scope);
    let id = ctx.deployment_id();
    let mut polls = 0;
    let mut future = Box::pin(ctx.wrap(
        TaskId::next(),
        futures::future::poll_fn(move |_| {
            assert_eq!(Context::current().unwrap().deployment_id(), id);
            polls += 1;
            if polls == 2 {
                Poll::Ready(TaskResult::Finished(Ok(())))
            } else {
                Poll::Pending
            }
        }),
    ));
    let mut threads = Vec::new();
    for n in 0..2 {
        future = std::thread::spawn(move || {
            assert!(Context::current().is_none());
            let waker = futures::task::noop_waker();
            let mut cx = TaskContext::from_waker(&waker);
            assert_eq!(future.as_mut().poll(&mut cx).is_ready(), n == 1);
            assert!(Context::current().is_none());
            (future, std::thread::current().id())
        })
        .join()
        .map(|(f, t)| {
            threads.push(t);
            f
        })
        .unwrap();
    }
    assert_ne!(threads[0], threads[1]);
    drop(future);
    assert!(Context::current().is_none());
    vx.close().await.unwrap();
}
#[rstest::rstest]
#[case::normal_return(false)]
#[case::unwind(true)]
#[tokio::test]
async fn guard_restores_nested_context_and_panic(#[case] panic: bool) {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(2)
        .build()
        .unwrap();
    let make = || {
        Context::new_worker(
            &vx.inner,
            Arc::new(DeploymentScope::new(
                DeploymentId::next(),
                CancellationToken::new(),
            )),
        )
    };
    let (a, b) = (make(), make());
    {
        let _a = a.enter();
        assert!(Context::current().unwrap().same(&a));
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _b = b.enter();
            assert!(Context::current().unwrap().same(&b));
            if panic {
                panic!("guard test");
            }
        }));
        assert_eq!(r.is_err(), panic);
        assert!(Context::current().unwrap().same(&a));
    }
    assert!(Context::current().is_none());
    vx.close().await.unwrap();
}
#[rstest::rstest]
#[case::event_loop(false)]
#[case::worker(true)]
#[tokio::test]
async fn user_destructor_panic_releases_an_unpoisoned_execution_gate(#[case] worker: bool) {
    struct PanicDrop {
        context: ContextId,
        worker: bool,
    }
    impl Drop for PanicDrop {
        fn drop(&mut self) {
            assert_eq!(Context::current().unwrap().id(), self.context);
            assert_eq!(Context::is_on_worker_thread(), self.worker);
            panic!("injected user destructor panic");
        }
    }
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build()
        .unwrap();
    let scope = Arc::new(DeploymentScope::new(
        DeploymentId::next(),
        vx.inner.lifetime.cancel.child_token(),
    ));
    let ctx = if worker {
        Context::new_worker(&vx.inner, scope)
    } else {
        Context::new_event_loop(EventLoopId(0), &vx.inner, scope)
    };
    let (ready, started) = tokio::sync::oneshot::channel();
    let probe = PanicDrop {
        context: ctx.id(),
        worker,
    };
    let task = ctx
        .spawn(async move {
            let _probe = probe;
            ready.send(()).unwrap();
            std::future::pending::<Result<()>>().await
        })
        .unwrap();
    started.await.unwrap();
    task.abort();
    assert!(matches!(task.await, Err(VertxError::Panic(_))));
    if let super::Target::Worker {
        execution: gate, ..
    } = &ctx.data.target
    {
        assert!(
            !gate.is_poisoned(),
            "user panic must not poison the Worker execution gate"
        );
    }
    let id = ctx.id();
    ctx.spawn(async move {
        assert_eq!(Context::current().unwrap().id(), id);
        Ok(())
    })
    .unwrap()
    .await
    .unwrap();
    assert!(Context::current().is_none());
    vx.close().await.unwrap();
    assert_eq!(vx.diagnostics().active_tasks, 0);
}

#[tokio::test]
async fn registration_and_stop_share_one_admission_transaction() {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(2)
        .build()
        .unwrap();
    for _ in 0..50 {
        let scope = Arc::new(DeploymentScope::new(
            DeploymentId::next(),
            vx.inner.lifetime.cancel.child_token(),
        ));
        let ctx = Context::new_worker(&vx.inner, scope.clone());
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let c = ctx.clone();
        let b = barrier.clone();
        let register = std::thread::spawn(move || {
            b.wait();
            c.event_bus().consumer::<()>("race")
        });
        let c = ctx.clone();
        let b = barrier.clone();
        let stop = std::thread::spawn(move || {
            b.wait();
            c.scope().request_stop();
            c.scope().unregister_resources();
        });
        let result = register.join().unwrap();
        stop.join().unwrap();
        // Check the route while a successful registration still owns its receiver.
        assert!(matches!(
            vx.event_bus().try_send("race", ()),
            Err(crate::VertxError::NoConsumer(_))
        ));
        match result {
            Ok(mut consumer) => assert!(matches!(
                consumer.recv().await,
                Err(VertxError::DeploymentTerminated(_))
            )),
            Err(crate::VertxError::DeploymentTerminated(_)) => {}
            _ => panic!("unexpected registration outcome"),
        }
    }
    vx.close().await.unwrap();
}

#[tokio::test]
async fn rejected_dispatch_releases_its_task_registration() {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build()
        .unwrap();
    let scope = Arc::new(DeploymentScope::new(
        DeploymentId::next(),
        vx.inner.lifetime.cancel.child_token(),
    ));
    let ctx = Context::new_event_loop(EventLoopId(0), &vx.inner, scope.clone());
    let (completion, mut joined) = tokio::sync::watch::channel(None);
    vx.inner
        .executors
        .event_loops
        .finish_shutdown(Vec::new(), completion);
    joined.changed().await.unwrap();
    assert!(matches!(joined.borrow().as_ref(), Some(Ok(()))));
    assert!(matches!(
        ctx.spawn(async { Ok(()) }),
        Err(VertxError::VertxClosed)
    ));
    scope.request_stop();
    let drain = scope.drain_tasks(std::time::Duration::from_secs(1));
    tokio::pin!(drain);
    assert!(
        futures::poll!(&mut drain).is_ready(),
        "rejected dispatch left pending tracked work"
    );
    assert_eq!(vx.diagnostics().active_tasks, 0);
    assert!(vx.inner.executors.workers.shutdown().await.is_empty());
}
