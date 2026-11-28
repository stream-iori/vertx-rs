//! Business errors, panic recovery, hooks and retained cleanup failures.

use super::support::{capture, in_test_subprocess, local, vx, worker};
use rstest::rstest;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use vertx_rs::{ExecutionInfo, Result, Verticle, Vertx, VertxError, VertxHandle};

struct OnDrop<F: FnOnce()>(Option<F>);

impl<F: FnOnce()> Drop for OnDrop<F> {
    fn drop(&mut self) {
        self.0.take().unwrap()();
    }
}

#[rstest]
#[tokio::test]
async fn exception_hook_update_after_stop_is_rejected_even_with_retained_consumer(vx: Vertx) {
    let (ready, registered) = oneshot::channel();
    let deployment = vx
        .deploy(move || {
            local(move |vertx| async move {
                let consumer = vertx.event_bus()?.consumer::<()>("retained-hook-owner")?;
                ready.send(consumer).ok().unwrap();
                Ok(())
            })
        })
        .await
        .unwrap();
    let consumer = registered.await.unwrap();
    deployment.stop().await.unwrap();

    assert!(matches!(
        deployment.set_exception_handler(|_| {}),
        Err(VertxError::DeploymentTerminated(id)) if id == deployment.id()
    ));
    drop(consumer);
    assert!(matches!(
        deployment.clear_exception_handler(),
        Err(VertxError::DeploymentTerminated(id)) if id == deployment.id()
    ));
    vx.close().await.unwrap();
}

#[tokio::test]
async fn exception_handler_destructor_can_reenter_its_slot() {
    if !in_test_subprocess("errors::exception_handler_destructor_can_reenter_its_slot") {
        return;
    }
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build()
        .unwrap();
    let (ctx, _) = capture(&vx).await;

    let owner = vx.clone();
    let global = OnDrop(Some(move || owner.clear_exception_handler()));
    vx.set_exception_handler(move |_| {
        std::hint::black_box(&global);
    });
    vx.set_exception_handler(|_| {});

    let handle = ctx.clone();
    let local = OnDrop(Some(move || handle.set_exception_handler(|_| {}).unwrap()));
    ctx.set_exception_handler(move |_| {
        std::hint::black_box(&local);
    })
    .unwrap();
    ctx.clear_exception_handler().unwrap();
    vx.close().await.unwrap();
}

#[rstest]
#[case::context(false)]
#[case::global(true)]
#[tokio::test]
async fn exception_handler_destructor_panic_is_a_cleanup_error(vx: Vertx, #[case] global: bool) {
    let (ctx, _) = capture(&vx).await;
    let probe = OnDrop(Some(|| panic!("hook destructor")));
    let hook = move |_: &VertxError| {
        std::hint::black_box(&probe);
    };
    if global {
        vx.set_exception_handler(hook);
        for _ in 0..2 {
            assert!(matches!(vx.close().await,
                Err(VertxError::CleanupFailed(errors))
                    if matches!(errors.as_slice(), [VertxError::Panic(message)] if message == "hook destructor")
            ));
        }
        let snapshot = vx.diagnostics();
        assert_eq!(snapshot.close_stage, "failed");
        assert_eq!(snapshot.active_tasks, 0);
        assert!(snapshot.threads.is_empty());
    } else {
        ctx.set_exception_handler(hook).unwrap();
        assert!(
            matches!(ctx.stop().await, Err(VertxError::Panic(message)) if message == "hook destructor")
        );
        assert_eq!(vx.diagnostics().active_deployments, 0);
        // The next deployment exercises the instance after the isolated panic.
        let _ = capture(&vx).await;
        vx.close().await.unwrap();
    }
}

#[rstest]
#[tokio::test]
async fn exception_handler_receives_handler_errors(vx: Vertx) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    vx.set_exception_handler(move |e| {
        tx.send(e.to_string()).ok();
    });
    vx.deploy(|| {
        local(|ctx| async move {
            ctx.event_bus()
                .unwrap()
                .consumer::<()>("error")?
                .handler(async |_| Err(VertxError::Other("expected".into())))?;
            Ok(())
        })
    })
    .await
    .unwrap();
    vx.event_bus().unwrap().send("error", ()).await.unwrap();
    assert!(rx.recv().await.unwrap().contains("expected"));
    vx.close().await.unwrap();
}

#[rstest]
#[case::event_loop_error(false, false)]
#[case::event_loop_panic(false, true)]
#[case::worker_error(true, false)]
#[case::worker_panic(true, true)]
#[tokio::test]
async fn task_error_and_handler_panic_preserve_context_and_subscription(
    vx: Vertx,
    #[case] worker_context: bool,
    #[case] panic: bool,
) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    vx.set_exception_handler(move |error| {
        tx.send((
            ExecutionInfo::current().unwrap().deployment_id,
            error.to_string(),
        ))
        .ok();
    });
    let handler = move |env: vertx_rs::Envelope<bool>| async move {
        if *env.body() {
            assert!(!panic, "test panic");
            return Err(VertxError::Other("expected handler failure".into()));
        }
        env.reply(42u32)
    };
    let id = if worker_context {
        vx.deploy_worker(move || {
            worker(move |ctx| async move {
                ctx.event_bus()
                    .unwrap()
                    .consumer::<bool>("panic")?
                    .handler_worker(handler)?;
                Ok(())
            })
        })
        .await
        .unwrap()
    } else {
        vx.deploy(move || {
            local(move |ctx| async move {
                ctx.event_bus()
                    .unwrap()
                    .consumer::<bool>("panic")?
                    .handler(handler)?;
                Ok(())
            })
        })
        .await
        .unwrap()
    };
    assert!(matches!(
        vx.event_bus()
            .unwrap()
            .request::<_, u32>("panic", true)
            .await,
        Err(VertxError::ReplyClosed)
    ));
    let (owner, message) = rx.recv().await.unwrap();
    assert_eq!(owner, Some(id.id()));
    assert!(message.contains(if panic {
        "test panic"
    } else {
        "expected handler failure"
    }));
    assert_eq!(
        vx.event_bus()
            .unwrap()
            .request::<_, u32>("panic", false)
            .await
            .unwrap(),
        42
    );
    // A successful subsequent response follows error reporting in this serial loop.
    assert!(
        matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ),
        "each failed message must report exactly once"
    );
    id.stop().await.unwrap();
    let snapshot = vx.diagnostics();
    assert_eq!(snapshot.active_deployments, 0);
    assert!(
        snapshot
            .deployments
            .iter()
            .all(|deployment| deployment.deployment_id != id.id())
    );
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn context_exception_handler_can_clear_itself_without_deadlock(vx: Vertx) {
    let global = Arc::new(AtomicUsize::new(0));
    let g = global.clone();
    vx.set_exception_handler(move |_| {
        g.fetch_add(1, Ordering::SeqCst);
    });
    let (tx, rx) = oneshot::channel();
    vx.deploy(move || {
        local(move |ctx| async move {
            let tx = Mutex::new(Some(tx));
            let deployment = ctx.current_deployment().unwrap();
            let own = deployment.clone();
            deployment.set_exception_handler(move |_| {
                own.clear_exception_handler().unwrap();
                if let Some(tx) = tx.lock().unwrap().take() {
                    tx.send(()).ok();
                }
            })?;
            ctx.event_bus()
                .unwrap()
                .consumer::<()>("fail")?
                .handler(async |_| Err(VertxError::Other("error".into())))?;
            Ok(())
        })
    })
    .await
    .unwrap();
    vx.event_bus().unwrap().send("fail", ()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(global.load(Ordering::SeqCst), 0);
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn lifecycle_keeps_start_stop_and_drop_errors_and_source(vx: Vertx) {
    struct Broken;
    impl Drop for Broken {
        fn drop(&mut self) {
            panic!("drop failure");
        }
    }
    impl Verticle for Broken {
        async fn start(&mut self, _: VertxHandle) -> Result<()> {
            Err(VertxError::StartFailed("start failure".into()))
        }
        async fn stop(&mut self, _: VertxHandle) -> Result<()> {
            Err(VertxError::Other("stop failure".into()))
        }
    }
    let error = vx.deploy(|| Broken).await.unwrap_err();
    assert!(
        std::error::Error::source(&error)
            .unwrap()
            .to_string()
            .contains("start failure")
    );
    let VertxError::LifecycleFailed { cause, cleanup } = error else {
        panic!("missing lifecycle errors")
    };
    assert!(matches!(*cause, VertxError::StartFailed(_)));
    assert!(
        matches!(&cleanup[..], [VertxError::Other(s), VertxError::Panic(p)] if s == "stop failure" && p == "drop failure")
    );
    vx.close().await.unwrap();
}
