//! Business errors, panic recovery, hooks and retained cleanup failures.

use super::support::{local, vx, worker};
use async_trait::async_trait;
use rstest::rstest;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use vertx_rs::{Context, Result, Verticle, Vertx, VertxError};

#[rstest]
#[tokio::test]
async fn exception_handler_receives_handler_errors(vx: Vertx) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    vx.set_exception_handler(move |e| {
        tx.send(e.to_string()).ok();
    });
    vx.deploy(|_| {
        local(|ctx| async move {
            ctx.event_bus()
                .consumer::<()>("error")?
                .handler(|_| async { Err(VertxError::Other("expected".into())) })
        })
    })
    .await
    .unwrap();
    vx.event_bus().send("error", ()).await.unwrap();
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
            Context::current().unwrap().deployment_id(),
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
        vx.deploy_worker(move |_| {
            worker(move |ctx| async move {
                ctx.event_bus()
                    .consumer::<bool>("panic")?
                    .handler_worker(handler)
            })
        })
        .await
        .unwrap()
    } else {
        vx.deploy(move |_| {
            local(move |ctx| async move {
                ctx.event_bus().consumer::<bool>("panic")?.handler(handler)
            })
        })
        .await
        .unwrap()
    };
    assert!(matches!(
        vx.event_bus().request::<_, u32>("panic", true).await,
        Err(VertxError::ReplyClosed)
    ));
    let (owner, message) = rx.recv().await.unwrap();
    assert_eq!(owner, Some(id));
    assert!(message.contains(if panic {
        "test panic"
    } else {
        "expected handler failure"
    }));
    assert_eq!(
        vx.event_bus()
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
    vx.undeploy(id).await.unwrap();
    let snapshot = vx.diagnostics();
    assert_eq!(snapshot.active_deployments, 0);
    assert!(
        snapshot
            .deployments
            .iter()
            .all(|deployment| deployment.deployment_id != id)
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
    vx.deploy(move |_| {
        local(move |ctx| async move {
            let tx = Mutex::new(Some(tx));
            let own = ctx.clone();
            ctx.set_exception_handler(move |_| {
                own.clear_exception_handler();
                if let Some(tx) = tx.lock().unwrap().take() {
                    tx.send(()).ok();
                }
            });
            ctx.event_bus()
                .consumer::<()>("fail")?
                .handler(|_| async { Err(VertxError::Other("error".into())) })
        })
    })
    .await
    .unwrap();
    vx.event_bus().send("fail", ()).await.unwrap();
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
    #[async_trait(?Send)]
    impl Verticle for Broken {
        async fn start(&mut self, _: Context) -> Result<()> {
            Err(VertxError::StartFailed("start failure".into()))
        }
        async fn stop(&mut self, _: Context) -> Result<()> {
            Err(VertxError::Other("stop failure".into()))
        }
    }
    let error = vx.deploy(|_| Broken).await.unwrap_err();
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
