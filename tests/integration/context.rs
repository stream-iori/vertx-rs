//! Logical Context isolation, physical thread affinity and managed dispatch.

use super::support::{capture, local, vx, worker};
use async_trait::async_trait;
use rstest::rstest;
use std::cell::Cell;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use vertx_rs::{Context, Result, ThreadingModel, Verticle, Vertx, VertxError};

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
            assert!(Context::current().is_some());
        }
    }
    #[async_trait(?Send)]
    impl Verticle for Affinity {
        async fn start(&mut self, ctx: Context) -> Result<()> {
            self.samples
                .lock()
                .unwrap()
                .push(std::thread::current().id());
            let (samples, count) = (self.samples.clone(), self.non_send.clone());
            ctx.event_bus()
                .consumer::<()>("affinity")?
                .handler(move |env| {
                    let (samples, count) = (samples.clone(), count.clone());
                    async move {
                        samples.lock().unwrap().push(std::thread::current().id());
                        tokio::task::yield_now().await;
                        let ctx = Context::current().unwrap();
                        ctx.execute_blocking(|| 42).await?;
                        samples.lock().unwrap().push(std::thread::current().id());
                        count.set(count.get() + 1);
                        env.reply(count.get())
                    }
                })
        }
        async fn stop(&mut self, _: Context) -> Result<()> {
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
        .deploy(move |_| {
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
                .request::<_, u64>("affinity", ())
                .await
                .unwrap(),
            i
        );
    }
    vx.undeploy(id).await.unwrap();
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
    vx.deploy(|_| {
        local(|ctx| async move {
            let count = Cell::new(0u64);
            ctx.event_bus().consumer::<()>("inc")?.handler(move |env| {
                count.set(count.get() + 1);
                let n = count.get();
                async move { env.reply(n) }
            })
        })
    })
    .await
    .unwrap();
    for n in 1..=3 {
        assert_eq!(
            vx.event_bus().request::<_, u64>("inc", ()).await.unwrap(),
            n
        );
    }
    vx.close().await.unwrap();
}

#[tokio::test]
async fn context_current_is_none_outside_vertx() {
    assert!(Context::current().is_none());
    assert!(!Context::is_on_vertx_thread());
}

#[rstest]
#[tokio::test]
async fn context_introspection_event_loop_full(vx: Vertx) {
    let (id, ctx) = capture(&vx).await;
    assert_eq!(ctx.threading_model(), ThreadingModel::EventLoop);
    assert_eq!(ctx.deployment_id(), Some(id));
    assert!(ctx.owner().is_some());
    assert!(ctx.event_loop_id().is_some());
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn context_introspection_worker_full(vx: Vertx) {
    vx.deploy_worker(|_| {
        worker(|ctx| async move {
            assert_eq!(ctx.threading_model(), ThreadingModel::Worker);
            assert!(ctx.event_loop_id().is_none());
            assert!(Context::is_on_worker_thread());
            let id = ctx.deployment_id();
            tokio::task::yield_now().await;
            assert_eq!(Context::current().unwrap().deployment_id(), id);
            Ok(())
        })
    })
    .await
    .unwrap();
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn external_thread_can_hop_into_event_loop_via_run_on_context(vx: Vertx) {
    let (_, ctx) = capture(&vx).await;
    let (tx, rx) = oneshot::channel();
    let id = ctx.deployment_id();
    std::thread::spawn(move || {
        ctx.run_on_context(async move {
            assert!(Context::is_on_event_loop_thread());
            tx.send(Context::current().unwrap().deployment_id()).ok();
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
async fn run_on_context_after_undeploy_returns_terminated(vx: Vertx) {
    let (id, ctx) = capture(&vx).await;
    vx.undeploy(id).await.unwrap();
    assert!(matches!(
        ctx.run_on_context(async { Ok(()) }),
        Err(VertxError::DeploymentTerminated(_))
    ));
    vx.close().await.unwrap();
}

#[tokio::test]
async fn two_contexts_on_one_event_loop_never_leak_identity() {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(2)
        .build();
    for address in ["a", "b"] {
        vx.deploy(move |_| {
            local(move |ctx| async move {
                let id = ctx.deployment_id();
                ctx.event_bus()
                    .consumer::<()>(address)?
                    .handler(move |env| async move {
                        for _ in 0..100 {
                            tokio::task::yield_now().await;
                            assert_eq!(Context::current().unwrap().deployment_id(), id);
                        }
                        env.reply(id)
                    })
            })
        })
        .await
        .unwrap();
    }
    let bus = vx.event_bus();
    let (a, b) = tokio::join!(
        bus.request::<_, Option<vertx_rs::DeploymentId>>("a", ()),
        bus.request::<_, Option<vertx_rs::DeploymentId>>("b", ())
    );
    assert_ne!(a.unwrap(), b.unwrap());
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn raw_tokio_spawn_has_no_logical_context(vx: Vertx) {
    vx.deploy(|_| {
        local(|ctx| async move {
            let own = ctx.deployment_id();
            tokio::spawn(async {
                assert!(Context::current().is_none());
                assert!(Context::is_on_event_loop_thread());
            })
            .await?;
            assert_eq!(Context::current().unwrap().deployment_id(), own);
            Ok(())
        })
    })
    .await
    .unwrap();
    vx.close().await.unwrap();
}

#[tokio::test]
async fn spawn_local_rejects_other_context_even_on_same_thread() {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(2)
        .build();
    let (_, a) = capture(&vx).await;
    let (tx, rx) = oneshot::channel();
    let other = a.clone();
    vx.deploy(move |_| {
        local(move |ctx| async move {
            assert!(matches!(
                other.spawn_local(async { Ok(()) }),
                Err(VertxError::ContextMismatch)
            ));
            assert!(matches!(
                other
                    .event_bus()
                    .consumer::<()>("wrong")?
                    .handler(|_| async { Ok(()) }),
                Err(VertxError::ContextMismatch)
            ));
            let id = ctx.deployment_id();
            ctx.spawn_local(async move {
                tokio::task::yield_now().await;
                assert_eq!(Context::current().unwrap().deployment_id(), id);
                tx.send(()).ok();
                Ok(())
            })?;
            Ok(())
        })
    })
    .await
    .unwrap();
    rx.await.unwrap();
    assert!(matches!(
        a.spawn_local(async { Ok(()) }),
        Err(VertxError::ContextMismatch)
    ));
    vx.close().await.unwrap();
}

#[tokio::test]
async fn spawn_local_rejects_another_vertx_instance() {
    let a = vx();
    let b = vx();
    let (_, ctx) = capture(&a).await;
    b.deploy(move |_| {
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
        .build();
    let (_, first) = capture(&vx).await;
    let (_, second) = capture(&vx).await;
    let first_id = first.deployment_id();
    let second_id = second.deployment_id();
    let ran = Arc::new(AtomicUsize::new(0));
    let observed = ran.clone();
    first
        .spawn(async move {
            let thread = std::thread::current().id();
            assert_eq!(Context::current().unwrap().deployment_id(), first_id);
            let task = second.spawn(async move {
                assert_eq!(std::thread::current().id(), thread);
                assert_eq!(Context::current().unwrap().deployment_id(), second_id);
                ran.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
                assert_eq!(Context::current().unwrap().deployment_id(), second_id);
                Ok(())
            })?;
            assert_eq!(observed.load(Ordering::SeqCst), 0);
            assert_eq!(Context::current().unwrap().deployment_id(), first_id);
            task.await?;
            assert_eq!(observed.load(Ordering::SeqCst), 1);
            assert_eq!(Context::current().unwrap().deployment_id(), first_id);
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
        id: Option<vertx_rs::DeploymentId>,
        dropped: Arc<AtomicUsize>,
    }
    impl Drop for DropCheck {
        fn drop(&mut self) {
            assert!(Context::is_on_event_loop_thread());
            assert_eq!(Context::current().unwrap().deployment_id(), self.id);
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build();
    let (_, ctx) = capture(&vx).await;
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
        id: ctx.deployment_id(),
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
    assert!(Context::current().is_none());
    vx.close().await.unwrap();
}
