//! Native Worker scheduling, parallelism, migration and destructor ownership.

use super::support::{WorkerDropProbe, capture_worker, vx, worker};
use rstest::rstest;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use vertx_rs::{ExecutionInfo, ExecutorThread, Result, Vertx, VertxError};

#[rstest]
#[tokio::test]
async fn worker_verticle_processes_messages(vx: Vertx) {
    vx.deploy_worker(|| {
        worker(|ctx| async move {
            ctx.event_bus()
                .unwrap()
                .consumer::<u64>("worker")?
                .handler_worker(|env| async move {
                    assert!(matches!(
                        ExecutionInfo::current().map(|info| info.thread),
                        Some(ExecutorThread::Worker)
                    ));
                    let n = *env.body() * 2;
                    env.reply(n)
                })?;
            Ok(())
        })
    })
    .await
    .unwrap();
    assert_eq!(
        vx.event_bus()
            .unwrap()
            .request::<_, u64>("worker", 21u64)
            .await
            .unwrap(),
        42
    );
    vx.close().await.unwrap();
}

#[tokio::test]
async fn worker_context_serializes_all_ordinary_executions() {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(4)
        .build()
        .unwrap();
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = oneshot::channel();
    let (a, m) = (active.clone(), maximum.clone());
    vx.deploy_worker(move || {
        worker(move |ctx| async move {
            let id = ctx.current_execution().unwrap().deployment_id;
            let mut handles = Vec::new();
            for _ in 0..32 {
                let (a, m) = (a.clone(), m.clone());
                handles.push(ctx.spawn(async move {
                    for _ in 0..5 {
                        let n = a.fetch_add(1, Ordering::SeqCst) + 1;
                        m.fetch_max(n, Ordering::SeqCst);
                        assert_eq!(ExecutionInfo::current().unwrap().deployment_id, id);
                        std::thread::sleep(Duration::from_micros(100));
                        a.fetch_sub(1, Ordering::SeqCst);
                        tokio::task::yield_now().await;
                    }
                    Ok(())
                })?);
            }
            ctx.spawn(async move {
                for handle in handles {
                    handle.await?;
                }
                tx.send(()).ok();
                Ok(())
            })?;
            Ok(())
        })
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(3), rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(maximum.load(Ordering::SeqCst), 1);
    vx.close().await.unwrap();
}

#[tokio::test]
async fn worker_contexts_can_execute_in_parallel() {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(4)
        .build()
        .unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let mut jobs = Vec::new();
    for _ in 0..2 {
        let v = vx.clone();
        let b = barrier.clone();
        jobs.push(tokio::spawn(async move {
            v.deploy_worker(move || {
                worker(move |_| async move {
                    b.wait();
                    Ok(())
                })
            })
            .await
        }));
    }
    for job in jobs {
        tokio::time::timeout(Duration::from_secs(2), job)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    vx.close().await.unwrap();
}

#[tokio::test]
async fn single_worker_reuses_thread_while_future_waits_and_can_await_blocking() {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build()
        .unwrap();
    let (ctx, _) = capture_worker(&vx).await;
    let (entered, started) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    let id = Some(ctx.id());
    let first = ctx
        .spawn(async move {
            entered.send(std::thread::current().id()).ok();
            wait.await.unwrap();
            assert_eq!(ExecutionInfo::current().unwrap().deployment_id, id);
            Ok(())
        })
        .unwrap();
    let thread = started.await.unwrap();
    let c = vx.handle();
    let second = ctx
        .spawn(async move {
            assert_eq!(std::thread::current().id(), thread);
            assert_eq!(
                tokio::runtime::Handle::current().runtime_flavor(),
                tokio::runtime::RuntimeFlavor::CurrentThread
            );
            let result = c
                .execute_blocking(move || {
                    assert_eq!(std::thread::current().id(), thread);
                    assert!(matches!(
                        ExecutionInfo::current().map(|info| info.thread),
                        Some(ExecutorThread::Worker)
                    ));
                    assert_eq!(ExecutionInfo::current().unwrap().deployment_id, id);
                    42
                })
                .await?;
            assert_eq!(result, 42);
            assert_eq!(ExecutionInfo::current().unwrap().deployment_id, id);
            release.send(()).ok();
            Ok(())
        })
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        first.await?;
        second.await
    })
    .await
    .unwrap()
    .unwrap();
    vx.close().await.unwrap();
}

#[tokio::test]
async fn worker_and_blocking_use_exactly_the_same_fixed_thread_set() {
    use std::collections::HashSet;
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(2)
        .build()
        .unwrap();
    let threads = Arc::new(Mutex::new(HashSet::new()));
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let mut deployments = Vec::new();
    for _ in 0..2 {
        let (v, threads, barrier) = (vx.clone(), threads.clone(), barrier.clone());
        deployments.push(tokio::spawn(async move {
            v.deploy_worker(move || {
                worker(move |_| async move {
                    threads.lock().unwrap().insert(std::thread::current().id());
                    barrier.wait();
                    Ok(())
                })
            })
            .await
        }));
    }
    for deployment in deployments {
        deployment.await.unwrap().unwrap();
    }
    let blocking_threads = Arc::new(Mutex::new(HashSet::new()));
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let jobs = (0..2).map(|_| {
        let (barrier, threads) = (barrier.clone(), blocking_threads.clone());
        vx.execute_blocking(move || {
            threads.lock().unwrap().insert(std::thread::current().id());
            barrier.wait();
        })
    });
    for result in futures::future::join_all(jobs).await {
        result.unwrap();
    }
    assert_eq!(threads.lock().unwrap().len(), 2);
    assert_eq!(*threads.lock().unwrap(), *blocking_threads.lock().unwrap());
    vx.close().await.unwrap();
}

#[tokio::test]
async fn worker_can_use_eventloop_io_and_timer_drivers_without_changing_executor() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build()
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut byte = [0];
        socket.read_exact(&mut byte).await.unwrap();
        socket.write_all(&[byte[0] + 1]).await.unwrap();
    });
    vx.deploy_worker(move || {
        worker(move |ctx| async move {
            let id = ctx.current_execution().unwrap().deployment_id;
            let thread = std::thread::current().id();
            let mut socket = tokio::time::timeout(
                Duration::from_secs(2),
                tokio::net::TcpStream::connect(address),
            )
            .await
            .unwrap()
            .unwrap();
            socket.write_all(&[41]).await.unwrap();
            let mut byte = [0];
            socket.read_exact(&mut byte).await.unwrap();
            assert_eq!(byte, [42]);
            ctx.timer(Duration::from_millis(1))?.await?;
            let mut periodic = ctx.periodic(Duration::ZERO, Duration::from_millis(1))?;
            periodic.tick().await?;
            assert_eq!(ExecutionInfo::current().unwrap().deployment_id, id);
            assert_eq!(std::thread::current().id(), thread);
            assert!(matches!(
                ExecutionInfo::current().map(|info| info.thread),
                Some(ExecutorThread::Worker)
            ));
            Ok(())
        })
    })
    .await
    .unwrap();
    server.await.unwrap();
    vx.close().await.unwrap();
}

#[tokio::test]
async fn worker_abort_before_first_poll_drops_on_owner_and_finishes_after_drop() {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build()
        .unwrap();
    let (ctx, _) = capture_worker(&vx).await;
    let (entered, started) = oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let c = vx.handle();
    let blocker = tokio::spawn(async move {
        c.execute_blocking(move || {
            entered.send(()).ok();
            wait.recv().unwrap();
        })
        .await
    });
    started.await.unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let probe = WorkerDropProbe {
        id: ctx.id(),
        dropped: dropped.clone(),
        panic: false,
    };
    let task = ctx
        .spawn(async move {
            let _probe = probe;
            panic!("cancelled future was polled");
            #[allow(unreachable_code)]
            Ok(())
        })
        .unwrap();
    let abort = task.abort_handle();
    task.abort();
    assert!(!abort.is_finished());
    release.send(()).unwrap();
    assert!(matches!(task.await, Err(VertxError::TaskCancelled)));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(abort.is_finished());
    blocker.await.unwrap().unwrap();
    vx.close().await.unwrap();
    abort.abort(); // Retaining a cancellation handle must not retain executor threads.
}

#[tokio::test]
async fn worker_destructor_panic_does_not_kill_thread_or_leak_context() {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build()
        .unwrap();
    let (ctx, _) = capture_worker(&vx).await;
    let dropped = Arc::new(AtomicUsize::new(0));
    let probe = WorkerDropProbe {
        id: ctx.id(),
        dropped: dropped.clone(),
        panic: true,
    };
    let (entered, started) = oneshot::channel();
    let task = ctx
        .spawn(async move {
            let _probe = probe;
            entered.send(()).ok();
            std::future::pending::<Result<()>>().await
        })
        .unwrap();
    started.await.unwrap();
    task.abort();
    assert!(matches!(task.await, Err(VertxError::Panic(_))));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    ctx.spawn(async {
        assert!(ExecutionInfo::current().is_some());
        Ok(())
    })
    .unwrap()
    .await
    .unwrap();
    assert!(ExecutionInfo::current().is_none());
    vx.close().await.unwrap();
}

#[tokio::test]
async fn future_combinations_preserve_worker_context_and_keep_spawned_tasks_independent() {
    use futures::{FutureExt, future};
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build()
        .unwrap();
    let (ctx, _) = capture_worker(&vx).await;
    let owner = ctx.clone();
    ctx.spawn(async move {
        let id = ExecutionInfo::current().unwrap().deployment_id;
        let futures = (0..3u8).map(|n| async move {
            tokio::task::yield_now().await;
            assert_eq!(ExecutionInfo::current().unwrap().deployment_id, id);
            Ok::<_, VertxError>(n)
        });
        assert_eq!(future::try_join_all(futures).await?, vec![0, 1, 2]);
        let outcomes = future::join_all(vec![
            future::ready(Ok::<_, VertxError>(1)),
            future::ready(Err(VertxError::Other("expected".into()))),
        ])
        .await;
        assert!(outcomes[0].is_ok());
        assert!(outcomes[1].is_err());
        let (value, remaining) = future::select_ok(vec![
            future::ready(Err::<u8, _>(VertxError::Other("first failed".into()))).boxed(),
            async move {
                tokio::task::yield_now().await;
                assert_eq!(ExecutionInfo::current().unwrap().deployment_id, id);
                Ok(42u8)
            }
            .boxed(),
        ])
        .await?;
        assert_eq!(value, 42);
        drop(remaining);
        assert!(
            future::select_ok(vec![future::ready(Err::<u8, _>(VertxError::Other(
                "all failed".into()
            )))])
            .await
            .is_err()
        );

        // Fail-fast drops a handle, but the submitted task remains managed.
        let (entered, started) = oneshot::channel();
        let (release, wait) = oneshot::channel();
        let (done, finished) = oneshot::channel();
        let child = owner.spawn(async move {
            entered.send(()).ok();
            wait.await.unwrap();
            done.send(()).ok();
            Ok(())
        })?;
        let abort = child.abort_handle();
        started.await.unwrap();
        let result = future::try_join(
            future::ready(Err::<(), _>(VertxError::Other("fail fast".into()))),
            child,
        )
        .await;
        assert!(result.is_err());
        assert!(!abort.is_finished());
        release.send(()).ok();
        finished.await.unwrap();
        assert_eq!(ExecutionInfo::current().unwrap().deployment_id, id);
        Ok(())
    })
    .unwrap()
    .await
    .unwrap();
    vx.close().await.unwrap();
}

#[tokio::test]
async fn worker_cancellation_destructor_serializes_with_ordinary_polls() {
    struct DropGate {
        entered: Option<oneshot::Sender<()>>,
        release: std::sync::mpsc::Receiver<()>,
        active: Arc<AtomicUsize>,
        id: vertx_rs::DeploymentId,
    }
    impl Drop for DropGate {
        fn drop(&mut self) {
            assert_eq!(
                ExecutionInfo::current().unwrap().deployment_id,
                Some(self.id)
            );
            self.active.store(1, Ordering::SeqCst);
            self.entered.take().unwrap().send(()).ok();
            self.release.recv().unwrap();
            self.active.store(0, Ordering::SeqCst);
        }
    }
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(2)
        .build()
        .unwrap();
    let (ctx, _) = capture_worker(&vx).await;
    let (other, _) = capture_worker(&vx).await;
    let active = Arc::new(AtomicUsize::new(0));
    let (entered, dropping) = oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let gate = DropGate {
        entered: Some(entered),
        release: wait,
        active: active.clone(),
        id: ctx.id(),
    };
    let (entered, running) = oneshot::channel();
    let task = ctx
        .spawn(async move {
            let _gate = gate;
            entered.send(()).ok();
            std::future::pending::<Result<()>>().await
        })
        .unwrap();
    running.await.unwrap();
    task.abort();
    dropping.await.unwrap();
    let same = active.clone();
    let peer = ctx
        .spawn(async move {
            assert_eq!(same.load(Ordering::SeqCst), 0);
            Ok(())
        })
        .unwrap();
    let independent = other
        .spawn(async move {
            assert_eq!(active.load(Ordering::SeqCst), 1);
            Ok(())
        })
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), independent).await;
    release.send(()).unwrap();
    result.unwrap().unwrap();
    assert!(matches!(task.await, Err(VertxError::TaskCancelled)));
    peer.await.unwrap();
    vx.close().await.unwrap();
}
