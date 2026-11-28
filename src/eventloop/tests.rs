use super::task::LocalTaskControl;
use super::{EventLoopId, EventLoopPool};
use crate::{Result, TaskHandle, TaskId, VertxError};
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::{oneshot, watch};

struct ExitProbe(Arc<AtomicUsize>);
impl Drop for ExitProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
thread_local! {
    static EXIT_PROBE: RefCell<Option<ExitProbe>> = const { RefCell::new(None) };
}
struct OwnerDrop {
    thread: std::thread::ThreadId,
    dropped: Arc<AtomicBool>,
}
impl Drop for OwnerDrop {
    fn drop(&mut self) {
        assert_eq!(std::thread::current().id(), self.thread);
        assert!(crate::context::Context::is_on_event_loop_thread());
        self.dropped.store(true, Ordering::Release);
    }
}
async fn finish(pool: &EventLoopPool) {
    let (sender, mut receiver) = watch::channel(None);
    pool.finish_shutdown(Vec::new(), sender);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(result) = receiver.borrow().clone() {
                result.unwrap();
                return;
            }
            receiver.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
}
async fn occupy(pool: &EventLoopPool) -> (std::sync::mpsc::Sender<()>, TaskHandle<()>) {
    let (entered, started) = oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let task = pool
        .spawn_pinned(EventLoopId(0), TaskId::next(), move || async move {
            entered.send(()).ok();
            wait.recv().unwrap();
            Ok(())
        })
        .unwrap();
    started.await.unwrap();
    (release, task)
}
#[derive(Clone, Copy)]
enum PanicStage {
    Factory,
    Poll,
    Drop,
}

mod dispatch {
    //! Deferred dispatch and local Future ownership.
    use super::*;

    #[tokio::test]
    async fn same_thread_dispatch_is_deferred_and_supports_local_futures() {
        let pool = Arc::new(EventLoopPool::new(1, 2003).unwrap());
        let target = pool.clone();
        pool.spawn_pinned(EventLoopId(0), TaskId::next(), move || async move {
            let local = Rc::new(Cell::new(0));
            let invoked = Arc::new(AtomicBool::new(false));
            let flag = invoked.clone();
            let thread = std::thread::current().id();
            let task = target.spawn_pinned(EventLoopId(0), TaskId::next(), move || {
                flag.store(true, Ordering::Release);
                let local = Rc::new(Cell::new(0));
                async move {
                    tokio::task::yield_now().await;
                    assert_eq!(std::thread::current().id(), thread);
                    local.set(1);
                    Ok(())
                }
            })?;
            assert!(!invoked.load(Ordering::Acquire));
            local.set(1);
            task.await?;
            assert!(invoked.load(Ordering::Acquire));
            assert_eq!(local.get(), 1);
            Ok(())
        })
        .unwrap()
        .await
        .unwrap();
        finish(&pool).await;
    }
}

mod cancellation {
    //! Abort before/after binding and destructor thread affinity.
    use super::*;

    #[tokio::test]
    async fn queued_abort_drops_before_first_poll_on_the_owning_thread() {
        let pool = EventLoopPool::new(1, 2001).unwrap();
        let (release, occupied) = occupy(&pool).await;
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = OwnerDrop {
            thread: pool.loops[0].thread,
            dropped: dropped.clone(),
        };
        let task = pool
            .spawn_pinned(EventLoopId(0), TaskId::next(), move || {
                let local = Rc::new(Cell::new(0));
                async move {
                    let _guard = guard;
                    local.set(1);
                    panic!("cancelled future was polled");
                    #[allow(unreachable_code)]
                    Ok(())
                }
            })
            .unwrap();
        let abort = task.abort_handle();
        abort.abort();
        assert!(!abort.is_finished());
        assert!(!dropped.load(Ordering::Acquire));
        release.send(()).unwrap();
        assert!(matches!(task.await, Err(VertxError::TaskCancelled)));
        assert!(abort.is_finished());
        assert!(dropped.load(Ordering::Acquire));
        occupied.await.unwrap();
        finish(&pool).await;
        abort.abort();
    }

    #[tokio::test]
    async fn running_cross_thread_task_can_be_aborted_and_drops_on_owner() {
        let pool = EventLoopPool::new(1, 2002).unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = OwnerDrop {
            thread: pool.loops[0].thread,
            dropped: dropped.clone(),
        };
        let (entered, started) = oneshot::channel();
        let task = pool
            .spawn_pinned(EventLoopId(0), TaskId::next(), move || async move {
                let _guard = guard;
                entered.send(()).ok();
                std::future::pending::<Result<()>>().await
            })
            .unwrap();
        started.await.unwrap();
        task.abort();
        assert!(matches!(task.await, Err(VertxError::TaskCancelled)));
        assert!(dropped.load(Ordering::Acquire));
        finish(&pool).await;
    }

    #[tokio::test]
    async fn early_abort_is_forwarded_when_local_abort_handle_is_bound() {
        let control = LocalTaskControl::default();
        // The cancellation bridge must also handle abort-before-bind.
        let task = tokio::spawn(std::future::pending::<()>());
        control.abort();
        control.bind(task.abort_handle());
        assert!(task.await.unwrap_err().is_cancelled());
    }
}

mod shutdown {
    //! Joining, partial startup and admission/shutdown races.
    use super::*;

    #[tokio::test]
    async fn shutdown_joins_threads_despite_retained_handles_drivers_and_wakers() {
        let pool = EventLoopPool::new(1, 2004).unwrap();
        let exited = Arc::new(AtomicUsize::new(0));
        let probe = exited.clone();
        let driver = pool.driver(EventLoopId(0));
        let (entered, started) = oneshot::channel();
        let task = pool
            .spawn_pinned(EventLoopId(0), TaskId::next(), move || {
                EXIT_PROBE.with(|slot| *slot.borrow_mut() = Some(ExitProbe(probe)));
                let mut entered = Some(entered);
                futures::future::poll_fn(move |cx| {
                    if let Some(entered) = entered.take() {
                        entered.send(cx.waker().clone()).ok();
                    }
                    Poll::<Result<()>>::Pending
                })
            })
            .unwrap();
        let wake = started.await.unwrap();
        let abort = task.abort_handle();
        finish(&pool).await;
        assert_eq!(exited.load(Ordering::SeqCst), 1);
        assert!(abort.is_finished());
        assert!(matches!(task.await, Err(VertxError::TaskCancelled)));
        assert!(matches!(
            pool.spawn_pinned(EventLoopId(0), TaskId::next(), || async { Ok(()) }),
            Err(VertxError::VertxClosed)
        ));
        wake.wake_by_ref();
        abort.abort();
        drop(driver);
    }

    #[tokio::test]
    async fn shutdown_cancels_accepted_commands_on_the_receiver_thread() {
        let pool = EventLoopPool::new(1, 2005).unwrap();
        let (release, occupied) = occupy(&pool).await;
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = OwnerDrop {
            thread: pool.loops[0].thread,
            dropped: dropped.clone(),
        };
        let task = pool
            .spawn_pinned(EventLoopId(0), TaskId::next(), move || async move {
                let _guard = guard;
                panic!("shutdown command was polled");
                #[allow(unreachable_code)]
                Ok(())
            })
            .unwrap();
        let (done, mut result) = watch::channel(None);
        pool.finish_shutdown(Vec::new(), done);
        release.send(()).unwrap();
        occupied.await.unwrap();
        result.changed().await.unwrap();
        result.borrow().as_ref().unwrap().as_ref().unwrap();
        assert!(matches!(task.await, Err(VertxError::TaskCancelled)));
        assert!(dropped.load(Ordering::Acquire));
    }

    #[rstest::rstest]
    #[case::first_thread(0)]
    #[case::middle_thread(1)]
    #[case::last_thread(2)]
    fn partial_initialization_failure_joins_all_started_threads(#[case] fail_at: usize) {
        let exited = Arc::new(AtomicUsize::new(0));
        let probe = exited.clone();
        let result = EventLoopPool::with_runtime_factory(3, 2006, move |idx| {
            EXIT_PROBE.with(|slot| *slot.borrow_mut() = Some(ExitProbe(probe.clone())));
            if idx == fail_at {
                Err(std::io::Error::other("injected startup failure"))
            } else {
                Builder::new_current_thread().enable_all().build()
            }
        });
        assert!(matches!(result, Err(VertxError::Other(message))
            if message.contains("event loops") && message.contains("injected startup failure")));
        assert_eq!(exited.load(Ordering::SeqCst), fail_at + 1);
    }

    #[test]
    fn initialization_rollback_retains_cause_and_join_panic() {
        let pool = EventLoopPool {
            loops: Vec::new(),
            handles: Mutex::new(vec![std::thread::spawn(|| panic!("injected join failure"))]),
            instance: 2010,
            rr: AtomicUsize::new(0),
        };
        let error = pool.rollback_initialization(VertxError::Other("startup failure".into()));
        let VertxError::LifecycleFailed { cause, cleanup } = &error else {
            panic!("expected startup cause and rollback failure: {error}");
        };
        assert!(
            matches!(cause.as_ref(), VertxError::Other(message) if message == "startup failure")
        );
        assert!(
            matches!(cleanup.as_slice(), [VertxError::Panic(message)] if message.contains("injected join failure"))
        );
        assert!(std::error::Error::source(&error).is_some());
        assert!(pool.handles.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn submissions_racing_with_shutdown_always_finish_or_are_rejected() {
        let pool = Arc::new(EventLoopPool::new(2, 2008).unwrap());
        let barrier = Arc::new(std::sync::Barrier::new(5));
        let mut submitters = Vec::new();
        for n in 0..4 {
            let (target, start) = (pool.clone(), barrier.clone());
            submitters.push(std::thread::spawn(move || {
                let mut tasks = vec![
                    target
                        .spawn_pinned(EventLoopId(n % 2), TaskId::next(), || async { Ok(()) })
                        .unwrap(),
                ];
                start.wait();
                for _ in 0..64 {
                    match target
                        .spawn_pinned(EventLoopId(n % 2), TaskId::next(), || async { Ok(()) })
                    {
                        Ok(task) => tasks.push(task),
                        Err(VertxError::VertxClosed) => {}
                        Err(error) => panic!("unexpected admission error: {error}"),
                    }
                }
                tasks
            }));
        }
        barrier.wait();
        let (done, mut result) = watch::channel(None);
        pool.finish_shutdown(Vec::new(), done);
        let mut tasks = Vec::new();
        for submitter in submitters {
            tasks.extend(submitter.join().unwrap());
        }
        if result.borrow().is_none() {
            result.changed().await.unwrap();
        }
        result.borrow().as_ref().unwrap().as_ref().unwrap();
        assert!(tasks.len() >= 4);
        for task in tasks {
            assert!(task.is_finished());
            assert!(matches!(
                task.await,
                Ok(()) | Err(VertxError::TaskCancelled)
            ));
        }
    }
}

mod panic_recovery {
    //! Independent factory, poll and destructor failures.
    use super::*;

    #[rstest::rstest]
    #[case::factory(PanicStage::Factory)]
    #[case::future_poll(PanicStage::Poll)]
    #[case::future_drop(PanicStage::Drop)]
    #[tokio::test]
    async fn factory_future_and_destructor_panics_do_not_kill_dispatcher(
        #[case] stage: PanicStage,
    ) {
        struct PanicDrop;
        impl Drop for PanicDrop {
            fn drop(&mut self) {
                panic!("local destructor panic");
            }
        }
        let pool = EventLoopPool::new(1, 2007).unwrap();
        let task = match stage {
            PanicStage::Factory => pool
                .spawn_pinned(
                    EventLoopId(0),
                    TaskId::next(),
                    || -> std::future::Ready<Result<()>> { panic!("factory panic") },
                )
                .unwrap(),
            PanicStage::Poll => pool
                .spawn_pinned(EventLoopId(0), TaskId::next(), || async {
                    panic!("future panic");
                    #[allow(unreachable_code)]
                    Ok(())
                })
                .unwrap(),
            PanicStage::Drop => {
                let (entered, started) = oneshot::channel();
                let task = pool
                    .spawn_pinned(EventLoopId(0), TaskId::next(), move || async move {
                        let _guard = PanicDrop;
                        entered.send(()).ok();
                        std::future::pending::<Result<()>>().await
                    })
                    .unwrap();
                started.await.unwrap();
                task.abort();
                task
            }
        };
        assert!(matches!(task.await, Err(VertxError::Panic(_))));
        pool.spawn_pinned(EventLoopId(0), TaskId::next(), || async { Ok(()) })
            .unwrap()
            .await
            .unwrap();
        finish(&pool).await;
    }
}
