use super::WorkerPool;
use crate::VertxError;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};
use tokio::runtime::Handle;
use tokio::sync::oneshot;

mod scheduling {
    //! Wake coalescing and deterministic migration between OS threads.
    use super::*;

    #[tokio::test]
    async fn wakes_during_poll_are_coalesced_and_late_wakes_are_ignored() {
        let pool = WorkerPool::new(2, 1001).unwrap();
        let (send, receive) = oneshot::channel::<Waker>();
        let mut send = Some(send);
        let release = Arc::new(std::sync::Barrier::new(2));
        let gate = release.clone();
        let polls = Arc::new(AtomicUsize::new(0));
        let count = polls.clone();
        let task = pool
            .spawn(
                1,
                Handle::current(),
                futures::future::poll_fn(move |cx| {
                    if count.fetch_add(1, Ordering::SeqCst) == 0 {
                        send.take().unwrap().send(cx.waker().clone()).ok();
                        gate.wait();
                        Poll::Pending
                    } else {
                        Poll::Ready(Ok(()))
                    }
                }),
            )
            .unwrap();
        let wake = receive.await.unwrap();
        for _ in 0..1000 {
            wake.wake_by_ref();
        }
        release.wait();
        task.await.unwrap();
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        wake.wake_by_ref();
        pool.shutdown().await;
        assert!(pool.handles.lock().unwrap().is_empty());
        // A retained waker must not keep pool threads alive or enqueue new work.
        wake.wake_by_ref();
        assert!(pool.scheduler.ready_is_empty());
    }

    #[tokio::test]
    async fn a_pending_future_can_resume_on_a_different_os_thread() {
        let pool = WorkerPool::new(2, 1002).unwrap();
        let (send, receive) = oneshot::channel();
        let mut send = Some(send);
        let mut first = None;
        let task = pool
            .spawn(
                1,
                Handle::current(),
                futures::future::poll_fn(move |cx| {
                    let thread = std::thread::current().id();
                    match first {
                        None => {
                            first = Some(thread);
                            send.take().unwrap().send((thread, cx.waker().clone())).ok();
                            Poll::Pending
                        }
                        Some(first) => {
                            assert_ne!(thread, first);
                            Poll::Ready(Ok(()))
                        }
                    }
                }),
            )
            .unwrap();
        let (first, wake) = receive.await.unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let (release, wait) = std::sync::mpsc::channel();
        let wait = Arc::new(Mutex::new(wait));
        let (blocked, blocking) = oneshot::channel();
        let blocked = Arc::new(Mutex::new(Some(blocked)));
        let mut jobs = Vec::new();
        for _ in 0..2 {
            let (barrier, wait, blocked) = (barrier.clone(), wait.clone(), blocked.clone());
            jobs.push(
                pool.spawn_blocking(move || {
                    barrier.wait();
                    if std::thread::current().id() == first {
                        blocked.lock().unwrap().take().unwrap().send(()).ok();
                        wait.lock().unwrap().recv().unwrap();
                    }
                    Ok(())
                })
                .unwrap(),
            );
        }
        blocking.await.unwrap();
        wake.wake();
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), task).await;
        release.send(()).unwrap();
        result.unwrap().unwrap();
        for job in jobs {
            job.await.unwrap().unwrap();
        }
        pool.shutdown().await;
    }
}

mod shutdown {
    //! Join errors and abandoned result destructors.
    use super::*;

    #[tokio::test]
    async fn shutdown_retains_thread_join_failures() {
        let pool = WorkerPool::new(1, 1004).unwrap();
        pool.handles
            .lock()
            .unwrap()
            .push(std::thread::spawn(|| panic!("worker join failure")));
        let errors = pool.shutdown().await;
        assert!(matches!(&errors[..], [VertxError::Panic(s)] if s == "worker join failure"));
    }

    #[rstest::rstest]
    #[case::future_result(false)]
    #[case::blocking_result(true)]
    #[tokio::test]
    async fn abandoned_results_with_panicking_destructors_do_not_kill_workers(
        #[case] blocking: bool,
    ) {
        struct PanicDrop;
        impl Drop for PanicDrop {
            fn drop(&mut self) {
                panic!("abandoned result destructor");
            }
        }
        let pool = WorkerPool::new(1, 1003).unwrap();
        if blocking {
            let (release, wait) = std::sync::mpsc::channel();
            let result = pool
                .spawn_blocking(move || {
                    wait.recv().unwrap();
                    Ok(PanicDrop)
                })
                .unwrap();
            drop(result);
            release.send(()).unwrap();
        } else {
            let (release, wait) = oneshot::channel();
            let task = pool
                .spawn(1, Handle::current(), async move {
                    wait.await.unwrap();
                    Ok(PanicDrop)
                })
                .unwrap();
            drop(task);
            release.send(()).unwrap();
        }
        // One Worker and the same lane: completion follows the result's Drop.
        pool.spawn(1, Handle::current(), async { Ok(()) })
            .unwrap()
            .await
            .unwrap();
        pool.shutdown().await;
    }
}
