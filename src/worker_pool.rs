use crate::context::{set_thread, ThreadIdentity};
use crate::error::panic_error;
use crate::TaskHandle;
use crate::{Result, VertxError};
use futures::task::{waker_ref, ArcWake};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::task::{Context, Poll};
use std::thread::JoinHandle;
use tokio::runtime::Handle;
use tokio::sync::{oneshot, Notify};

struct WorkerControl {
    task: Weak<Task>,
    finished: Arc<AtomicBool>,
}
impl crate::task::TaskControl for WorkerControl {
    fn abort(&self) {
        if let Some(task) = self.task.upgrade() {
            task.schedule(true);
        }
    }
    fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }
}

type Completion = Box<dyn FnOnce() + Send>;
trait Work: Send {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()>;
    // Drop user state before publishing completion, even if its destructor panics.
    fn finish(self: Box<Self>, error: Option<VertxError>) -> Completion;
}
struct FutureWork<F: Future<Output = Result<T>>, T> {
    future: Option<Pin<Box<F>>>,
    output: Option<Result<T>>,
    sender: oneshot::Sender<Result<T>>,
}
impl<F, T> Work for FutureWork<F, T>
where
    F: Future<Output = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        match self
            .future
            .as_mut()
            .expect("unfinished work owns its future")
            .as_mut()
            .poll(cx)
        {
            Poll::Pending => Poll::Pending,
            Poll::Ready(output) => {
                self.output = Some(output);
                Poll::Ready(())
            }
        }
    }
    fn finish(mut self: Box<Self>, error: Option<VertxError>) -> Completion {
        let dropped =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(self.future.take())));
        let output = match dropped {
            Err(error) => Err(panic_error(error)),
            Ok(()) => error.map_or_else(
                || self.output.take().expect("Ready work has an output"),
                Err,
            ),
        };
        Box::new(move || {
            let _ = self.sender.send(output);
        })
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum TaskPhase {
    Waiting,
    Queued,
    Running,
    Complete,
}
struct TaskState {
    work: Option<Box<dyn Work>>,
    phase: TaskPhase,
    notified: bool,
    cancelled: bool,
}
struct Task {
    id: u64,
    lane: u64,
    instance: u64,
    driver: Handle,
    pool: Weak<Core>,
    state: Mutex<TaskState>,
    finished: Arc<AtomicBool>,
}
impl Task {
    fn schedule(self: &Arc<Self>, cancel: bool) {
        let enqueue = {
            let mut state = self.state.lock().unwrap();
            if state.phase == TaskPhase::Complete {
                return;
            }
            state.cancelled |= cancel;
            if state.phase == TaskPhase::Running {
                state.notified = true;
                false
            } else if state.phase == TaskPhase::Queued {
                false
            } else {
                state.phase = TaskPhase::Queued;
                true
            }
        };
        if enqueue {
            if let Some(pool) = self.pool.upgrade() {
                pool.enqueue(self.clone());
            }
        }
    }
    fn run(self: &Arc<Self>, pool: &Core) {
        let (mut work, cancelled) = {
            let mut state = self.state.lock().unwrap();
            state.phase = TaskPhase::Running;
            state.notified = false;
            (
                state.work.take().expect("queued task owns unpolled work"),
                state.cancelled,
            )
        };
        assert!(crate::context::thread_identity() == Some(ThreadIdentity::Worker(self.instance)));
        let _runtime = self.driver.enter();
        let result = if cancelled {
            Ok(Poll::Pending)
        } else {
            let waker = waker_ref(self);
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                work.poll(&mut Context::from_waker(&waker))
            }))
        };
        // The Context lane remains owned until run() returns, including user
        // destruction. A concurrent wake may enqueue, but cannot poll it yet.
        let (finish, enqueue, error) = {
            let mut state = self.state.lock().unwrap();
            let ready = matches!(&result, Ok(Poll::Ready(())));
            let error = match result {
                Err(error) => Some(panic_error(error)),
                _ if state.cancelled => Some(VertxError::TaskCancelled),
                _ => None,
            };
            let finish = error.is_some() || ready;
            if finish {
                state.phase = TaskPhase::Complete;
            } else {
                state.phase = if state.notified {
                    TaskPhase::Queued
                } else {
                    TaskPhase::Waiting
                };
            }
            (finish, !finish && state.phase == TaskPhase::Queued, error)
        };
        if finish {
            let complete = work.finish(error);
            self.finished.store(true, Ordering::Release);
            pool.state.lock().unwrap().tasks.remove(&self.id);
            pool.available.notify_all();
            if let Err(error) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(complete)) {
                tracing::error!(error = %panic_error(error), "task result destructor panicked");
            }
        } else {
            self.state.lock().unwrap().work = Some(work);
            if enqueue {
                pool.enqueue(self.clone());
            }
        }
    }
}
impl ArcWake for Task {
    fn wake_by_ref(task: &Arc<Self>) {
        task.schedule(false);
    }
}

#[derive(Default)]
struct Lane {
    tasks: VecDeque<Arc<Task>>,
    scheduled: bool,
}
enum Dispatch {
    Context(u64),
    Blocking(Box<dyn FnOnce() + Send>),
}
struct PoolState {
    ready: VecDeque<Dispatch>,
    lanes: HashMap<u64, Lane>,
    tasks: HashMap<u64, Arc<Task>>,
    closing: bool,
}
struct Core {
    state: Mutex<PoolState>,
    available: Condvar,
}
impl Core {
    fn enqueue(&self, task: Arc<Task>) {
        let mut state = self.state.lock().unwrap();
        let id = task.lane;
        let lane = state.lanes.entry(id).or_default();
        lane.tasks.push_back(task);
        if !lane.scheduled {
            lane.scheduled = true;
            state.ready.push_back(Dispatch::Context(id));
        }
        self.available.notify_one();
    }
    fn run(&self, instance: u64) {
        set_thread(Some(ThreadIdentity::Worker(instance)));
        loop {
            let dispatch = {
                let mut state = self.state.lock().unwrap();
                loop {
                    if let Some(dispatch) = state.ready.pop_front() {
                        break dispatch;
                    }
                    if state.closing && state.tasks.is_empty() {
                        set_thread(None);
                        return;
                    }
                    state = self.available.wait(state).unwrap();
                }
            };
            match dispatch {
                Dispatch::Context(id) => {
                    let task = self
                        .state
                        .lock()
                        .unwrap()
                        .lanes
                        .get_mut(&id)
                        .unwrap()
                        .tasks
                        .pop_front()
                        .unwrap();
                    // Keep scheduled=true while executing, so new work on this
                    // Context cannot be dispatched by another OS thread.
                    task.run(self);
                    let mut state = self.state.lock().unwrap();
                    let lane = state.lanes.get_mut(&id).unwrap();
                    if lane.tasks.is_empty() {
                        state.lanes.remove(&id);
                    } else {
                        state.ready.push_back(Dispatch::Context(id));
                        self.available.notify_one();
                    }
                }
                Dispatch::Blocking(job) => {
                    if let Err(error) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job))
                    {
                        tracing::error!(error = %panic_error(error), "blocking result destructor panicked");
                    }
                }
            }
        }
    }
}

pub(crate) struct WorkerPool {
    core: Arc<Core>,
    handles: Mutex<Vec<JoinHandle<()>>>,
    threads: usize,
    instance: u64,
    next_task: AtomicU64,
    exited: Arc<Notify>,
}
impl WorkerPool {
    pub fn new(threads: usize, instance: u64) -> std::io::Result<Self> {
        assert!(threads > 0, "worker thread count must be > 0");
        let core = Arc::new(Core {
            state: Mutex::new(PoolState {
                ready: VecDeque::new(),
                lanes: HashMap::new(),
                tasks: HashMap::new(),
                closing: false,
            }),
            available: Condvar::new(),
        });
        let exited = Arc::new(Notify::new());
        let mut handles = Vec::with_capacity(threads);
        for _ in 0..threads {
            let worker = core.clone();
            let exit = exited.clone();
            match std::thread::Builder::new()
                .name("vertx-rs-worker".into())
                .spawn(move || {
                    worker.run(instance);
                    exit.notify_one();
                }) {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    core.state.lock().unwrap().closing = true;
                    core.available.notify_all();
                    for handle in handles {
                        let _ = handle.join();
                    }
                    return Err(error);
                }
            }
        }
        Ok(Self {
            core,
            handles: Mutex::new(handles),
            threads,
            instance,
            next_task: AtomicU64::new(1),
            exited,
        })
    }
    pub fn threads(&self) -> usize {
        self.threads
    }
    pub fn spawn<F, T>(&self, lane: u64, driver: Handle, future: F) -> Result<TaskHandle<T>>
    where
        F: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        let finished = Arc::new(AtomicBool::new(false));
        let task = Arc::new(Task {
            id: self.next_task.fetch_add(1, Ordering::Relaxed),
            lane,
            instance: self.instance,
            driver,
            pool: Arc::downgrade(&self.core),
            finished: finished.clone(),
            state: Mutex::new(TaskState {
                work: Some(Box::new(FutureWork {
                    future: Some(Box::pin(future)),
                    output: None,
                    sender,
                })),
                phase: TaskPhase::Waiting,
                notified: false,
                cancelled: false,
            }),
        });
        {
            let mut state = self.core.state.lock().unwrap();
            if state.closing {
                return Err(VertxError::VertxClosed);
            }
            state.tasks.insert(task.id, task.clone());
        }
        task.schedule(false);
        Ok(TaskHandle::from_channel(
            receiver,
            Arc::new(WorkerControl {
                task: Arc::downgrade(&task),
                finished,
            }),
        ))
    }
    pub fn spawn_blocking<F, T>(&self, f: F) -> Result<oneshot::Receiver<Result<T>>>
    where
        F: FnOnce() -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let mut state = self.core.state.lock().unwrap();
        if state.closing {
            return Err(VertxError::VertxClosed);
        }
        let (sender, receiver) = oneshot::channel();
        state.ready.push_back(Dispatch::Blocking(Box::new(move || {
            let output = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f))
                .unwrap_or_else(|e| Err(panic_error(e)));
            let _ = sender.send(output);
        })));
        self.core.available.notify_one();
        Ok(receiver)
    }
    pub async fn shutdown(&self) -> Vec<VertxError> {
        self.core.state.lock().unwrap().closing = true;
        self.core.available.notify_all();
        loop {
            let notified = self.exited.notified();
            if self
                .handles
                .lock()
                .unwrap()
                .iter()
                .all(JoinHandle::is_finished)
            {
                break;
            }
            tokio::select! {
                _ = notified => {},
                _ = tokio::time::sleep(std::time::Duration::from_millis(1)) => {},
            }
        }
        // All handles finished above; join cannot block and failures remain visible.
        self.handles
            .lock()
            .unwrap()
            .drain(..)
            .filter_map(|handle| handle.join().err().map(panic_error))
            .collect()
    }
}
impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.core.state.lock().unwrap().closing = true;
        self.core.available.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::task::Waker;

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
            assert!(pool.core.state.lock().unwrap().ready.is_empty());
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
}
