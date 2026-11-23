use super::scheduler::Scheduler;
use crate::context::{ThreadIdentity, thread_identity};
use crate::error::panic_error;
use crate::{Result, TaskHandle, VertxError};
use futures::task::{ArcWake, waker_ref};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use tokio::runtime::Handle;
use tokio::sync::oneshot;

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
        let dropped = catch_unwind(AssertUnwindSafe(|| drop(self.future.take())));
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
pub(super) struct Task {
    pub(super) id: u64,
    pub(super) lane: u64,
    driver: Handle,
    scheduler: Weak<Scheduler>,
    state: Mutex<TaskState>,
    finished: Arc<AtomicBool>,
}

enum PollOutcome {
    Complete(Box<dyn Work>, Option<VertxError>),
    Requeue,
    Wait,
}

impl Task {
    pub(super) fn new<F, T>(
        id: u64,
        lane: u64,
        driver: Handle,
        scheduler: &Arc<Scheduler>,
        future: F,
    ) -> (Arc<Self>, oneshot::Receiver<Result<T>>)
    where
        F: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        let task = Arc::new(Self {
            id,
            lane,
            driver,
            scheduler: Arc::downgrade(scheduler),
            finished: Arc::new(AtomicBool::new(false)),
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
        (task, receiver)
    }

    pub(super) fn handle<T>(
        self: &Arc<Self>,
        receiver: oneshot::Receiver<Result<T>>,
    ) -> TaskHandle<T> {
        let control = Arc::new(WorkerControl {
            task: Arc::downgrade(self),
            finished: self.finished.clone(),
        });
        TaskHandle::from_channel(receiver, control)
    }

    pub(super) fn schedule(self: &Arc<Self>, cancel: bool) {
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
        if enqueue && let Some(scheduler) = self.scheduler.upgrade() {
            scheduler.enqueue(self.clone());
        }
    }

    pub(super) fn run(self: &Arc<Self>, scheduler: &Scheduler) {
        assert!(thread_identity() == Some(ThreadIdentity::Worker(scheduler.instance)));
        let (mut work, cancelled) = self.begin_poll();
        let _runtime = self.driver.enter();
        let result = if cancelled {
            Ok(Poll::Pending)
        } else {
            let waker = waker_ref(self);
            catch_unwind(AssertUnwindSafe(|| {
                work.poll(&mut Context::from_waker(&waker))
            }))
            .map_err(panic_error)
        };
        match self.after_poll(work, result) {
            PollOutcome::Complete(work, error) => self.finish(scheduler, work, error),
            PollOutcome::Requeue => scheduler.enqueue(self.clone()),
            PollOutcome::Wait => {}
        }
    }

    fn begin_poll(&self) -> (Box<dyn Work>, bool) {
        let mut state = self.state.lock().unwrap();
        state.phase = TaskPhase::Running;
        state.notified = false;
        (
            state.work.take().expect("queued task owns unpolled work"),
            state.cancelled,
        )
    }

    fn after_poll(&self, work: Box<dyn Work>, result: Result<Poll<()>>) -> PollOutcome {
        let mut state = self.state.lock().unwrap();
        let ready = matches!(&result, Ok(Poll::Ready(())));
        let error = match result {
            Err(error) => Some(error),
            _ if state.cancelled => Some(VertxError::TaskCancelled),
            _ => None,
        };
        if ready || error.is_some() {
            state.phase = TaskPhase::Complete;
            return PollOutcome::Complete(work, error);
        }

        state.work = Some(work);
        if state.notified {
            state.phase = TaskPhase::Queued;
            PollOutcome::Requeue
        } else {
            state.phase = TaskPhase::Waiting;
            PollOutcome::Wait
        }
    }

    fn finish(&self, scheduler: &Scheduler, work: Box<dyn Work>, error: Option<VertxError>) {
        // Drop the future while this Worker still owns its Context lane.
        let complete = work.finish(error);
        self.finished.store(true, Ordering::Release);
        scheduler.task_finished(self.id);
        if let Err(error) = catch_unwind(AssertUnwindSafe(complete)) {
            tracing::error!(error = %panic_error(error), "task result destructor panicked");
        }
    }
}
impl ArcWake for Task {
    fn wake_by_ref(task: &Arc<Self>) {
        task.schedule(false);
    }
}
