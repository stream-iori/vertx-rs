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
            task.signal(TaskSignal::Abort);
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
enum TaskSignal {
    Wake,
    Abort,
}

impl TaskSignal {
    fn merge(self, incoming: Self) -> Self {
        match self {
            Self::Abort => Self::Abort,
            Self::Wake => incoming,
        }
    }
}

enum TaskState {
    Waiting(Box<dyn Work>),
    Queued {
        work: Box<dyn Work>,
        signal: TaskSignal,
    },
    Running {
        pending: Option<TaskSignal>,
    },
    // Ignore further signals while the Worker destroys the future and publishes completion.
    Complete,
}

impl TaskState {
    fn on_signal(self, signal: TaskSignal) -> (Self, bool) {
        match self {
            Self::Waiting(work) => (Self::Queued { work, signal }, true),
            Self::Queued {
                work,
                signal: queued,
            } => (
                Self::Queued {
                    work,
                    signal: queued.merge(signal),
                },
                false,
            ),
            Self::Running { pending } => {
                let pending = pending.map_or(signal, |queued| queued.merge(signal));
                (
                    Self::Running {
                        pending: Some(pending),
                    },
                    false,
                )
            }
            Self::Complete => (Self::Complete, false),
        }
    }
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
            state: Mutex::new(TaskState::Waiting(Box::new(FutureWork {
                future: Some(Box::pin(future)),
                output: None,
                sender,
            }))),
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

    pub(super) fn schedule(self: &Arc<Self>) {
        self.signal(TaskSignal::Wake);
    }

    fn signal(self: &Arc<Self>, signal: TaskSignal) {
        let enqueue = {
            let mut state = self.state.lock().unwrap();
            let (next, enqueue) =
                std::mem::replace(&mut *state, TaskState::Complete).on_signal(signal);
            *state = next;
            enqueue
        };
        if enqueue && let Some(scheduler) = self.scheduler.upgrade() {
            scheduler.enqueue(self.clone());
        }
    }

    pub(super) fn run(self: &Arc<Self>, scheduler: &Scheduler) {
        assert!(thread_identity() == Some(ThreadIdentity::Worker(scheduler.instance)));
        let (mut work, signal) = self.take_queued_work();
        let _runtime = self.driver.enter();
        if signal == TaskSignal::Abort {
            self.finish(scheduler, work, Some(VertxError::TaskCancelled));
            return;
        }
        let waker = waker_ref(self);
        let result = catch_unwind(AssertUnwindSafe(|| {
            work.poll(&mut Context::from_waker(&waker))
        }))
        .map_err(panic_error);
        match self.after_poll(work, result) {
            PollOutcome::Complete(work, error) => self.finish(scheduler, work, error),
            PollOutcome::Requeue => scheduler.enqueue(self.clone()),
            PollOutcome::Wait => {}
        }
    }

    fn take_queued_work(&self) -> (Box<dyn Work>, TaskSignal) {
        let mut state = self.state.lock().unwrap();
        let queued = std::mem::replace(&mut *state, TaskState::Complete);
        let TaskState::Queued { work, signal } = queued else {
            *state = queued;
            panic!("scheduled task must be queued");
        };
        if signal == TaskSignal::Wake {
            *state = TaskState::Running { pending: None };
        }
        (work, signal)
    }

    fn after_poll(&self, work: Box<dyn Work>, result: Result<Poll<()>>) -> PollOutcome {
        let mut state = self.state.lock().unwrap();
        let TaskState::Running { pending } = *state else {
            panic!("polled task must be running");
        };
        let error = match (result, pending) {
            (Err(error), _) => Some(error),
            (_, Some(TaskSignal::Abort)) => Some(VertxError::TaskCancelled),
            (Ok(Poll::Ready(())), _) => None,
            (Ok(Poll::Pending), Some(signal @ TaskSignal::Wake)) => {
                *state = TaskState::Queued { work, signal };
                return PollOutcome::Requeue;
            }
            (Ok(Poll::Pending), None) => {
                *state = TaskState::Waiting(work);
                return PollOutcome::Wait;
            }
        };
        *state = TaskState::Complete;
        PollOutcome::Complete(work, error)
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
        task.schedule();
    }
}
