use crate::error::panic_error;
use crate::{Result, TaskHandle, VertxError};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::sync::oneshot;

#[derive(Default)]
struct CancelState {
    cancelled: bool,
    finished: bool,
    abort: Option<tokio::task::AbortHandle>,
}
// Only cancellation metadata crosses threads; the non-Send Future stays local.
#[derive(Default)]
pub(super) struct LocalTaskControl(Mutex<CancelState>);
impl LocalTaskControl {
    pub(super) fn abort(&self) {
        let abort = {
            let mut state = self.0.lock().unwrap();
            if state.finished {
                return;
            }
            state.cancelled = true;
            state.abort.clone()
        };
        if let Some(abort) = abort {
            abort.abort();
        }
    }
    pub(super) fn is_finished(&self) -> bool {
        self.0.lock().unwrap().finished
    }
    pub(super) fn bind(&self, abort: tokio::task::AbortHandle) {
        let cancel = {
            let mut state = self.0.lock().unwrap();
            if state.finished {
                return;
            }
            state.abort = Some(abort.clone());
            state.cancelled
        };
        if cancel {
            abort.abort();
        }
    }
}
impl crate::task::TaskControl for LocalTaskControl {
    fn abort(&self) {
        self.abort();
    }
    fn is_finished(&self) -> bool {
        self.is_finished()
    }
}
struct Completion<T> {
    sender: Option<oneshot::Sender<Result<T>>>,
    control: Arc<LocalTaskControl>,
}
impl<T> Completion<T> {
    fn finish(&mut self, mut output: Result<T>) {
        let (abort, cancelled) = {
            let mut state = self.control.0.lock().unwrap();
            state.finished = true;
            (state.abort.take(), state.cancelled)
        };
        drop(abort);
        if cancelled && output.is_ok() {
            output = Err(VertxError::TaskCancelled);
        }
        if let Some(sender) = self.sender.take() {
            // An abandoned result may itself have a panicking destructor.
            if let Err(error) = catch_unwind(AssertUnwindSafe(|| {
                let _ = sender.send(output);
            })) {
                tracing::error!(error = %panic_error(error), "local task result destructor panicked");
            }
        }
    }
}
impl<T> Drop for Completion<T> {
    fn drop(&mut self) {
        if self.sender.is_some() {
            self.finish(Err(VertxError::TaskCancelled));
        }
    }
}
struct LocalTask<F: Future<Output = Result<T>>, T> {
    future: Option<Pin<Box<F>>>,
    completion: Completion<T>,
}
impl<F: Future<Output = Result<T>>, T> LocalTask<F, T> {
    fn finish(&mut self, output: Result<T>) {
        let dropped = catch_unwind(AssertUnwindSafe(|| drop(self.future.take())));
        self.completion.finish(match dropped {
            Ok(()) => output,
            Err(error) => Err(panic_error(error)),
        });
    }
}
impl<F: Future<Output = Result<T>>, T> Future for LocalTask<F, T> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let future = this
            .future
            .as_mut()
            .expect("local task polled after completion");
        let output = match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(output)) => output,
            Err(error) => Err(panic_error(error)),
        };
        this.finish(output);
        Poll::Ready(())
    }
}
impl<F: Future<Output = Result<T>>, T> Drop for LocalTask<F, T> {
    fn drop(&mut self) {
        if self.future.is_some() {
            self.finish(Err(VertxError::TaskCancelled));
        }
    }
}
pub(super) trait Command: Send {
    fn run(self: Box<Self>);
}

pub(super) fn spawn_command<F, Fut, T>(factory: F) -> (Box<dyn Command>, TaskHandle<T>)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<T>> + 'static,
    T: Send + 'static,
{
    let (sender, receiver) = oneshot::channel();
    let control = Arc::new(LocalTaskControl::default());
    let command = SpawnCommand {
        factory: Some(factory),
        completion: Some(Completion {
            sender: Some(sender),
            control: control.clone(),
        }),
    };
    (
        Box::new(command),
        TaskHandle::from_channel(receiver, control),
    )
}

struct SpawnCommand<F, T> {
    factory: Option<F>,
    completion: Option<Completion<T>>,
}
impl<F, Fut, T> Command for SpawnCommand<F, T>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<T>> + 'static,
    T: Send + 'static,
{
    fn run(mut self: Box<Self>) {
        let factory = self.factory.take().unwrap();
        let mut completion = self.completion.take().unwrap();
        let future = match catch_unwind(AssertUnwindSafe(factory)) {
            Ok(future) => future,
            Err(error) => {
                completion.finish(Err(panic_error(error)));
                return;
            }
        };
        let control = completion.control.clone();
        let task = LocalTask {
            future: Some(Box::pin(future)),
            completion,
        };
        let cancelled = control.0.lock().unwrap().cancelled;
        if cancelled {
            drop(task);
        } else {
            let handle = tokio::task::spawn_local(task);
            // abort() may have raced with creation of the local task.
            control.bind(handle.abort_handle());
        }
    }
}
impl<F, T> Drop for SpawnCommand<F, T> {
    fn drop(&mut self) {
        if let Some(factory) = self.factory.take() {
            // The receiver drains commands on its owning EventLoop, including
            // commands which were accepted but never dispatched.
            let dropped = catch_unwind(AssertUnwindSafe(|| drop(factory)));
            if let Some(mut completion) = self.completion.take() {
                completion.finish(Err(match dropped {
                    Ok(()) => VertxError::TaskCancelled,
                    Err(error) => panic_error(error),
                }));
            }
        }
    }
}
