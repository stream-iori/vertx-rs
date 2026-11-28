mod task;
mod thread;

use self::thread::EventLoop;
use crate::context::{Context, ThreadIdentity, thread_identity};
use crate::error::{panic_error, retain_failure};
use crate::{Result, TaskHandle, TaskId, VertxError};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::sync::watch;

/// Identifies one fixed-thread EventLoop within its Vertx instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EventLoopId(pub(crate) usize);
impl std::fmt::Display for EventLoopId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "el#{}", self.0)
    }
}

pub(crate) struct EventLoopPool {
    loops: Vec<EventLoop>,
    handles: Mutex<Vec<JoinHandle<()>>>,
    instance: u64,
    rr: AtomicUsize,
}
impl EventLoopPool {
    pub fn new(size: usize, instance: u64) -> Result<Self> {
        Self::with_runtime_factory(size, instance, |_| {
            Builder::new_current_thread().enable_all().build()
        })
    }
    pub(crate) fn with_runtime_factory<F>(
        size: usize,
        instance: u64,
        runtime_factory: F,
    ) -> Result<Self>
    where
        F: Fn(usize) -> std::io::Result<Runtime> + Send + Sync + 'static,
    {
        let runtime_factory = Arc::new(runtime_factory);
        assert!(size > 0, "event loop pool size must be > 0");
        let mut pool = Self {
            loops: Vec::with_capacity(size),
            handles: Mutex::new(Vec::with_capacity(size)),
            instance,
            rr: AtomicUsize::new(0),
        };
        for index in 0..size {
            let factory = runtime_factory.clone();
            let started = EventLoop::start(
                instance,
                EventLoopId(index),
                move || factory(index),
                pool.handles.get_mut().unwrap(),
            );
            match started {
                Ok(event_loop) => pool.loops.push(event_loop),
                Err(error) => {
                    let cause =
                        VertxError::Other(format!("failed to initialize event loops: {error}"));
                    return Err(pool.rollback_initialization(cause));
                }
            }
        }
        Ok(pool)
    }
    pub(crate) fn rollback_initialization(&self, cause: VertxError) -> VertxError {
        let mut outcome = Err(cause);
        for error in self.shutdown_initialization() {
            retain_failure(&mut outcome, error);
        }
        outcome.expect_err("initialization rollback always retains the original error")
    }

    // Only for construction rollback: no business tasks have been admitted.
    // Join synchronously so a failed build never leaves startup threads behind.
    fn shutdown_initialization(&self) -> Vec<VertxError> {
        self.request_stop();
        let mut handles = self
            .handles
            .lock()
            .expect("event loop handles poisoned during initialization");
        let threads = std::mem::take(&mut *handles);
        drop(handles);
        threads
            .into_iter()
            .filter_map(|handle| handle.join().err().map(panic_error))
            .collect()
    }

    pub fn driver(&self, id: EventLoopId) -> Handle {
        self.loops[id.0].driver.clone()
    }
    pub fn size(&self) -> usize {
        self.loops.len()
    }
    pub fn pick(&self) -> EventLoopId {
        EventLoopId(self.rr.fetch_add(1, Ordering::Relaxed) % self.size())
    }
    pub fn check_thread(&self, id: EventLoopId) -> bool {
        thread_identity() == Some(ThreadIdentity::EventLoop(self.instance, id))
            && self.loops[id.0].thread == std::thread::current().id()
    }
    pub fn owns_current_thread(&self) -> bool {
        match thread_identity() {
            Some(ThreadIdentity::EventLoop(instance, id)) if instance == self.instance => {
                self.check_thread(id)
            }
            _ => false,
        }
    }
    pub fn spawn_pinned<F, Fut, T>(
        &self,
        loop_id: EventLoopId,
        task_id: TaskId,
        factory: F,
    ) -> Result<TaskHandle<T>>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + 'static,
        T: Send + 'static,
    {
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        let parent = tracing::Span::current();
        let factory = move || {
            tracing::dispatcher::with_default(&dispatch, || {
                let _span = parent.enter();
                crate::task::TracedFuture::new(factory(), dispatch.clone(), parent.clone())
            })
        };
        let admission = self.loops[loop_id.0].admission.lock().unwrap();
        let sender = admission.as_ref().ok_or(VertxError::VertxClosed)?;
        if self.check_thread(loop_id) && Context::current().is_some() {
            // Defer the factory as well: user code never executes inline.
            drop(admission);
            return Ok(TaskHandle::from_tokio(
                task_id,
                tokio::task::spawn_local(async move { factory().await }),
            ));
        }
        let (command, handle) = task::spawn_command(task_id, factory);
        let sent = sender.send(command);
        drop(admission);
        sent.map_err(|_| VertxError::VertxClosed)?;
        Ok(handle)
    }
    fn request_stop(&self) {
        for event_loop in &self.loops {
            event_loop.request_stop();
        }
    }
    // Called as the last phase of close, after all deployments and Workers stop.
    // A temporary reaper can join the coordinating EventLoop without self-wait.
    pub fn finish_shutdown(
        &self,
        mut errors: Vec<VertxError>,
        result: watch::Sender<Option<Result<()>>>,
    ) {
        self.request_stop();
        let handles = std::mem::take(&mut *self.handles.lock().unwrap());
        let failed_result = result.clone();
        let mut failed_errors = errors.clone();
        if let Err(error) = std::thread::Builder::new()
            .name("vertx-rs-close".into())
            .spawn(move || {
                for handle in handles {
                    if let Err(error) = handle.join() {
                        errors.push(panic_error(error));
                    }
                }
                result.send_replace(Some(if errors.is_empty() {
                    Ok(())
                } else {
                    Err(VertxError::CleanupFailed(errors))
                }));
            })
        {
            failed_errors.push(VertxError::Other(format!(
                "failed to start EventLoop reaper: {error}"
            )));
            failed_result.send_replace(Some(Err(VertxError::CleanupFailed(failed_errors))));
        }
    }
}
impl Drop for EventLoopPool {
    fn drop(&mut self) {
        self.request_stop();
    }
}

#[cfg(test)]
mod tests;
