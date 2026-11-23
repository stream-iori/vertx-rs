mod scheduler;
mod task;

use crate::error::{panic_error, retain_failure};
use crate::{Result, TaskHandle, VertxError};
use scheduler::Scheduler;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use task::Task;
use tokio::runtime::Handle;
use tokio::sync::{Notify, oneshot};

pub(crate) struct WorkerPool {
    scheduler: Arc<Scheduler>,
    handles: Mutex<Vec<JoinHandle<()>>>,
    threads: usize,
    next_task: AtomicU64,
    exited: Arc<Notify>,
}

impl WorkerPool {
    pub fn new(threads: usize, instance: u64) -> Result<Self> {
        assert!(threads > 0, "worker thread count must be > 0");
        let scheduler = Arc::new(Scheduler::new(instance));
        let exited = Arc::new(Notify::new());
        let mut handles = Vec::with_capacity(threads);
        for _ in 0..threads {
            let worker = scheduler.clone();
            let exit = exited.clone();
            match std::thread::Builder::new()
                .name("vertx-rs-worker".into())
                .spawn(move || {
                    worker.run();
                    exit.notify_one();
                }) {
                Ok(handle) => handles.push(handle),
                Err(error) => return Err(rollback_startup(&scheduler, handles, error)),
            }
        }
        Ok(Self {
            scheduler,
            handles: Mutex::new(handles),
            threads,
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
        let id = self.next_task.fetch_add(1, Ordering::Relaxed);
        let (task, receiver) = Task::new(id, lane, driver, &self.scheduler, future);
        self.scheduler.submit(task.clone())?;
        Ok(task.handle(receiver))
    }

    pub fn spawn_blocking<F, T>(&self, f: F) -> Result<oneshot::Receiver<Result<T>>>
    where
        F: FnOnce() -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        self.scheduler.submit_blocking(move || {
            let output =
                catch_unwind(AssertUnwindSafe(f)).unwrap_or_else(|error| Err(panic_error(error)));
            let _ = sender.send(output);
        })?;
        Ok(receiver)
    }

    pub async fn shutdown(&self) -> Vec<VertxError> {
        self.scheduler.close();
        self.wait_for_exit().await;
        self.handles
            .lock()
            .unwrap()
            .drain(..)
            .filter_map(|handle| handle.join().err().map(panic_error))
            .collect()
    }

    async fn wait_for_exit(&self) {
        loop {
            let notified = self.exited.notified();
            if self
                .handles
                .lock()
                .unwrap()
                .iter()
                .all(JoinHandle::is_finished)
            {
                return;
            }
            tokio::select! {
                _ = notified => {},
                _ = tokio::time::sleep(Duration::from_millis(1)) => {},
            }
        }
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.scheduler.close();
    }
}

fn rollback_startup(
    scheduler: &Scheduler,
    handles: Vec<JoinHandle<()>>,
    error: std::io::Error,
) -> VertxError {
    scheduler.close();
    let mut outcome = Err(VertxError::Other(format!(
        "failed to initialize worker pool: {error}"
    )));
    for handle in handles {
        if let Err(error) = handle.join() {
            retain_failure(&mut outcome, panic_error(error));
        }
    }
    outcome.expect_err("initialization failure retains its cause")
}

#[cfg(test)]
mod tests;
