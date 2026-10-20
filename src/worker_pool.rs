use crate::context::{set_thread, ThreadIdentity};
use crate::{Result, VertxError};
use std::future::Future;
use std::sync::{Arc, Mutex};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

pub(crate) struct WorkerPool {
    rt: Mutex<Option<Runtime>>,
    threads: usize,
    blocking_threads: usize,
    pub permits: Arc<Semaphore>,
}
impl WorkerPool {
    pub fn new(threads: usize, blocking_threads: usize, instance: u64) -> std::io::Result<Self> {
        assert!(
            threads > 0 && blocking_threads > 0,
            "thread counts must be > 0"
        );
        let rt = Builder::new_multi_thread()
            .worker_threads(threads)
            .max_blocking_threads(blocking_threads)
            .thread_name("vertx-rs-worker")
            .on_thread_start(move || set_thread(Some(ThreadIdentity::Worker(instance))))
            .on_thread_stop(|| set_thread(None))
            .enable_all()
            .build()?;
        Ok(Self {
            rt: Mutex::new(Some(rt)),
            threads,
            blocking_threads,
            permits: Arc::new(Semaphore::new(blocking_threads)),
        })
    }
    pub fn threads(&self) -> usize {
        self.threads
    }
    pub fn blocking_threads(&self) -> usize {
        self.blocking_threads
    }
    pub fn spawn<F>(&self, future: F) -> Result<JoinHandle<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let rt = self.rt.lock().unwrap();
        Ok(rt.as_ref().ok_or(VertxError::VertxClosed)?.spawn(future))
    }
    pub fn spawn_blocking<F, R>(&self, f: F) -> Result<JoinHandle<R>>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let rt = self.rt.lock().unwrap();
        Ok(rt
            .as_ref()
            .ok_or(VertxError::VertxClosed)?
            .spawn_blocking(f))
    }
    pub fn shutdown(&self) {
        self.permits.close();
        if let Some(rt) = self.rt.lock().unwrap().take() {
            rt.shutdown_background();
        }
    }
}
impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}
