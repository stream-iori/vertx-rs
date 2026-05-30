use std::future::Future;
use std::sync::Arc;

use tokio::runtime::{Builder, Runtime};
use tokio::task::JoinHandle;

/// Wraps `Runtime` so it can be dropped from any context (including inside
/// another tokio runtime, as happens in `#[tokio::test]`).
pub(crate) struct OwnedRuntime(Option<Runtime>);

impl OwnedRuntime {
    fn new(rt: Runtime) -> Self {
        Self(Some(rt))
    }
    fn get(&self) -> &Runtime {
        self.0.as_ref().expect("runtime taken")
    }
}

impl Drop for OwnedRuntime {
    fn drop(&mut self) {
        if let Some(rt) = self.0.take() {
            rt.shutdown_background();
        }
    }
}

/// Dedicated multi-threaded tokio runtime for `WorkerVerticle`s and
/// `execute_blocking` work. Independent of the EventLoop pool.
pub(crate) struct WorkerPool {
    rt: Arc<OwnedRuntime>,
    threads: usize,
}

impl WorkerPool {
    pub fn new(threads: usize) -> std::io::Result<Self> {
        assert!(threads > 0, "worker pool size must be > 0");
        let rt = Builder::new_multi_thread()
            .worker_threads(threads)
            .thread_name("vertx-rs-worker")
            .enable_all()
            .build()?;
        Ok(Self {
            rt: Arc::new(OwnedRuntime::new(rt)),
            threads,
        })
    }

    pub fn threads(&self) -> usize {
        self.threads
    }

    pub fn spawn<F>(&self, fut: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.rt.get().spawn(fut)
    }

    pub fn spawn_blocking<F, R>(&self, f: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.rt.get().spawn_blocking(f)
    }
}
