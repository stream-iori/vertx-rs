use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::task::JoinHandle;
use tokio_util::task::LocalPoolHandle;

/// Stable identifier for one EventLoop (one OS thread + LocalSet).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EventLoopId(pub(crate) usize);

impl std::fmt::Display for EventLoopId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "el#{}", self.0)
    }
}

/// Pool of single-threaded tokio runtimes ("event loops"), one per OS thread.
/// Verticles deployed here are pinned to a specific loop for their entire lifetime
/// and may hold `!Send` state.
pub(crate) struct EventLoopPool {
    pool: LocalPoolHandle,
    size: usize,
    rr: AtomicUsize,
}

impl EventLoopPool {
    pub fn new(size: usize) -> Self {
        assert!(size > 0, "event loop pool size must be > 0");
        Self {
            pool: LocalPoolHandle::new(size),
            size,
            rr: AtomicUsize::new(0),
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    /// Round-robin selection.
    pub fn pick(&self) -> EventLoopId {
        EventLoopId(self.rr.fetch_add(1, Ordering::Relaxed) % self.size)
    }

    /// Spawn a task on a specific EventLoop. The factory runs on that loop's
    /// thread, so the future it produces may be `!Send`.
    pub fn spawn_pinned<F, Fut, R>(&self, id: EventLoopId, factory: F) -> JoinHandle<R>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = R> + 'static,
        R: Send + 'static,
    {
        self.pool.spawn_pinned_by_idx(factory, id.0)
    }
}
