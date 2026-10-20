use crate::context::{set_thread, ThreadIdentity};
use crate::{Result, VertxError};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::task::LocalPoolHandle;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EventLoopId(pub(crate) usize);
impl std::fmt::Display for EventLoopId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "el#{}", self.0)
    }
}
pub(crate) struct EventLoopPool {
    pool: Mutex<Option<LocalPoolHandle>>,
    size: usize,
    instance: u64,
    rr: AtomicUsize,
    threads: Vec<std::thread::ThreadId>,
    drivers: Vec<tokio::runtime::Handle>,
}
impl EventLoopPool {
    pub fn new(size: usize, instance: u64) -> Self {
        assert!(size > 0, "event loop pool size must be > 0");
        let pool = LocalPoolHandle::new(size);
        let mut threads = Vec::with_capacity(size);
        let mut drivers = Vec::with_capacity(size);
        for idx in 0..size {
            let (tx, rx) = std::sync::mpsc::sync_channel(1);
            pool.spawn_pinned_by_idx(
                move || async move {
                    set_thread(Some(ThreadIdentity::EventLoop(instance, EventLoopId(idx))));
                    let _ = tx.send((
                        std::thread::current().id(),
                        tokio::runtime::Handle::current(),
                    ));
                },
                idx,
            );
            let (thread, driver) = rx.recv().expect("event loop initialization failed");
            threads.push(thread);
            drivers.push(driver);
        }
        Self {
            pool: Mutex::new(Some(pool)),
            size,
            instance,
            rr: AtomicUsize::new(0),
            threads,
            drivers,
        }
    }
    pub fn driver(&self, id: EventLoopId) -> tokio::runtime::Handle {
        self.drivers[id.0].clone()
    }
    pub fn size(&self) -> usize {
        self.size
    }
    pub fn pick(&self) -> EventLoopId {
        EventLoopId(self.rr.fetch_add(1, Ordering::Relaxed) % self.size)
    }
    pub fn check_thread(&self, id: EventLoopId) -> bool {
        crate::context::thread_identity() == Some(ThreadIdentity::EventLoop(self.instance, id))
            && self.threads[id.0] == std::thread::current().id()
    }
    pub fn spawn_pinned<F, Fut, R>(&self, id: EventLoopId, factory: F) -> Result<JoinHandle<R>>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = R> + 'static,
        R: Send + 'static,
    {
        let p = self.pool.lock().unwrap();
        let pool = p.as_ref().ok_or(VertxError::VertxClosed)?;
        Ok(pool.spawn_pinned_by_idx(factory, id.0))
    }
    pub fn shutdown(&self) {
        self.pool.lock().unwrap().take();
    }
}
