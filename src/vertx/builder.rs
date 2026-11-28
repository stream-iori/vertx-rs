use super::{Executors, InstanceLifetime, InstanceServices, Vertx, VertxInner};
use crate::context::ExceptionSlot;
use crate::eventloop::EventLoopPool;
use crate::scope::WorkScope;
use crate::worker_pool::WorkerPool;
use crate::{EventBus, Result, VertxError};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::watch;

/// Configures fixed execution pools and shutdown limits.
#[must_use = "builder settings take effect only when built"]
pub struct VertxBuilder {
    event_loop_threads: Option<usize>,
    worker_threads: usize,
    shutdown_grace: Duration,
    diagnostics: crate::DiagnosticsOptions,
}
impl Default for VertxBuilder {
    fn default() -> Self {
        Self {
            event_loop_threads: None,
            worker_threads: 20,
            shutdown_grace: Duration::from_secs(1),
            diagnostics: crate::DiagnosticsOptions::default(),
        }
    }
}
static NEXT_INSTANCE: AtomicU64 = AtomicU64::new(1);
impl VertxBuilder {
    /// Configures counters and optional bounded task and queue details.
    pub fn diagnostics(mut self, options: crate::DiagnosticsOptions) -> Self {
        self.diagnostics = options;
        self
    }
    /// Sets the fixed EventLoop count; zero is rejected when built.
    pub fn event_loop_threads(mut self, n: usize) -> Self {
        self.event_loop_threads = Some(n);
        self
    }
    /// Sets the shared Worker/Future/blocking thread count; zero is rejected when built.
    pub fn worker_threads(mut self, n: usize) -> Self {
        self.worker_threads = n;
        self
    }
    /// Sets a positive wait limit for shutdown operations.
    /// Timeout stops waiting, not ongoing cleanup or already-started blocking work.
    pub fn shutdown_grace(mut self, d: Duration) -> Self {
        self.shutdown_grace = d;
        self
    }
    /// Creates an instance, joining startup threads before returning a failure.
    ///
    /// Zero thread counts or shutdown grace return [`VertxError::InvalidOptions`].
    /// Runtime/thread creation errors return [`VertxError::Other`] with the failed
    /// stage and underlying error text. Rollback errors are retained in
    /// [`VertxError::LifecycleFailed`]. No business work runs during rollback.
    ///
    /// ```
    /// use vertx_rs::{Result, Vertx};
    ///
    /// #[tokio::main(flavor = "current_thread")]
    /// async fn main() -> Result<()> {
    ///     let vertx = Vertx::builder().event_loop_threads(1).worker_threads(2).build()?;
    ///     vertx.close().await
    /// }
    /// ```
    pub fn build(self) -> Result<Vertx> {
        self.build_with_pools(EventLoopPool::new, WorkerPool::new)
    }

    fn build_with_pools<E, W>(self, event_loops: E, workers: W) -> Result<Vertx>
    where
        E: FnOnce(usize, u64) -> Result<EventLoopPool>,
        W: FnOnce(usize, u64) -> Result<WorkerPool>,
    {
        self.validate()?;
        let instance = NEXT_INSTANCE.fetch_add(1, Ordering::Relaxed);
        let loop_count = self.event_loop_threads.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get().saturating_mul(2))
                .unwrap_or(2)
        });
        let event_loops = event_loops(loop_count, instance)?;
        let workers = workers(self.worker_threads, instance)
            .map_err(|error| event_loops.rollback_initialization(error))?;
        Ok(self.assemble(instance, event_loops, workers))
    }

    fn validate(&self) -> Result<()> {
        if self.event_loop_threads == Some(0) || self.worker_threads == 0 {
            return Err(VertxError::InvalidOptions(
                "thread counts must be positive".into(),
            ));
        }
        if self.shutdown_grace.is_zero() {
            return Err(VertxError::InvalidOptions(
                "shutdown grace must be positive".into(),
            ));
        }
        Ok(())
    }

    fn assemble(self, instance: u64, event_loops: EventLoopPool, workers: WorkerPool) -> Vertx {
        let diagnostics = crate::diagnostics::Diagnostics::new(instance, self.diagnostics);
        let scope = Arc::new(WorkScope::instance());
        let event_bus = EventBus::new(&scope, diagnostics.clone());
        let inner = Arc::new(VertxInner {
            executors: Arc::new(Executors {
                event_loops,
                workers,
            }),
            services: Arc::new(InstanceServices {
                diagnostics,
                event_bus,
                tcp: crate::tcp::TcpListeners::default(),
                exception_handler: ExceptionSlot::default(),
            }),
            lifetime: Arc::new(InstanceLifetime {
                scope,
                shutdown_grace: self.shutdown_grace,
            }),
            deployments: Default::default(),
            close_result: watch::channel(None).0,
        });
        Vertx { inner }
    }
}

#[cfg(test)]
mod tests {
    use crate::eventloop::EventLoopPool;
    use crate::{Vertx, VertxError};
    use std::cell::RefCell;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    struct ExitProbe(Arc<AtomicUsize>);
    impl Drop for ExitProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    thread_local! {
        static EXIT_PROBE: RefCell<Option<ExitProbe>> = const { RefCell::new(None) };
    }

    #[rstest::rstest]
    #[case::event_loop(0, 1, Duration::from_secs(1))]
    #[case::worker(1, 0, Duration::from_secs(1))]
    #[case::grace(1, 1, Duration::ZERO)]
    fn invalid_options_are_rejected_before_pool_creation(
        #[case] event_loops: usize,
        #[case] workers: usize,
        #[case] grace: Duration,
    ) {
        let result = Vertx::builder()
            .event_loop_threads(event_loops)
            .worker_threads(workers)
            .shutdown_grace(grace)
            .build_with_pools(
                |_, _| panic!("invalid options must not create event loops"),
                |_, _| panic!("invalid options must not create workers"),
            );
        assert!(matches!(result, Err(VertxError::InvalidOptions(_))));
    }

    #[test]
    fn worker_initialization_failure_joins_event_loops_before_returning() {
        let exited = Arc::new(AtomicUsize::new(0));
        let probe = exited.clone();
        let result = Vertx::builder()
            .event_loop_threads(2)
            .worker_threads(1)
            .build_with_pools(
                move |count, instance| {
                    EventLoopPool::with_runtime_factory(count, instance, move |_| {
                        EXIT_PROBE.with(|slot| *slot.borrow_mut() = Some(ExitProbe(probe.clone())));
                        tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                    })
                },
                |_, _| Err(VertxError::Other("injected worker pool failure".into())),
            );
        assert!(
            matches!(result, Err(VertxError::Other(message)) if message == "injected worker pool failure")
        );
        assert_eq!(
            exited.load(Ordering::SeqCst),
            2,
            "build must join startup threads before returning"
        );
    }
}
