use crate::context::{Context, ContextShared};
use crate::deployment::{CancelOnDrop, DeploymentScope};
use crate::diagnostics::{CloseStage, DeploymentStage};
use crate::error::{panic_error, retain_failure};
use crate::eventloop::EventLoopPool;
use crate::worker_pool::WorkerPool;
use crate::{DeploymentId, EventBus, Result, Verticle, VertxError, WorkerVerticle};
use futures::FutureExt;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{oneshot, watch};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub(crate) struct VertxInner {
    pub shared: Arc<ContextShared>,
    deployments: Mutex<HashMap<DeploymentId, Arc<DeploymentScope>>>,
    admission: Mutex<()>,
    close_result: watch::Sender<Option<Result<()>>>,
}
impl Drop for VertxInner {
    fn drop(&mut self) {
        self.shared.cancel.cancel();
        for scope in self.deployments.get_mut().unwrap().values() {
            scope.request_stop();
        }
        self.shared.event_bus.close();
    }
}
#[derive(Clone)]
pub struct Vertx {
    pub(crate) inner: Arc<VertxInner>,
}

async fn quiesce_deployment(ctx: &Context) {
    ctx.inner
        .diagnostics
        .deployment(ctx.scope.deployment, DeploymentStage::Quiescing);
    ctx.scope.request_stop();
    ctx.inner.event_bus.cleanup_scope(&ctx.scope);
    ctx.scope.quiesce(ctx.inner.shutdown_grace).await;
}
fn finish_deployment(
    ctx: &Context,
    outcome: Result<()>,
    startup_failure: Option<VertxError>,
    ready: Option<oneshot::Sender<Result<()>>>,
) {
    if let Err(error) = &outcome {
        ctx.report_error(error);
    }
    ctx.clear_exception_handler();
    ctx.scope.finish(outcome.clone());
    if let Some(tx) = ready {
        let error = outcome
            .err()
            .or(startup_failure)
            .unwrap_or_else(|| VertxError::StartFailed("startup cancelled".into()));
        let _ = tx.send(Err(error));
    }
    if let Some(owner) = ctx.inner.vertx.upgrade() {
        owner
            .deployments
            .lock()
            .unwrap()
            .remove(&ctx.scope.deployment);
    }
    ctx.inner
        .diagnostics
        .deployment(ctx.scope.deployment, DeploymentStage::Stopped);
}

// A cancelled startup still reports non-readiness to deploy, but does not turn
// orderly instance shutdown into a cleanup failure.
fn record_start_failure(start: Result<()>, ctx: &Context, outcome: &mut Result<()>) -> VertxError {
    let error = start
        .err()
        .unwrap_or(VertxError::DeploymentTerminated(ctx.scope.deployment));
    if !matches!(error, VertxError::DeploymentTerminated(_)) {
        *outcome = Err(error.clone());
    }
    error
}

fn normalize_start(result: std::thread::Result<Result<()>>) -> (Result<()>, bool) {
    match result {
        Ok(result) => (result, false),
        Err(error) => (Err(panic_error(error)), true),
    }
}

fn notify_started(ctx: &Context, ready: &mut Option<oneshot::Sender<Result<()>>>) {
    ctx.inner
        .diagnostics
        .deployment(ctx.scope.deployment, DeploymentStage::Running);
    if ready
        .take()
        .expect("readiness sent only once")
        .send(Ok(()))
        .is_err()
    {
        ctx.scope.request_stop();
    }
}

// Separate drivers retain the Worker Send bound and EventLoop !Send support.
// Only lifecycle calls vary; synchronous outcome/notification work stays in functions.
macro_rules! lifecycle_driver {
    ($name:ident, $trait:ident) => {
        async fn $name<V, F>(ctx: Context, factory: F, ready: oneshot::Sender<Result<()>>)
        where
            V: $trait,
            F: FnOnce(Context) -> V,
        {
            ctx.inner
                .diagnostics
                .deployment(ctx.scope.deployment, DeploymentStage::Starting);
            let mut ready = Some(ready);
            let scope = ctx.scope.clone();
            let created =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| factory(ctx.clone())));
            let mut outcome = Ok(());
            let mut startup_failure = None;
            match created {
                Err(error) => outcome = Err(panic_error(error)),
                Ok(mut verticle) => {
                    let (start, panicked) = tokio::select! { biased;
                        _ = scope.cancel.cancelled() => (
                            Err(VertxError::DeploymentTerminated(scope.deployment)), false,
                        ),
                        result = std::panic::AssertUnwindSafe(verticle.start(ctx.clone()))
                            .catch_unwind() => normalize_start(result),
                    };
                    if start.is_ok() && scope.running() {
                        notify_started(&ctx, &mut ready);
                        scope.cancel.cancelled().await;
                    } else {
                        startup_failure = Some(record_start_failure(start, &ctx, &mut outcome));
                    }
                    quiesce_deployment(&ctx).await;
                    if !panicked {
                        ctx.inner
                            .diagnostics
                            .deployment(scope.deployment, DeploymentStage::Stopping);
                        let stop_ctx = ctx.for_stop();
                        let stopped = std::panic::AssertUnwindSafe(
                            stop_ctx.scope_future(verticle.stop(stop_ctx.clone())),
                        )
                        .catch_unwind()
                        .await
                        .unwrap_or_else(|error| Err(panic_error(error)));
                        if let Err(error) = stopped {
                            retain_failure(&mut outcome, error);
                        }
                    }
                    let dropped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        drop(verticle);
                    }));
                    if let Err(error) = dropped {
                        retain_failure(&mut outcome, panic_error(error));
                    }
                }
            }
            quiesce_deployment(&ctx).await;
            finish_deployment(&ctx, outcome, startup_failure, ready);
        }
    };
}
lifecycle_driver!(drive_local, Verticle);
lifecycle_driver!(drive_worker, WorkerVerticle);

impl Vertx {
    pub(crate) fn from_inner(inner: Arc<VertxInner>) -> Self {
        Self { inner }
    }
    /// Creates an instance using default pool sizes and shutdown grace.
    ///
    /// # Panics
    /// Panics on runtime or thread initialization failure. For fallible construction,
    /// use `Vertx::builder().try_build()`.
    pub fn new() -> Self {
        Self::builder().build()
    }
    /// Starts configuration without creating threads.
    pub fn builder() -> VertxBuilder {
        VertxBuilder::default()
    }
    /// Samples counters and current operations without dispatching work.
    /// The result is not a globally atomic snapshot.
    pub fn diagnostics(&self) -> crate::DiagnosticsSnapshot {
        let mut snapshot = self.inner.shared.diagnostics.snapshot();
        if snapshot.enabled
            && let Some(result) = self.inner.close_result.borrow().as_ref()
        {
            snapshot.close_stage = if result.is_ok() {
                CloseStage::Closed
            } else {
                CloseStage::Failed
            }
            .as_str();
        }
        snapshot
    }
    /// Returns the instance bus. Consumer registration in one of this instance's
    /// Contexts binds to it; registration outside a matching Context is unbound.
    pub fn event_bus(&self) -> EventBus {
        self.inner.shared.event_bus.clone()
    }
    pub fn event_loop_count(&self) -> usize {
        self.inner.shared.event_loops.size()
    }
    pub fn worker_thread_count(&self) -> usize {
        self.inner.shared.workers.threads()
    }
    /// Returns whether business admission has closed.
    /// Cleanup and thread joins may still be in progress. Await [`Self::close`]
    /// to confirm that all executor threads have exited.
    pub fn is_closed(&self) -> bool {
        self.inner.shared.cancel.is_cancelled()
    }
    /// Creates a timer with an absolute deadline starting now.
    /// Returns `VertxClosed` after closing. Polling needs a Tokio time driver.
    /// An extremely large delay may panic if the deadline cannot be represented.
    pub fn timer(&self, delay: Duration) -> Result<crate::Timer> {
        self.event_bus().ensure_open()?;
        Ok(crate::Timer::new(delay, self.inner.shared.cancel.clone()))
    }
    /// Creates a periodic timer with an absolute initial deadline and Skip policy.
    /// Returns `InvalidOptions` for a zero period, or `VertxClosed` after closing.
    /// Ticking needs a Tokio time driver; an unrepresentable initial deadline may panic.
    pub fn periodic(&self, init: Duration, period: Duration) -> Result<crate::Periodic> {
        self.event_bus().ensure_open()?;
        crate::Periodic::new(init, period, self.inner.shared.cancel.clone())
    }
    pub fn set_exception_handler<F>(&self, handler: F)
    where
        F: Fn(&VertxError) + Send + Sync + 'static,
    {
        *self.inner.shared.exception_handler.write().unwrap() = Some(Arc::new(handler));
    }
    pub fn clear_exception_handler(&self) {
        self.inner.shared.exception_handler.write().unwrap().take();
    }
    /// Constructs a non-Send verticle on its assigned EventLoop and awaits start.
    /// Factory, lifecycle and managed work stay on that OS thread. Dropping the
    /// waiter requests background cleanup. Successful start leaves the deployment
    /// running until undeploy/close; startup and cleanup failures are retained.
    pub async fn deploy<F, V>(&self, factory: F) -> Result<DeploymentId>
    where
        F: FnOnce(Context) -> V + Send + 'static,
        V: Verticle,
    {
        let (scope, rx) = {
            let _admission = self.inner.admission.lock().unwrap();
            self.event_bus().ensure_open()?;
            let id = DeploymentId::next();
            let scope = Arc::new(DeploymentScope::new(
                id,
                self.inner.shared.cancel.child_token(),
            ));
            let loop_id = self.inner.shared.event_loops.pick();
            let ctx = Context::new_event_loop(loop_id, self.inner.shared.clone(), scope.clone());
            let (tx, rx) = oneshot::channel();
            self.inner
                .shared
                .diagnostics
                .deployment(id, DeploymentStage::Queued);
            self.inner
                .deployments
                .lock()
                .unwrap()
                .insert(id, scope.clone());
            if let Err(e) = self
                .inner
                .shared
                .event_loops
                .spawn_pinned(loop_id, move || {
                    ctx.clone().wrap(async move {
                        drive_local(ctx, factory, tx).await;
                        Ok(())
                    })
                })
            {
                self.inner.deployments.lock().unwrap().remove(&id);
                self.inner
                    .shared
                    .diagnostics
                    .deployment(id, DeploymentStage::Stopped);
                return Err(e);
            }
            (scope, rx)
        };
        let mut cancel = CancelOnDrop(Some(scope.clone()));
        let result = rx
            .await
            .map_err(|_| VertxError::StartFailed("lifecycle driver terminated".into()))?;
        result?;
        cancel.0 = None;
        Ok(scope.deployment)
    }
    /// Constructs a Send Worker verticle and awaits its asynchronous start.
    /// Same-Context polls are serialized but may migrate across Worker threads.
    /// Dropping the waiter requests background cleanup, as with [`Self::deploy`].
    pub async fn deploy_worker<F, V>(&self, factory: F) -> Result<DeploymentId>
    where
        F: FnOnce(Context) -> V + Send + 'static,
        V: WorkerVerticle,
    {
        let (scope, rx) = {
            let _admission = self.inner.admission.lock().unwrap();
            self.event_bus().ensure_open()?;
            let id = DeploymentId::next();
            let scope = Arc::new(DeploymentScope::new(
                id,
                self.inner.shared.cancel.child_token(),
            ));
            let ctx = Context::new_worker(self.inner.shared.clone(), scope.clone());
            let (tx, rx) = oneshot::channel();
            self.inner
                .shared
                .diagnostics
                .deployment(id, DeploymentStage::Queued);
            self.inner
                .deployments
                .lock()
                .unwrap()
                .insert(id, scope.clone());
            if let Err(e) = self.inner.shared.workers.spawn(
                ctx.worker_lane(),
                ctx.worker_driver(),
                ctx.clone().wrap(async move {
                    drive_worker(ctx, factory, tx).await;
                    Ok(())
                }),
            ) {
                self.inner.deployments.lock().unwrap().remove(&id);
                self.inner
                    .shared
                    .diagnostics
                    .deployment(id, DeploymentStage::Stopped);
                return Err(e);
            }
            (scope, rx)
        };
        let mut cancel = CancelOnDrop(Some(scope.clone()));
        rx.await
            .map_err(|_| VertxError::StartFailed("worker lifecycle driver terminated".into()))??;
        cancel.0 = None;
        Ok(scope.deployment)
    }
    /// Deploys instances sequentially with shared typed configuration.
    /// Failure rolls back prior instances and retains rollback errors. Dropping
    /// the waiter requests cleanup of current and prior instances. Zero instances
    /// are normalized to one by [`crate::DeploymentOptions::instances`].
    pub async fn deploy_with<F, V, C>(
        &self,
        factory: F,
        options: crate::DeploymentOptions<C>,
    ) -> Result<Vec<DeploymentId>>
    where
        F: Fn(Context, Arc<C>) -> V + Send + Sync + 'static,
        V: Verticle,
        C: Send + Sync + 'static,
    {
        let factory = Arc::new(factory);
        let config = options.shared_config();
        let mut ids = Vec::new();
        struct Rollback(Vec<Arc<DeploymentScope>>);
        impl Drop for Rollback {
            fn drop(&mut self) {
                for scope in &self.0 {
                    scope.request_stop();
                }
            }
        }
        let mut guard = Rollback(Vec::new());
        for _ in 0..options.instances_count() {
            let (f, cfg) = (factory.clone(), config.clone());
            match self.deploy(move |ctx| f(ctx, cfg)).await {
                Ok(id) => {
                    if let Some(s) = self.inner.deployments.lock().unwrap().get(&id) {
                        guard.0.push(s.clone());
                    }
                    ids.push(id);
                }
                Err(cause) => {
                    let mut rollback = Vec::new();
                    for scope in &guard.0 {
                        scope.request_stop();
                    }
                    for scope in &guard.0 {
                        if let Err(e) = self.wait_scope(scope).await {
                            rollback.push(e);
                        }
                    }
                    return Err(VertxError::DeploymentFailed {
                        cause: Box::new(cause),
                        rollback,
                    });
                }
            }
        }
        guard.0.clear();
        Ok(ids)
    }
    async fn wait_scope(&self, scope: &DeploymentScope) -> Result<()> {
        tokio::time::timeout(self.inner.shared.shutdown_grace, scope.wait())
            .await
            .map_err(|_| VertxError::ShutdownTimeout(self.inner.shared.shutdown_grace))?
    }
    /// Requests shutdown and waits for managed state destruction and stop.
    /// One shutdown grace bounds the wait; timeout leaves cleanup running.
    /// Waiting from the same deployment returns `WouldDeadlock`; use
    /// [`Context::request_undeploy`] there. Unknown IDs return `DeploymentNotFound`.
    pub async fn undeploy(&self, id: DeploymentId) -> Result<()> {
        if Context::current().is_some_and(|c| {
            c.deployment_id() == Some(id)
                && c.owner()
                    .is_some_and(|v| Arc::ptr_eq(&v.inner, &self.inner))
        }) {
            return Err(VertxError::WouldDeadlock);
        }
        let scope = self
            .inner
            .deployments
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or(VertxError::DeploymentNotFound(id))?;
        scope.request_stop();
        let result = self.wait_scope(&scope).await;
        if matches!(result, Err(VertxError::ShutdownTimeout(_))) {
            tracing::warn!(deployment = %id, "shutdown timed out; cleanup continues");
        }
        result
    }
    /// Idempotent asynchronous shutdown. Cancellation of this waiter does not
    /// cancel cleanup. A timeout leaves the instance closed to new work.
    ///
    /// Success means all Worker and EventLoop threads have exited and been joined.
    /// Repeated/concurrent calls observe the same cleanup; a later call may wait
    /// again after timeout. Waiting in this instance's Context or on any of its
    /// EventLoop threads returns `WouldDeadlock`, including from raw Tokio tasks.
    pub async fn close(&self) -> Result<()> {
        if self.inner.shared.event_loops.owns_current_thread()
            || Context::current().is_some_and(|c| {
                c.owner()
                    .is_some_and(|v| Arc::ptr_eq(&v.inner, &self.inner))
            })
        {
            return Err(VertxError::WouldDeadlock);
        }
        {
            let _admission = self.inner.admission.lock().unwrap();
            if !self.is_closed() {
                let owner = self.inner.clone();
                let scopes: Vec<_> = owner
                    .deployments
                    .lock()
                    .unwrap()
                    .values()
                    .cloned()
                    .collect();
                owner
                    .shared
                    .diagnostics
                    .close_stage(CloseStage::Deployments);
                owner.shared.cancel.cancel();
                owner.shared.event_bus.close();
                for scope in &scopes {
                    scope.request_stop();
                }
                owner.shared.blocking.close();
                let control_loop = owner.shared.event_loops.pick();
                self.inner
                    .shared
                    .event_loops
                    .spawn_pinned(control_loop, move || async move {
                        let mut errors = Vec::new();
                        for scope in scopes {
                            if let Err(e) = scope.wait().await {
                                errors.push(e);
                            }
                        }
                        owner.shared.diagnostics.close_stage(CloseStage::Blocking);
                        owner.shared.blocking.wait().await;
                        owner.shared.exception_handler.write().unwrap().take();
                        owner.shared.diagnostics.close_stage(CloseStage::WorkerJoin);
                        errors.extend(owner.shared.workers.shutdown().await);
                        owner
                            .shared
                            .diagnostics
                            .close_stage(CloseStage::EventLoopJoin);
                        owner
                            .shared
                            .event_loops
                            .finish_shutdown(errors, owner.close_result.clone());
                        Ok(())
                    })?;
            }
        }
        let wait = async {
            let mut rx = self.inner.close_result.subscribe();
            loop {
                if let Some(result) = rx.borrow().clone() {
                    return result;
                }
                rx.changed().await.map_err(|_| VertxError::VertxClosed)?;
            }
        };
        tokio::time::timeout(self.inner.shared.shutdown_grace, wait)
            .await
            .map_err(|_| VertxError::ShutdownTimeout(self.inner.shared.shutdown_grace))?
    }
}
impl Default for Vertx {
    fn default() -> Self {
        Self::new()
    }
}
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
    /// Creates an instance with the configured fixed thread pools.
    ///
    /// # Panics
    /// Panics on zero thread counts, zero shutdown grace, or initialization failure.
    /// Use [`Self::try_build`] to handle these failures without unwinding.
    pub fn build(self) -> Vertx {
        self.try_build().expect("failed to initialize Vertx")
    }

    /// Creates an instance, joining startup threads before returning a failure.
    ///
    /// Zero thread counts or shutdown grace return [`VertxError::InvalidOptions`].
    /// Runtime/thread creation errors return [`VertxError::Other`] with the failed
    /// stage and underlying error text. Rollback errors are retained in
    /// [`VertxError::LifecycleFailed`]. No business work runs during rollback.
    ///
    /// ```
    /// # use vertx_rs::{Result, Vertx};
    /// # async fn example() -> Result<()> {
    /// let vx = Vertx::builder().event_loop_threads(1).worker_threads(2).try_build()?;
    /// vx.close().await?;
    /// # Ok(()) }
    /// ```
    pub fn try_build(self) -> Result<Vertx> {
        self.build_with_pools(EventLoopPool::new, WorkerPool::new)
    }

    fn build_with_pools<E, W>(self, event_loops: E, workers: W) -> Result<Vertx>
    where
        E: FnOnce(usize, u64) -> Result<EventLoopPool>,
        W: FnOnce(usize, u64) -> Result<WorkerPool>,
    {
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
        let instance = NEXT_INSTANCE.fetch_add(1, Ordering::Relaxed);
        let el = self.event_loop_threads.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get().saturating_mul(2))
                .unwrap_or(2)
        });
        let event_loops = Arc::new(event_loops(el, instance)?);
        let workers = match workers(self.worker_threads, instance) {
            Ok(workers) => Arc::new(workers),
            Err(error) => {
                return Err(event_loops.rollback_initialization(error));
            }
        };
        let diagnostics = crate::diagnostics::Diagnostics::new(instance, self.diagnostics);
        let cancel = CancellationToken::new();
        let event_bus = EventBus::new(cancel.clone(), diagnostics.clone());
        let inner = Arc::new_cyclic(|weak| {
            let shared = Arc::new(ContextShared {
                diagnostics,
                event_bus,
                event_loops,
                workers,
                vertx: weak.clone(),
                exception_handler: std::sync::RwLock::new(None),
                cancel,
                blocking: TaskTracker::new(),
                shutdown_grace: self.shutdown_grace,
            });
            VertxInner {
                shared,
                deployments: Mutex::new(HashMap::new()),
                admission: Mutex::new(()),
                close_result: watch::channel(None).0,
            }
        });
        Ok(Vertx { inner })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::sync::atomic::AtomicUsize;

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
            "try_build must join startup threads before returning"
        );
    }
}
