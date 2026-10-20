use crate::context::{Context, ContextShared};
use crate::deployment::{CancelOnDrop, DeploymentScope};
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
        .deployment(ctx.scope.deployment, "quiescing");
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
        .deployment(ctx.scope.deployment, "stopped");
}

// Generate separate drivers so worker futures retain their Send bound while
// EventLoop instances and their lifecycle futures can remain non-Send.
macro_rules! lifecycle_driver {
    ($name:ident, $trait:ident) => {
        async fn $name<V, F>(ctx: Context, factory: F, ready: oneshot::Sender<Result<()>>)
        where V: $trait, F: FnOnce(Context) -> V {
            ctx.inner.diagnostics.deployment(ctx.scope.deployment, "starting");
            let mut ready = Some(ready);
            let scope = ctx.scope.clone();
            let created = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| factory(ctx.clone())));
            let mut outcome = Ok(());
            let mut startup_failure = None;
            match created {
                Err(e) => { outcome = Err(panic_error(e)); }
                Ok(mut v) => {
                    let (start, panicked) = tokio::select! { biased;
                        _ = scope.cancel.cancelled() => (Err(VertxError::DeploymentTerminated(scope.deployment)), false),
                        result = std::panic::AssertUnwindSafe(v.start(ctx.clone())).catch_unwind() => match result {
                            Ok(result) => (result, false),
                            Err(error) => (Err(panic_error(error)), true),
                        },
                    };
                    if start.is_ok() && scope.running() {
                        ctx.inner.diagnostics.deployment(scope.deployment, "running");
                        if ready.take().expect("readiness sent only once").send(Ok(())).is_err() { scope.request_stop(); }
                        scope.cancel.cancelled().await;
                    } else {
                        let error = start.err().unwrap_or(VertxError::DeploymentTerminated(scope.deployment));
                        // Cancelling startup is expected shutdown, but the deploy
                        // waiter must still learn that readiness was never reached.
                        if !matches!(error, VertxError::DeploymentTerminated(_)) { outcome = Err(error.clone()); }
                        startup_failure = Some(error);
                    }
                    quiesce_deployment(&ctx).await;
                    if !panicked {
                        ctx.inner.diagnostics.deployment(scope.deployment, "stopping");
                        let stopped = std::panic::AssertUnwindSafe(ctx.for_stop().scope_future(v.stop(ctx.for_stop()))).catch_unwind().await
                            .unwrap_or_else(|e| Err(panic_error(e)));
                        if let Err(e) = stopped { retain_failure(&mut outcome, e); }
                    }
                    if let Err(e) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(v))) {
                        retain_failure(&mut outcome, panic_error(e));
                    }
                }
            }
            quiesce_deployment(&ctx).await;
            finish_deployment(&ctx, outcome, startup_failure, ready);
        }
    }
}
lifecycle_driver!(drive_local, Verticle);
lifecycle_driver!(drive_worker, WorkerVerticle);

impl Vertx {
    pub(crate) fn from_inner(inner: Arc<VertxInner>) -> Self {
        Self { inner }
    }
    pub fn new() -> Self {
        Self::builder().build()
    }
    pub fn builder() -> VertxBuilder {
        VertxBuilder::default()
    }
    pub fn diagnostics(&self) -> crate::DiagnosticsSnapshot {
        let mut snapshot = self.inner.shared.diagnostics.snapshot();
        if snapshot.enabled {
            if let Some(result) = self.inner.close_result.borrow().as_ref() {
                snapshot.close_stage = if result.is_ok() { "closed" } else { "failed" };
            }
        }
        snapshot
    }
    pub fn event_bus(&self) -> EventBus {
        self.inner.shared.event_bus.clone()
    }
    pub fn event_loop_count(&self) -> usize {
        self.inner.shared.event_loops.size()
    }
    pub fn worker_thread_count(&self) -> usize {
        self.inner.shared.workers.threads()
    }
    pub fn is_closed(&self) -> bool {
        self.inner.shared.cancel.is_cancelled()
    }
    pub fn timer(&self, delay: Duration) -> Result<crate::Timer> {
        self.event_bus().ensure_open()?;
        Ok(crate::Timer::new(delay, self.inner.shared.cancel.clone()))
    }
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
            self.inner.shared.diagnostics.deployment(id, "queued");
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
                self.inner.shared.diagnostics.deployment(id, "stopped");
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
            self.inner.shared.diagnostics.deployment(id, "queued");
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
                self.inner.shared.diagnostics.deployment(id, "stopped");
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
                owner.shared.diagnostics.close_stage("deployments");
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
                        owner.shared.diagnostics.close_stage("blocking");
                        owner.shared.blocking.wait().await;
                        owner.shared.exception_handler.write().unwrap().take();
                        owner.shared.diagnostics.close_stage("worker_join");
                        errors.extend(owner.shared.workers.shutdown().await);
                        owner.shared.diagnostics.close_stage("event_loop_join");
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
    pub fn diagnostics(mut self, options: crate::DiagnosticsOptions) -> Self {
        self.diagnostics = options;
        self
    }
    pub fn event_loop_threads(mut self, n: usize) -> Self {
        self.event_loop_threads = Some(n);
        self
    }
    pub fn worker_threads(mut self, n: usize) -> Self {
        self.worker_threads = n;
        self
    }
    pub fn shutdown_grace(mut self, d: Duration) -> Self {
        self.shutdown_grace = d;
        self
    }
    pub fn build(self) -> Vertx {
        assert!(
            !self.shutdown_grace.is_zero(),
            "shutdown grace must be positive"
        );
        let instance = NEXT_INSTANCE.fetch_add(1, Ordering::Relaxed);
        let el = self.event_loop_threads.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get().saturating_mul(2))
                .unwrap_or(2)
        });
        let event_loops =
            Arc::new(EventLoopPool::new(el, instance).expect("failed to initialize event loops"));
        let workers = Arc::new(
            WorkerPool::new(self.worker_threads, instance)
                .expect("failed to initialize worker pool"),
        );
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
        Vertx { inner }
    }
}
