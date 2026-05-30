use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::context::{Context, ContextShared};
use crate::eventbus::{DeploymentScope, EventBus};
use crate::eventloop::EventLoopPool;
use crate::worker_pool::WorkerPool;
use crate::{DeploymentId, Result, Verticle, VertxError, WorkerVerticle};

const DEFAULT_WORKER_THREADS: usize = 20;
/// How long `undeploy` waits for `run()` to exit naturally on cancellation
/// before aborting the task.
const UNDEPLOY_GRACE: Duration = Duration::from_secs(1);

pub(crate) struct Deployment {
    join: JoinHandle<()>,
    scope: Arc<DeploymentScope>,
}

pub(crate) struct VertxInner {
    pub shared: Arc<ContextShared>,
    pub(crate) deployments: DashMap<DeploymentId, Deployment>,
}

/// Top-level handle. Cheap to clone.
#[derive(Clone)]
pub struct Vertx {
    pub(crate) inner: Arc<VertxInner>,
}

impl Vertx {
    pub(crate) fn from_inner(inner: Arc<VertxInner>) -> Self {
        Self { inner }
    }

    /// Defaults: EventLoop pool = `available_parallelism`, Worker pool = 20.
    pub fn new() -> Self {
        Self::builder().build()
    }

    pub fn builder() -> VertxBuilder {
        VertxBuilder::default()
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

    pub fn timer(&self, delay: Duration) -> crate::Timer {
        crate::Timer::new(delay, None)
    }

    pub fn periodic(&self, initial_delay: Duration, period: Duration) -> crate::Periodic {
        crate::Periodic::new(initial_delay, period, None)
    }

    /// Install a global exception handler. Framework components that would
    /// otherwise swallow an error (e.g. `Consumer::handler`'s per-message
    /// loop) will forward errors here. Mirrors `Vertx.exceptionHandler(...)`.
    ///
    /// Calling this replaces any previously-installed handler. Pass `None`
    /// (via `clear_exception_handler`) to remove it.
    pub fn set_exception_handler<F>(&self, handler: F)
    where
        F: Fn(&VertxError) + Send + Sync + 'static,
    {
        if let Ok(mut guard) = self.inner.shared.exception_handler.write() {
            *guard = Some(std::sync::Arc::new(handler));
        }
    }

    pub fn clear_exception_handler(&self) {
        if let Ok(mut guard) = self.inner.shared.exception_handler.write() {
            *guard = None;
        }
    }

    /// Deploy a Verticle on an EventLoop chosen round-robin.
    ///
    /// `factory` runs **synchronously on the target EventLoop thread** with
    /// the deployment's `Context`. Use it to register consumers and stash
    /// any other initialization that must complete before `run` starts.
    /// `deploy` returns as soon as `factory` returns — at that point the
    /// verticle is "ready" (consumers registered, etc.) and `run` is
    /// scheduled to take over.
    pub async fn deploy<F, V>(&self, factory: F) -> Result<DeploymentId>
    where
        F: FnOnce(Context) -> V + Send + 'static,
        V: Verticle,
    {
        let loop_id = self.inner.shared.event_loops.pick();
        let dep_id = DeploymentId::next();
        let cancel = CancellationToken::new();
        let scope = Arc::new(DeploymentScope::new(dep_id, cancel.clone()));

        let shared = Arc::clone(&self.inner.shared);
        let event_loops = Arc::clone(&shared.event_loops);

        let (ready_tx, ready_rx) = oneshot::channel::<()>();
        let scope_for_task = Arc::clone(&scope);

        let join = event_loops.spawn_pinned(loop_id, move || async move {
            let ctx = Context::new_event_loop(loop_id, shared, Some(scope_for_task));
            let _g = ctx.clone().enter();

            // Synchronous init phase: ctor + sync consumer registration.
            let v = factory(ctx.clone());
            // Signal "ready" so deploy() can return. We do this BEFORE run()
            // is polled — but factory has already had a chance to register
            // consumers on the bound EventBus, so external sends are safe.
            let _ = ready_tx.send(());

            if let Err(e) = v.run(ctx).await {
                tracing::error!(error = %e, "verticle run failed");
            }
        });

        match ready_rx.await {
            Ok(()) => {
                self.inner
                    .deployments
                    .insert(dep_id, Deployment { join, scope });
                Ok(dep_id)
            }
            Err(_) => Err(VertxError::StartFailed(
                "verticle factory panicked before signaling ready".into(),
            )),
        }
    }

    /// Multi-instance deploy with `DeploymentOptions`. `factory` is called
    /// once per instance (on its own EventLoop, picked round-robin) with a
    /// shared `Arc<C>` config. Returns the `DeploymentId`s in deploy order.
    ///
    /// Mirrors `Vertx.deployVerticle(supplier, DeploymentOptions)` with
    /// `setInstances(n).setConfig(c)`.
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
        let n = options.instances_count();
        let cfg = options.shared_config();
        let factory = Arc::new(factory);
        let mut ids = Vec::with_capacity(n);
        for _ in 0..n {
            let cfg = Arc::clone(&cfg);
            let factory = Arc::clone(&factory);
            let id = self
                .deploy(move |ctx| factory(ctx, cfg))
                .await?;
            ids.push(id);
        }
        Ok(ids)
    }

    /// Deploy a `WorkerVerticle` on the worker pool.
    pub async fn deploy_worker<V>(&self, v: V) -> Result<DeploymentId>
    where
        V: WorkerVerticle,
    {
        let dep_id = DeploymentId::next();
        let cancel = CancellationToken::new();
        let scope = Arc::new(DeploymentScope::new(dep_id, cancel.clone()));
        let shared = Arc::clone(&self.inner.shared);

        let run_ctx = Context::new_worker(Arc::clone(&shared), Some(Arc::clone(&scope)));
        let join = shared.workers.spawn({
            let run_ctx = run_ctx.clone();
            async move {
                let _g = run_ctx.clone().enter();
                if let Err(e) = v.run(run_ctx).await {
                    tracing::error!(error = %e, "worker verticle run failed");
                }
            }
        });

        self.inner
            .deployments
            .insert(dep_id, Deployment { join, scope });
        Ok(dep_id)
    }

    pub async fn undeploy(&self, id: DeploymentId) -> Result<()> {
        let (_, dep) = self
            .inner
            .deployments
            .remove(&id)
            .ok_or(VertxError::DeploymentNotFound(id))?;
        // 1. Reject any new scheduling on this Context.
        dep.scope.mark_dead();
        // 2. Wake bus consumer recv-loops by dropping their senders.
        self.inner.shared.event_bus.cleanup_scope(&dep.scope);
        // 3. Cooperative cancel: well-behaved run() selects on this.
        dep.scope.cancel.cancel();
        // 4. Wait for run() to exit naturally, with a grace period; if the
        //    verticle ignores cancellation we abort.
        let abort_handle = dep.join.abort_handle();
        match tokio::time::timeout(UNDEPLOY_GRACE, dep.join).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) if e.is_cancelled() => Ok(()),
            Ok(Err(e)) => Err(VertxError::Join(e)),
            Err(_) => {
                tracing::warn!(%id, "verticle did not honor cancellation; aborting");
                abort_handle.abort();
                Ok(())
            }
        }
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
}

impl Default for VertxBuilder {
    fn default() -> Self {
        Self {
            event_loop_threads: None,
            worker_threads: DEFAULT_WORKER_THREADS,
        }
    }
}

impl VertxBuilder {
    pub fn event_loop_threads(mut self, n: usize) -> Self {
        self.event_loop_threads = Some(n);
        self
    }

    pub fn worker_threads(mut self, n: usize) -> Self {
        self.worker_threads = n;
        self
    }

    pub fn build(self) -> Vertx {
        let el = self.event_loop_threads.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        });
        let event_loops = Arc::new(EventLoopPool::new(el));
        let workers = Arc::new(
            WorkerPool::new(self.worker_threads).expect("failed to init worker pool"),
        );
        let event_bus = EventBus::new();
        let inner = Arc::new_cyclic(|weak| {
            let shared = Arc::new(ContextShared {
                event_bus,
                event_loops,
                workers,
                vertx: weak.clone(),
                exception_handler: std::sync::RwLock::new(None),
            });
            VertxInner {
                shared,
                deployments: DashMap::new(),
            }
        });
        Vertx { inner }
    }
}
