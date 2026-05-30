use std::cell::RefCell;
use std::future::Future;
use std::sync::{Arc, Weak};

use tokio_util::sync::CancellationToken;

use crate::eventbus::{DeploymentScope, EventBus};
use crate::eventloop::{EventLoopId, EventLoopPool};
use crate::vertx::{Vertx, VertxInner};
use crate::worker_pool::WorkerPool;
use crate::{DeploymentId, Result, ThreadingModel, VertxError};

thread_local! {
    static CURRENT: RefCell<Option<Context>> = const { RefCell::new(None) };
}

pub(crate) struct ContextShared {
    pub event_bus: EventBus,
    pub event_loops: Arc<EventLoopPool>,
    pub workers: Arc<WorkerPool>,
    pub vertx: Weak<VertxInner>,
}

#[derive(Clone)]
enum Kind {
    EventLoop(EventLoopId),
    Worker,
}

/// The "where am I running" handle. Every Verticle / WorkerVerticle is started
/// inside a Context, and any code spawned by Vertx infrastructure carries one
/// in thread-local storage.
#[derive(Clone)]
pub struct Context {
    kind: Kind,
    inner: Arc<ContextShared>,
    scope: Option<Arc<DeploymentScope>>,
}

impl Context {
    pub(crate) fn new_event_loop(
        id: EventLoopId,
        inner: Arc<ContextShared>,
        scope: Option<Arc<DeploymentScope>>,
    ) -> Self {
        Self { kind: Kind::EventLoop(id), inner, scope }
    }

    pub(crate) fn new_worker(
        inner: Arc<ContextShared>,
        scope: Option<Arc<DeploymentScope>>,
    ) -> Self {
        Self { kind: Kind::Worker, inner, scope }
    }

    pub fn is_event_loop(&self) -> bool {
        matches!(self.kind, Kind::EventLoop(_))
    }

    pub fn is_worker(&self) -> bool {
        matches!(self.kind, Kind::Worker)
    }

    /// Vert.x parity: returns the threading model of this Context.
    pub fn threading_model(&self) -> ThreadingModel {
        match self.kind {
            Kind::EventLoop(_) => ThreadingModel::EventLoop,
            Kind::Worker => ThreadingModel::Worker,
        }
    }

    pub fn event_loop_id(&self) -> Option<EventLoopId> {
        match self.kind {
            Kind::EventLoop(id) => Some(id),
            Kind::Worker => None,
        }
    }

    /// The deployment id this Context is bound to, if any.
    /// Unbound Contexts (created from `Vertx::event_bus`-side helpers) return `None`.
    pub fn deployment_id(&self) -> Option<DeploymentId> {
        self.scope.as_ref().map(|s| s.deployment)
    }

    /// The `Vertx` instance that owns this Context.
    /// Returns `None` only if the Vertx has been fully dropped — should not
    /// happen while a Context produced by it is still in use.
    pub fn owner(&self) -> Option<Vertx> {
        self.inner.vertx.upgrade().map(Vertx::from_inner)
    }

    /// `true` if the calling thread is an EventLoop thread managed by Vertx.
    /// Mirrors `Context.isOnEventLoopThread()`.
    pub fn is_on_event_loop_thread() -> bool {
        CURRENT.with(|c| matches!(&*c.borrow(), Some(ctx) if ctx.is_event_loop()))
    }

    /// `true` if the calling thread is a Worker thread managed by Vertx.
    /// Mirrors `Context.isOnWorkerThread()`.
    pub fn is_on_worker_thread() -> bool {
        CURRENT.with(|c| matches!(&*c.borrow(), Some(ctx) if ctx.is_worker()))
    }

    /// `true` if the calling thread is either an EventLoop or a Worker thread.
    /// Mirrors `Context.isOnVertxThread()`.
    pub fn is_on_vertx_thread() -> bool {
        CURRENT.with(|c| c.borrow().is_some())
    }

    /// Returns an EventBus handle. When called inside a Verticle, the returned
    /// handle is **bound** to that verticle: consumers registered through it
    /// are auto-unregistered on undeploy.
    pub fn event_bus(&self) -> EventBus {
        match &self.scope {
            Some(scope) => self.inner.event_bus.bound(Arc::clone(scope)),
            None => self.inner.event_bus.clone(),
        }
    }

    /// The Context of the currently-running task, if any.
    /// Returns `None` when called from a thread Vertx didn't set up.
    pub fn current() -> Option<Context> {
        CURRENT.with(|c| c.borrow().clone())
    }

    pub(crate) fn enter(self) -> ContextGuard {
        let prev = CURRENT.with(|c| c.replace(Some(self)));
        ContextGuard { prev }
    }

    /// Returns true if this Context's deployment is still alive.
    /// Unbound contexts (from `Vertx::event_bus`) are always alive.
    pub fn is_alive(&self) -> bool {
        match &self.scope {
            Some(s) => s.is_alive(),
            None => true,
        }
    }

    /// Cancellation token tied to the verticle that owns this Context.
    /// Cooperative children of this verticle (e.g. user-spawned `spawn_local`
    /// tasks) should `select!` on `token.cancelled()` to terminate cleanly on
    /// undeploy. For unbound contexts, returns a never-cancelled token.
    pub fn cancellation_token(&self) -> CancellationToken {
        match &self.scope {
            Some(s) => s.cancel.clone(),
            None => CancellationToken::new(),
        }
    }

    fn ensure_alive(&self) -> Result<()> {
        match &self.scope {
            Some(s) if !s.is_alive() => {
                Err(VertxError::DeploymentTerminated(s.deployment))
            }
            _ => Ok(()),
        }
    }

    /// Schedule `f` to run asynchronously on this Context.
    /// Vert.x-aligned semantics: **fire-and-forget**. Returns as soon as the
    /// closure is enqueued. If you need the result, wire up your own oneshot
    /// inside the closure.
    ///
    /// Returns `DeploymentTerminated` if the underlying verticle has already
    /// been undeployed — schedules nothing in that case.
    pub fn run_on_context<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce() + Send + 'static,
    {
        self.ensure_alive()?;
        match self.kind {
            Kind::EventLoop(id) => {
                let inner = Arc::clone(&self.inner);
                let scope = self.scope.clone();
                let event_loops = Arc::clone(&inner.event_loops);
                event_loops.spawn_pinned(id, move || async move {
                    if let Some(s) = &scope {
                        if !s.is_alive() {
                            return;
                        }
                    }
                    let ctx = Context::new_event_loop(id, inner, scope);
                    let _g = ctx.enter();
                    f();
                });
            }
            Kind::Worker => {
                let inner = Arc::clone(&self.inner);
                let scope = self.scope.clone();
                let workers = Arc::clone(&inner.workers);
                workers.spawn(async move {
                    if let Some(s) = &scope {
                        if !s.is_alive() {
                            return;
                        }
                    }
                    let ctx = Context::new_worker(inner, scope);
                    let _g = ctx.enter();
                    f();
                });
            }
        }
        Ok(())
    }

    /// Run a blocking closure on the worker pool's blocking thread pool.
    /// Works whether the caller is on an EventLoop or on the Worker pool.
    ///
    /// Returns `DeploymentTerminated` if the underlying verticle has been
    /// undeployed before scheduling.
    pub async fn execute_blocking<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.ensure_alive()?;
        let handle = self.inner.workers.spawn_blocking(f);
        handle.await.map_err(VertxError::Join)
    }

    /// Create a one-shot timer **bound to this Context's deployment**.
    /// Undeploying the verticle will cancel the timer, causing in-flight
    /// awaits to resolve with `VertxError::TimerCancelled`.
    ///
    /// For an unbound timer that outlives any verticle, use `Vertx::timer`.
    pub fn timer(&self, delay: std::time::Duration) -> crate::Timer {
        let scope_cancel = self.scope.as_ref().map(|s| s.cancel.clone());
        crate::Timer::new(delay, scope_cancel)
    }

    /// Create a periodic timer **bound to this Context's deployment**.
    pub fn periodic(
        &self,
        initial_delay: std::time::Duration,
        period: std::time::Duration,
    ) -> crate::Periodic {
        let scope_cancel = self.scope.as_ref().map(|s| s.cancel.clone());
        crate::Periodic::new(initial_delay, period, scope_cancel)
    }

    /// Spawn a non-`Send` future on this Context's LocalSet.
    ///
    /// Only valid on an EventLoop Context — the LocalSet is what allows
    /// `!Send` futures to run. Worker contexts panic; use the worker pool's
    /// own task spawner there.
    ///
    /// This is the safe way for Verticles to start child tasks: it preserves
    /// the EventLoop affinity contract so the future can borrow `Rc` /
    /// `RefCell` state from the Verticle.
    pub fn spawn_local<F>(&self, fut: F)
    where
        F: Future<Output = ()> + 'static,
    {
        match self.kind {
            Kind::EventLoop(_) => {
                tokio::task::spawn_local(fut);
            }
            Kind::Worker => {
                panic!("Context::spawn_local() can only be called on an EventLoop context");
            }
        }
    }
}

/// RAII guard restoring the previous Context on drop. Internal use only.
pub(crate) struct ContextGuard {
    prev: Option<Context>,
}

impl Drop for ContextGuard {
    fn drop(&mut self) {
        let prev = self.prev.take();
        CURRENT.with(|c| *c.borrow_mut() = prev);
    }
}
