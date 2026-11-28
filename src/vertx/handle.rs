use super::{Vertx, VertxInner};
use crate::context::Context;
use crate::scope::WorkScope;
use crate::{
    Deployment, EventBus, ExecutionInfo, Periodic, Result, TaskHandle, TcpConnection, TcpIncoming,
    TcpServer, TcpServerOptions, Timer, VertxError,
};
use std::future::Future;
use std::mem::DropGuard;
use std::net::SocketAddr;
use std::sync::{Arc, Weak};
use std::time::Duration;

/// A cloneable weak handle for business operations on a Vertx instance.
///
/// Managed calls inherit the current execution when it belongs to this
/// instance. Calls from outside that execution use the instance's lifetime.
/// The handle can be retained by tasks without keeping their runtime alive.
#[derive(Clone, Debug)]
pub struct VertxHandle {
    inner: Weak<VertxInner>,
}

impl VertxHandle {
    pub(crate) fn new(inner: Weak<VertxInner>) -> Self {
        Self { inner }
    }

    fn instance(&self) -> Result<Arc<VertxInner>> {
        self.inner.upgrade().ok_or(VertxError::VertxClosed)
    }

    pub(super) fn current_context(&self) -> Option<Context> {
        Context::current().filter(|ctx| ctx.belongs_to(&self.inner))
    }

    fn execution_context(&self) -> Result<Context> {
        if let Some(ctx) = self.current_context() {
            return Ok(ctx);
        }
        let instance = self.instance()?;
        let id = instance.executors.event_loops.pick();
        Ok(Context::new_event_loop(id, &instance, None))
    }

    fn local_context(&self) -> Result<Context> {
        if let Some(ctx) = self.current_context() {
            ctx.ensure_current_event_loop_context()?;
            return Ok(ctx);
        }
        self.instance()?.ensure_open()?;
        Err(VertxError::ContextMismatch)
    }

    fn work_scope(&self) -> Result<Arc<WorkScope>> {
        if let Some(ctx) = self.current_context() {
            return Ok(ctx.scope().clone());
        }
        Ok(self.instance()?.lifetime.scope.clone())
    }

    /// Returns the instance's bus. Consumer ownership is decided at registration.
    /// Returns `VertxClosed` once the instance has closed business admission.
    pub fn event_bus(&self) -> Result<EventBus> {
        let instance = self.instance()?;
        instance.ensure_open()?;
        Ok(instance.services.event_bus.clone())
    }

    /// Queues a Send future for managed execution and returns its completion handle.
    ///
    /// Within this instance's execution, the task uses the same Context and
    /// executor. External callers use an EventLoop assigned to this task.
    /// Dropping the task handle detaches the waiter.
    ///
    /// ```compile_fail,E0277
    /// use std::rc::Rc;
    /// use vertx_rs::VertxHandle;
    ///
    /// fn dispatch(vertx: &VertxHandle) {
    ///     let local = Rc::new(42);
    ///     let _ = vertx.spawn(async move { Ok(*local) });
    /// }
    /// ```
    pub fn spawn<F, T>(&self, future: F) -> Result<TaskHandle<T>>
    where
        F: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        self.execution_context()?.spawn(future)
    }

    /// Queues a local future on the current EventLoop of this instance.
    /// The future and its result may hold non-Send state.
    ///
    /// ```
    /// use std::rc::Rc;
    /// use vertx_rs::{Result, Verticle, VertxHandle};
    ///
    /// struct Service;
    /// impl Verticle for Service {
    ///     async fn start(&mut self, vertx: VertxHandle) -> Result<()> {
    ///         let local = Rc::new(42);
    ///         let task = vertx.spawn_local(async move {
    ///             tokio::task::yield_now().await;
    ///             Ok(local)
    ///         })?;
    ///         assert_eq!(*task.await?, 42);
    ///         Ok(())
    ///     }
    /// }
    /// ```
    pub fn spawn_local<F, T>(&self, future: F) -> Result<TaskHandle<T>>
    where
        F: Future<Output = Result<T>> + 'static,
        T: 'static,
    {
        self.local_context()?.spawn_local(future)
    }

    /// Creates a timer with a deadline measured from this call. Stopping the
    /// owning scope cancels it. Polling requires a Tokio time driver.
    pub fn timer(&self, delay: Duration) -> Result<Timer> {
        let scope = self.work_scope()?;
        scope.ensure_open()?;
        Timer::new(delay, scope.cancel.clone())
    }

    /// Creates a periodic timer with Skip policy and an initial deadline
    /// measured from this call. The period must be positive.
    pub fn periodic(&self, initial_delay: Duration, period: Duration) -> Result<Periodic> {
        let scope = self.work_scope()?;
        scope.ensure_open()?;
        Periodic::new(initial_delay, period, scope.cancel.clone())
    }

    /// Runs a blocking closure on the shared Worker pool, FIFO per source Context.
    /// Independent external calls each have their own source Context.
    /// Already-started closures run to completion even when the waiter is dropped.
    pub fn execute_blocking<F, T>(
        &self,
        operation: F,
    ) -> impl Future<Output = Result<T>> + Send + 'static + use<F, T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let ctx = self.execution_context();
        async move { ctx?.execute_blocking(operation).await }
    }

    /// Runs a blocking closure without taking the source Context's FIFO slot.
    /// Parallel execution is bounded by the shared Worker thread count.
    pub fn execute_blocking_unordered<F, T>(
        &self,
        operation: F,
    ) -> impl Future<Output = Result<T>> + Send + 'static + use<F, T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let ctx = self.execution_context();
        async move { ctx?.execute_blocking_unordered(operation).await }
    }

    /// Returns a read-only signal for stopping the current deployment, or the
    /// instance when called from outside a deployment. The signal can be moved
    /// into a task to wait for shutdown without retaining the instance.
    pub fn stopping(&self) -> Result<impl Future<Output = ()> + Send + 'static + use<>> {
        let token = self.work_scope()?.cancel.clone();
        Ok(token.cancelled_owned())
    }

    /// Returns execution metadata for a managed operation of this instance.
    /// Returns `None` outside such execution, including inside raw Tokio tasks.
    pub fn current_execution(&self) -> Option<ExecutionInfo> {
        self.current_context()?.execution_info()
    }

    /// Returns the current deployment's handle when called from one of its
    /// managed operations. Instance-level work has no deployment.
    pub fn current_deployment(&self) -> Option<Deployment> {
        self.current_context()?.deployment()
    }

    /// Starts a managed TCP service on an EventLoop. The async handler crosses
    /// threads as a `Send` value and is invoked on the selected EventLoop.
    /// Dropping the startup future before it returns cancels unclaimed startup.
    ///
    /// ```compile_fail,E0277
    /// use std::{net::SocketAddr, rc::Rc};
    /// use vertx_rs::{TcpConnection, TcpServerOptions, VertxHandle};
    ///
    /// fn start(vertx: &VertxHandle, addr: SocketAddr) {
    ///     let local = Rc::new(42);
    ///     let _ = vertx.serve_tcp(addr, TcpServerOptions::default(), async move |_: TcpConnection<'_>| {
    ///         assert_eq!(*local, 42);
    ///         Ok(())
    ///     });
    /// }
    /// ```
    pub fn serve_tcp<F>(
        &self,
        addr: SocketAddr,
        options: TcpServerOptions,
        handler: F,
    ) -> impl Future<Output = Result<TcpServer>> + Send + 'static + use<F>
    where
        F: for<'a> AsyncFn(TcpConnection<'a>) -> Result<()> + Send + 'static,
    {
        let ctx = self.execution_context();
        async move {
            let ctx = ctx?;
            let owner = ctx.clone();
            let task = ctx.spawn_pinned(move || async move {
                Ok(owner
                    .serve_tcp(addr, options, handler)
                    .await
                    .map(startup_ticket))
            })?;
            let cancel = DropGuard::new(task.abort_handle(), |task| task.abort());
            let result = task.await;
            drop(DropGuard::dismiss(cancel));
            Ok(DropGuard::dismiss(result??))
        }
    }

    /// Starts a managed TCP service on the current EventLoop.
    /// The handler may capture non-Send state.
    pub fn serve_tcp_local<F>(
        &self,
        addr: SocketAddr,
        options: TcpServerOptions,
        handler: F,
    ) -> impl Future<Output = Result<TcpServer>> + 'static + use<F>
    where
        F: for<'a> AsyncFn(TcpConnection<'a>) -> Result<()> + 'static,
    {
        let ctx = self.local_context();
        async move { ctx?.serve_tcp(addr, options, handler).await }
    }

    /// Registers a raw shared listener on the current EventLoop. Accept
    /// connections by polling the returned listener in the same execution.
    pub fn listen_tcp(
        &self,
        addr: SocketAddr,
    ) -> impl Future<Output = Result<TcpIncoming>> + 'static + use<> {
        let ctx = self.local_context();
        async move { ctx?.listen_tcp(addr).await }
    }
}

fn startup_ticket(server: TcpServer) -> DropGuard<TcpServer, impl FnOnce(TcpServer)> {
    DropGuard::new(server, |server| server.request_stop())
}

impl From<&Vertx> for VertxHandle {
    fn from(vertx: &Vertx) -> Self {
        vertx.handle()
    }
}

impl From<&VertxHandle> for VertxHandle {
    fn from(handle: &VertxHandle) -> Self {
        handle.clone()
    }
}

impl Vertx {
    /// Returns the instance's EventBus. See [`VertxHandle::event_bus`].
    pub fn event_bus(&self) -> Result<EventBus> {
        self.handle().event_bus()
    }

    /// Queues a managed Send future. See [`VertxHandle::spawn`].
    pub fn spawn<F, T>(&self, future: F) -> Result<TaskHandle<T>>
    where
        F: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        self.handle().spawn(future)
    }

    /// Queues a future on the current EventLoop. See [`VertxHandle::spawn_local`].
    pub fn spawn_local<F, T>(&self, future: F) -> Result<TaskHandle<T>>
    where
        F: Future<Output = Result<T>> + 'static,
        T: 'static,
    {
        self.handle().spawn_local(future)
    }

    /// Creates a timer. See [`VertxHandle::timer`].
    pub fn timer(&self, delay: Duration) -> Result<Timer> {
        self.handle().timer(delay)
    }

    /// Creates a periodic timer. See [`VertxHandle::periodic`].
    pub fn periodic(&self, initial_delay: Duration, period: Duration) -> Result<Periodic> {
        self.handle().periodic(initial_delay, period)
    }

    /// Runs a blocking closure. See [`VertxHandle::execute_blocking`].
    pub fn execute_blocking<F, T>(
        &self,
        operation: F,
    ) -> impl Future<Output = Result<T>> + Send + 'static + use<F, T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.handle().execute_blocking(operation)
    }

    /// Runs an unordered blocking closure. See [`VertxHandle::execute_blocking_unordered`].
    pub fn execute_blocking_unordered<F, T>(
        &self,
        operation: F,
    ) -> impl Future<Output = Result<T>> + Send + 'static + use<F, T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.handle().execute_blocking_unordered(operation)
    }

    /// Returns the current scope's stop signal. See [`VertxHandle::stopping`].
    pub fn stopping(&self) -> Result<impl Future<Output = ()> + Send + 'static + use<>> {
        self.handle().stopping()
    }

    /// Returns current execution metadata. See [`VertxHandle::current_execution`].
    pub fn current_execution(&self) -> Option<ExecutionInfo> {
        self.handle().current_execution()
    }

    /// Returns the current deployment's handle. See [`VertxHandle::current_deployment`].
    pub fn current_deployment(&self) -> Option<Deployment> {
        self.handle().current_deployment()
    }

    /// Starts a managed TCP service. See [`VertxHandle::serve_tcp`].
    pub fn serve_tcp<F>(
        &self,
        addr: SocketAddr,
        options: TcpServerOptions,
        handler: F,
    ) -> impl Future<Output = Result<TcpServer>> + Send + 'static + use<F>
    where
        F: for<'a> AsyncFn(TcpConnection<'a>) -> Result<()> + Send + 'static,
    {
        self.handle().serve_tcp(addr, options, handler)
    }

    /// Starts a local TCP service. See [`VertxHandle::serve_tcp_local`].
    pub fn serve_tcp_local<F>(
        &self,
        addr: SocketAddr,
        options: TcpServerOptions,
        handler: F,
    ) -> impl Future<Output = Result<TcpServer>> + 'static + use<F>
    where
        F: for<'a> AsyncFn(TcpConnection<'a>) -> Result<()> + 'static,
    {
        self.handle().serve_tcp_local(addr, options, handler)
    }

    /// Registers a raw listener. See [`VertxHandle::listen_tcp`].
    pub fn listen_tcp(
        &self,
        addr: SocketAddr,
    ) -> impl Future<Output = Result<TcpIncoming>> + 'static + use<> {
        self.handle().listen_tcp(addr)
    }
}

#[cfg(test)]
mod tests {
    use super::{TcpConnection, TcpServerOptions, Vertx, startup_ticket};
    use std::time::Duration;

    #[tokio::test]
    async fn dropping_an_unclaimed_startup_result_releases_the_bound_listener() {
        let vertx = Vertx::builder()
            .event_loop_threads(1)
            .worker_threads(1)
            .build()
            .unwrap();
        let handle = vertx.handle();
        let (ticket, server) = vertx
            .spawn(async move {
                let server = handle
                    .serve_tcp_local(
                        "127.0.0.1:0".parse().unwrap(),
                        TcpServerOptions::default(),
                        async |_: TcpConnection<'_>| Ok(()),
                    )
                    .await?;
                Ok((startup_ticket(server.clone()), server))
            })
            .unwrap()
            .await
            .unwrap();

        let addr = server.local_addr();
        assert!(std::net::TcpListener::bind(addr).is_err());
        drop(ticket);
        tokio::time::timeout(Duration::from_secs(3), server.wait_closed())
            .await
            .unwrap()
            .unwrap();
        drop(std::net::TcpListener::bind(addr).unwrap());
        vertx.close().await.unwrap();
    }
}
