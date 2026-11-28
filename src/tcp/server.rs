use super::listener::{PortBudget, PortPermit, TcpIncoming, listen_managed};
use crate::context::Context;
use crate::task::ActiveTasks;
use crate::{ContextId, Result, TaskAbortHandle, TaskId, VertxError};
use std::cell::RefCell;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::ops::AsyncFn;
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::sync::{Arc, OnceLock};
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;
use tokio::io::{self, AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::{TaskTracker, task_tracker::TaskTrackerToken};

/// Options shared by all managed services on one bound address.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpServerOptions {
    /// Connection admission budget shared by managed services on the address.
    pub max_connections: NonZeroUsize,
}

impl Default for TcpServerOptions {
    fn default() -> Self {
        Self {
            max_connections: NonZeroUsize::new(1024).unwrap(),
        }
    }
}

/// Borrowed I/O for one managed connection. The serving task owns the socket.
///
/// ```compile_fail
/// use vertx_rs::TcpConnection;
///
/// fn detach(connection: TcpConnection<'_>) {
///     tokio::spawn(async move { drop(connection) });
/// }
/// ```
pub struct TcpConnection<'a> {
    socket: &'a mut TcpStream,
    peer: SocketAddr,
    stop: CancellationToken,
}

impl TcpConnection<'_> {
    /// Returns the remote address recorded on accept.
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer
    }

    /// Signals that the service is draining. Cancelling this child token
    /// only affects this connection.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.stop.clone()
    }
}

impl AsyncRead for TcpConnection<'_> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.get_mut().socket).poll_read(cx, buf)
    }
}

impl AsyncWrite for TcpConnection<'_> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.get_mut().socket).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.get_mut().socket).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.get_mut().socket).poll_shutdown(cx)
    }

    fn is_write_vectored(&self) -> bool {
        self.socket.is_write_vectored()
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.get_mut().socket).poll_write_vectored(cx, bufs)
    }
}

/// A handle to a managed TCP service. Dropping a handle detaches it.
#[derive(Clone)]
pub struct TcpServer {
    control: Arc<Control>,
}

impl TcpServer {
    pub(crate) fn request_stop(&self) {
        self.control.stop.cancel();
    }

    /// Returns the bound address, including any OS-assigned port.
    pub fn local_addr(&self) -> SocketAddr {
        self.control.addr
    }

    /// Stops accepting and asks in-flight connections to finish without waiting.
    /// The first successful request sets the drain deadline. This can be called
    /// in the service's owning Context. Await cleanup with [`Self::wait_closed`]
    /// from another Context or from outside the runtime.
    ///
    /// Returns [`VertxError::InvalidOptions`] if the initial deadline cannot be
    /// represented. A zero grace requests immediate cancellation of remaining work.
    pub fn request_shutdown(&self, grace: Duration) -> Result<()> {
        self.control.request_shutdown(grace).map(|_| ())
    }

    /// Requests shutdown and waits until the drain deadline set by the first
    /// successful request. Cancelling a waiter leaves cleanup running.
    /// A timeout requests abort; use [`Self::wait_closed`] to await actual cleanup.
    /// The owning Context may use [`Self::request_shutdown`] instead of waiting.
    pub async fn shutdown(&self, grace: Duration) -> Result<()> {
        self.check_waiter()?;
        let drain = self.control.request_shutdown(grace)?;
        match tokio::time::timeout_at(drain.deadline, self.wait_closed()).await {
            Ok(outcome) => {
                outcome?;
                if self.control.force.is_cancelled() {
                    Err(VertxError::ShutdownTimeout(drain.grace))
                } else {
                    Ok(())
                }
            }
            Err(_) => {
                self.control.force.cancel();
                Err(VertxError::ShutdownTimeout(drain.grace))
            }
        }
    }

    /// Waits for the listener registration and the connections to be cleaned up.
    /// Waiting in the owning Context returns [`VertxError::WouldDeadlock`].
    pub async fn wait_closed(&self) -> Result<()> {
        self.check_waiter()?;
        let mut result = self.control.done.subscribe();
        loop {
            if let Some(outcome) = result.borrow().as_ref() {
                return outcome.clone();
            }
            result
                .changed()
                .await
                .map_err(|_| VertxError::VertxClosed)?;
        }
    }

    fn check_waiter(&self) -> Result<()> {
        if Context::current().is_some_and(|ctx| ctx.id() == self.control.owner) {
            Err(VertxError::WouldDeadlock)
        } else {
            Ok(())
        }
    }
}

impl Context {
    /// Starts a TCP service on this ordinary Verticle's EventLoop.
    ///
    /// The async handler may capture non-Send state and borrows each connection.
    /// Services on the same address share a single limit and must use identical
    /// options. The owning scope tracks the service and every connection task.
    pub(crate) async fn serve_tcp<F>(
        &self,
        addr: SocketAddr,
        options: TcpServerOptions,
        handler: F,
    ) -> Result<TcpServer>
    where
        F: for<'a> AsyncFn(TcpConnection<'a>) -> Result<()> + 'static,
    {
        let (incoming, budget) = listen_managed(self, addr, options.max_connections).await?;
        let control = Arc::new(Control {
            addr: incoming.local_addr(),
            owner: self.id(),
            drain: OnceLock::new(),
            stop: CancellationToken::new(),
            force: CancellationToken::new(),
            done: watch::channel(None).0,
        });
        let task = ServerTask {
            incoming: Some(incoming),
            handler: Rc::new(handler),
            connections: TaskTracker::new(),
            state: Rc::new(ServerState {
                control: control.clone(),
                children: RefCell::default(),
                outcome: RefCell::new(Ok(())),
            }),
        };
        self.spawn_local(run_server(self.clone(), task, budget))?;
        Ok(TcpServer { control })
    }
}

struct ConnectionPermit {
    _port: PortPermit,
    _tracked: TaskTrackerToken,
    registration: Option<ConnectionRegistration>,
    _state: Rc<ServerState>,
}

struct ConnectionRegistration {
    state: Weak<ServerState>,
    id: TaskId,
}

impl Drop for ConnectionRegistration {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            let handle = state.children.borrow_mut().remove(self.id);
            drop(handle);
        }
    }
}

struct Control {
    owner: ContextId,
    addr: SocketAddr,
    drain: OnceLock<Drain>,
    stop: CancellationToken,
    force: CancellationToken,
    done: watch::Sender<Option<Result<()>>>,
}

#[derive(Clone, Copy)]
struct Drain {
    deadline: Instant,
    grace: Duration,
}

impl Control {
    fn request_shutdown(&self, grace: Duration) -> Result<Drain> {
        let drain = match self.drain.get() {
            Some(drain) => *drain,
            None => {
                let deadline = Instant::now().checked_add(grace).ok_or_else(|| {
                    VertxError::InvalidOptions("TCP shutdown deadline is out of range".into())
                })?;
                *self.drain.get_or_init(|| Drain { deadline, grace })
            }
        };
        self.stop.cancel();
        Ok(drain)
    }
}

// The accept loop and connection tasks run on this server's EventLoop. A
// cross-thread shutdown can signal the loop through Control.
struct ServerState {
    control: Arc<Control>,
    children: RefCell<ActiveTasks>,
    outcome: RefCell<Result<()>>,
}

impl ServerState {
    fn register(self: &Rc<Self>, id: TaskId) -> Option<ConnectionRegistration> {
        if self.control.stop.is_cancelled() || self.control.drain.get().is_some() {
            return None;
        }
        self.children.borrow_mut().register(id);
        Some(ConnectionRegistration {
            state: Rc::downgrade(self),
            id,
        })
    }

    fn attach(&self, id: TaskId, task: TaskAbortHandle) {
        let abort = self.children.borrow_mut().attach(id, &task);
        if abort {
            task.abort();
        }
    }

    fn abort_connections(&self) {
        self.control.force.cancel();
        let children = self.children.borrow_mut().take_abort_handles();
        for child in children {
            child.abort();
        }
    }
}

impl Drop for ServerState {
    fn drop(&mut self) {
        self.control
            .done
            .send_replace(Some(self.outcome.get_mut().clone()));
    }
}

struct ServerTask<F> {
    incoming: Option<TcpIncoming>,
    handler: Rc<F>,
    connections: TaskTracker,
    state: Rc<ServerState>,
}

struct ConnectionTask<F> {
    handler: Rc<F>,
    accepted: Accepted,
    stop: CancellationToken,
}

// The socket must drop before the capacity and completion guards.
struct Accepted {
    socket: TcpStream,
    peer: SocketAddr,
    _permit: ConnectionPermit,
}

async fn handle<F>(mut task: ConnectionTask<F>) -> Result<()>
where
    F: for<'a> AsyncFn(TcpConnection<'a>) -> Result<()>,
{
    (task.handler)(TcpConnection {
        socket: &mut task.accepted.socket,
        peer: task.accepted.peer,
        stop: task.stop.clone(),
    })
    .await
}

async fn run_server<F>(ctx: Context, mut task: ServerTask<F>, budget: Arc<PortBudget>) -> Result<()>
where
    F: for<'a> AsyncFn(TcpConnection<'a>) -> Result<()> + 'static,
{
    let outcome = accept_connections(&ctx, &mut task, budget).await;
    let outcome = match outcome {
        Err(VertxError::VertxClosed) => Ok(()),
        Err(error) if ctx.is_expected_cancellation(&error) => Ok(()),
        outcome => outcome,
    };
    *task.state.outcome.borrow_mut() = outcome.clone();

    let listener_closed = task.incoming.take().unwrap().unregister();
    let control = &task.state.control;
    control.stop.cancel();
    task.connections.close();
    let cleaned = async {
        if let Some(closed) = listener_closed {
            closed.cancelled().await;
        }
        task.connections.wait().await;
    };
    let mut cleaned = std::pin::pin!(cleaned);
    if let Some(drain) = control.drain.get() {
        tokio::select! {
            biased;
            _ = cleaned.as_mut() => return outcome,
            _ = control.force.cancelled() => {}
            _ = ctx.scope().cancel.cancelled() => {}
            _ = tokio::time::sleep_until(drain.deadline) => {}
        }
    }
    task.state.abort_connections();
    cleaned.await;
    outcome
}

async fn accept_connections<F>(
    ctx: &Context,
    task: &mut ServerTask<F>,
    budget: Arc<PortBudget>,
) -> Result<()>
where
    F: for<'a> AsyncFn(TcpConnection<'a>) -> Result<()> + 'static,
{
    let control = &task.state.control;
    let incoming = task.incoming.as_mut().unwrap();
    loop {
        let accepted = tokio::select! {
            biased;
            _ = control.stop.cancelled() => return Ok(()),
            _ = ctx.scope().cancel.cancelled() => return Ok(()),
            accepted = async {
                let permit = ConnectionPermit {
                    _port: budget.acquire().await,
                    _tracked: task.connections.token(),
                    registration: None,
                    _state: task.state.clone(),
                };
                let (socket, peer) = incoming.accept().await?;
                Ok(Accepted { socket, peer, _permit: permit })
            } => accepted,
        };
        let mut accepted = match accepted {
            Err(VertxError::TcpIo {
                operation: "accept",
                kind: io::ErrorKind::Interrupted | io::ErrorKind::ConnectionAborted,
                ..
            }) => {
                tokio::task::yield_now().await;
                continue;
            }
            other => other?,
        };
        // Commit admission before handing the connection to a managed task.
        let id = TaskId::next();
        let Some(registration) = task.state.register(id) else {
            return Ok(());
        };
        accepted._permit.registration = Some(registration);
        let connection = ctx.spawn_local_with_id(
            id,
            handle(ConnectionTask {
                handler: task.handler.clone(),
                accepted,
                stop: control.stop.child_token(),
            }),
        )?;
        task.state.attach(id, connection.abort_handle());
    }
}
