use super::TcpIncoming;
use crate::{Context, ContextId, Result, TaskAbortHandle, VertxError};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::ops::AsyncFn;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;
use tokio::io::{self, AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::{TaskTracker, task_tracker::TaskTrackerToken};

/// Options shared by all managed services on one bound address.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpServerOptions {
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

/// A handle to one deployment's TCP service. Dropping a handle detaches it.
#[derive(Clone)]
pub struct TcpServer {
    control: Arc<Control>,
}

impl TcpServer {
    pub fn local_addr(&self) -> SocketAddr {
        self.control.addr
    }

    /// Stops this registration and signals its connections to drain. The first
    /// shutdown call sets the deadline. Cancelling a waiter leaves cleanup running.
    /// A timeout requests abort; use [`Self::wait_closed`] to await actual cleanup.
    pub async fn shutdown(&self, grace: Duration) -> Result<()> {
        self.check_waiter()?;
        let drain = {
            let mut state = self.control.state.lock().unwrap();
            if let Some(drain) = state.drain {
                drain
            } else {
                let deadline = Instant::now().checked_add(grace).ok_or_else(|| {
                    VertxError::InvalidOptions("TCP shutdown deadline is out of range".into())
                })?;
                let drain = Drain { deadline, grace };
                state.drain = Some(drain);
                drain
            }
        };
        self.control.stop.cancel();
        match tokio::time::timeout_at(drain.deadline, self.wait_closed()).await {
            Ok(outcome) => {
                outcome?;
                if self.control.state.lock().unwrap().forced {
                    Err(VertxError::ShutdownTimeout(drain.grace))
                } else {
                    Ok(())
                }
            }
            Err(_) => {
                self.control.abort_connections();
                Err(VertxError::ShutdownTimeout(drain.grace))
            }
        }
    }

    /// Waits for the listener registration and the connections to be cleaned up.
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
    /// options. The deployment tracks the service and every connection task.
    pub async fn serve_tcp<F>(
        &self,
        addr: SocketAddr,
        options: TcpServerOptions,
        handler: F,
    ) -> Result<TcpServer>
    where
        F: for<'a> AsyncFn(TcpConnection<'a>) -> Result<()> + 'static,
    {
        self.check_local()?;
        if options.max_connections.get() > Semaphore::MAX_PERMITS {
            return Err(VertxError::InvalidOptions(
                "TCP connection limit exceeds Semaphore::MAX_PERMITS".into(),
            ));
        }
        let incoming = self.open_tcp(addr, Some(options)).await?;
        let control = Arc::new(Control {
            addr: incoming.local_addr(),
            owner: self.id(),
            state: Mutex::default(),
            stop: CancellationToken::new(),
            done: watch::channel(None).0,
        });
        let task = ServiceTask {
            incoming: Some(incoming),
            handler: Rc::new(handler),
            connections: TaskTracker::new(),
            completion: Arc::new(Completion {
                control: control.clone(),
                outcome: Mutex::new(Ok(())),
            }),
        };
        self.spawn_local(run_service(self.clone(), task))?;
        Ok(TcpServer { control })
    }
}

pub(super) struct PortBudget {
    pub(super) options: TcpServerOptions,
    capacity: Arc<Semaphore>,
}

impl PortBudget {
    pub(super) fn new(options: TcpServerOptions) -> Self {
        Self {
            options,
            capacity: Arc::new(Semaphore::new(options.max_connections.get())),
        }
    }
}

struct ConnectionPermit {
    _capacity: OwnedSemaphorePermit,
    _budget: Arc<PortBudget>,
    _tracked: TaskTrackerToken,
    _completion: Arc<Completion>,
}

struct Control {
    owner: ContextId,
    addr: SocketAddr,
    state: Mutex<State>,
    stop: CancellationToken,
    done: watch::Sender<Option<Result<()>>>,
}

#[derive(Default)]
struct State {
    drain: Option<Drain>,
    forced: bool,
    children: Vec<TaskAbortHandle>,
}

#[derive(Clone, Copy)]
struct Drain {
    deadline: Instant,
    grace: Duration,
}

impl Control {
    fn admitting(&self) -> bool {
        self.state.lock().unwrap().drain.is_none()
    }

    fn track(&self, task: TaskAbortHandle) {
        {
            let mut state = self.state.lock().unwrap();
            if !state.forced {
                state.children.retain(|task| !task.is_finished());
                state.children.push(task);
                return;
            }
        }
        task.abort();
    }

    fn abort_connections(&self) {
        let children = {
            let mut state = self.state.lock().unwrap();
            state.forced = true;
            std::mem::take(&mut state.children)
        };
        for child in children {
            child.abort();
        }
    }
}

struct Completion {
    control: Arc<Control>,
    outcome: Mutex<Result<()>>,
}

impl Drop for Completion {
    fn drop(&mut self) {
        let children = std::mem::take(&mut self.control.state.lock().unwrap().children);
        drop(children);
        self.control
            .done
            .send_replace(Some(self.outcome.get_mut().unwrap().clone()));
    }
}

struct ServiceTask<F> {
    incoming: Option<TcpIncoming>,
    handler: Rc<F>,
    connections: TaskTracker,
    completion: Arc<Completion>,
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

async fn run_service<F>(ctx: Context, mut task: ServiceTask<F>) -> Result<()>
where
    F: for<'a> AsyncFn(TcpConnection<'a>) -> Result<()> + 'static,
{
    let outcome = accept_connections(&ctx, &mut task).await;
    let outcome = match outcome {
        Err(VertxError::VertxClosed) => Ok(()),
        Err(error) if ctx.is_expected_cancellation(&error) => Ok(()),
        outcome => outcome,
    };
    *task.completion.outcome.lock().unwrap() = outcome.clone();

    let listener_closed = task.incoming.take().unwrap().unregister();
    let control = &task.completion.control;
    control.stop.cancel();
    task.connections.close();
    let cleaned = async {
        if let Some(closed) = listener_closed {
            closed.cancelled().await;
        }
        task.connections.wait().await;
    };
    let mut cleaned = std::pin::pin!(cleaned);
    let drain = control.state.lock().unwrap().drain;
    if let Some(drain) = drain {
        tokio::select! {
            biased;
            _ = cleaned.as_mut() => return outcome,
            _ = ctx.scope.cancel.cancelled() => {}
            _ = tokio::time::sleep_until(drain.deadline) => {}
        }
    }
    control.abort_connections();
    cleaned.await;
    outcome
}

async fn accept_connections<F>(ctx: &Context, task: &mut ServiceTask<F>) -> Result<()>
where
    F: for<'a> AsyncFn(TcpConnection<'a>) -> Result<()> + 'static,
{
    let control = &task.completion.control;
    let incoming = task.incoming.as_mut().unwrap();
    let budget = incoming.budget.as_ref().unwrap().clone();
    loop {
        let accepted = tokio::select! {
            biased;
            _ = control.stop.cancelled() => return Ok(()),
            _ = ctx.scope.cancel.cancelled() => return Ok(()),
            accepted = async {
                let capacity = budget.capacity.clone().acquire_owned().await
                    .expect("the port capacity semaphore is never closed");
                let permit = ConnectionPermit {
                    _capacity: capacity,
                    _budget: budget.clone(),
                    _tracked: task.connections.token(),
                    _completion: task.completion.clone(),
                };
                let (socket, peer) = incoming.accept().await?;
                Ok(Accepted { socket, peer, _permit: permit })
            } => accepted,
        };
        let accepted = match accepted {
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
        if !control.admitting() {
            return Ok(());
        }
        let task = ctx.spawn_local(handle(ConnectionTask {
            handler: task.handler.clone(),
            accepted,
            stop: control.stop.child_token(),
        }))?;
        control.track(task.abort_handle());
    }
}
