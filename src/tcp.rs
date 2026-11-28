use crate::{Context, DeploymentId, Result, VertxError};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::future::poll_fn;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context as TaskContext, Poll};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;
use tokio_util::task::task_tracker::TaskTrackerToken;

/// A deployment's registration on a shared TCP listener.
///
/// Dropping it unregisters this deployment from the address. An accepted
/// stream belongs to its caller and can be owned by a managed task.
///
/// ```compile_fail,E0277
/// use vertx_rs::TcpIncoming;
///
/// fn require_send<T: Send>() {}
/// require_send::<TcpIncoming>();
/// ```
pub struct TcpIncoming {
    ctx: Context,
    addr: SocketAddr,
    turn: Arc<AsyncMutex<()>>,
    budget: Option<Arc<service::PortBudget>>,
    _local: PhantomData<Rc<()>>,
}

impl Context {
    /// Listens on an address shared by deployments in this Vertx instance.
    ///
    /// Call this from this exact Context on its assigned EventLoop.
    /// A live Worker Context or a different Context returns
    /// [`VertxError::ContextMismatch`]. Registering twice in one deployment
    /// on the same address returns [`VertxError::InvalidOptions`].
    /// Port zero requests a new address. Use [`TcpIncoming::local_addr`] to
    /// get the address that other deployments can register on.
    pub async fn listen_tcp(&self, addr: SocketAddr) -> Result<TcpIncoming> {
        self.open_tcp(addr, None).await
    }

    async fn open_tcp(
        &self,
        addr: SocketAddr,
        options: Option<TcpServerOptions>,
    ) -> Result<TcpIncoming> {
        self.check_local()?;
        tokio::select! {
            biased;
            _ = self.scope.cancel.cancelled() => Err(stopped(self)),
            incoming = self.inner.tcp.listen(self, addr, options) => incoming,
        }
    }
}

impl TcpIncoming {
    /// Returns the bound address, including the OS-assigned port.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Receives the next connection in order of waiting calls.
    ///
    /// Poll this future in the owning Context and EventLoop. Cancelling this
    /// call gives up its position in line. The registration can accept again.
    /// If nobody is waiting, connections stay in the OS backlog.
    pub async fn accept(&mut self) -> Result<(TcpStream, SocketAddr)> {
        self.ctx.check_local()?;
        let registry = &self.ctx.inner.tcp;
        let accept = async {
            let _turn = self.turn.lock().await;
            let accepted = poll_fn(|cx| {
                // Keep the listener alive only while this poll can use it.
                registry
                    .registered_listener(&self.ctx, self.addr)?
                    .poll_accept(cx)
            })
            .await?;
            // The OS may have accepted a connection while shutdown closed admission.
            registry.registered_listener(&self.ctx, self.addr)?;
            Ok(accepted)
        };
        tokio::select! {
            biased;
            _ = self.ctx.scope.cancel.cancelled() => Err(stopped(&self.ctx)),
            accepted = accept => accepted,
        }
    }

    fn unregister(&self) -> Option<CancellationToken> {
        self.ctx
            .inner
            .tcp
            .unregister(self.addr, self.ctx.scope.deployment)
    }
}

impl Drop for TcpIncoming {
    fn drop(&mut self) {
        self.unregister();
    }
}

#[derive(Default)]
pub(crate) struct TcpListeners {
    state: Arc<Mutex<State>>,
    binding: AsyncMutex<()>,
}

#[derive(Default)]
struct State {
    closed: bool,
    entries: HashMap<SocketAddr, Slot>,
    budgets: HashMap<SocketAddr, Weak<service::PortBudget>>,
}

enum Slot {
    Open(OpenListener),
    Closing {
        done: CancellationToken,
        tracked: TaskTrackerToken,
        deployment: DeploymentId,
    },
}

struct OpenListener {
    bound: Arc<BoundListener>,
    members: HashMap<DeploymentId, TaskTrackerToken>,
    budget: Option<Arc<service::PortBudget>>,
}

struct BoundListener {
    addr: SocketAddr,
    socket: Option<TcpListener>,
    turn: Arc<AsyncMutex<()>>,
    registry: Weak<Mutex<State>>,
}

impl BoundListener {
    fn poll_accept(&self, cx: &mut TaskContext<'_>) -> Poll<Result<(TcpStream, SocketAddr)>> {
        self.socket
            .as_ref()
            .expect("registered listener has a socket")
            .poll_accept(cx)
            .map_err(|error| io_error("accept", self.addr, error))
    }
}

impl Drop for BoundListener {
    fn drop(&mut self) {
        drop(self.socket.take());
        if let Some(registry) = self.registry.upgrade() {
            let slot = registry.lock().unwrap().entries.remove(&self.addr);
            if let Some(Slot::Closing { done, tracked, .. }) = slot {
                done.cancel();
                drop(tracked);
            }
        }
    }
}

enum Registration {
    Ready(Arc<BoundListener>, Option<Arc<service::PortBudget>>),
    Unbound,
    Closing(CancellationToken),
}

impl TcpListeners {
    async fn listen(
        &self,
        ctx: &Context,
        requested: SocketAddr,
        options: Option<TcpServerOptions>,
    ) -> Result<TcpIncoming> {
        loop {
            let binding = self.binding.lock().await;
            ctx.check_local()?;
            let registration = if requested.port() == 0 {
                Registration::Unbound
            } else {
                self.register(ctx, requested, options, None)?
            };
            let registration = match registration {
                Registration::Unbound => {
                    let socket = if options.is_some() {
                        bind_managed(requested)
                    } else {
                        TcpListener::bind(requested).await
                    }
                    .map_err(|error| io_error("bind", requested, error))?;
                    let addr = socket
                        .local_addr()
                        .map_err(|error| io_error("bind", requested, error))?;
                    self.register(ctx, addr, options, Some(socket))?
                }
                registration => registration,
            };
            drop(binding);
            match registration {
                Registration::Ready(bound, budget) => {
                    return Ok(TcpIncoming {
                        ctx: ctx.clone(),
                        addr: bound.addr,
                        turn: bound.turn.clone(),
                        budget,
                        _local: PhantomData,
                    });
                }
                Registration::Closing(done) => done.cancelled().await,
                Registration::Unbound => unreachable!("listener was just bound"),
            }
        }
    }

    fn register(
        &self,
        ctx: &Context,
        addr: SocketAddr,
        options: Option<TcpServerOptions>,
        socket: Option<TcpListener>,
    ) -> Result<Registration> {
        ctx.check_local()?;
        let _admission = ctx.scope.admit()?;
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(VertxError::VertxClosed);
        }
        state.budgets.retain(|_, budget| budget.strong_count() != 0);
        let mut budget = state.budgets.get(&addr).and_then(Weak::upgrade);
        if let Some(retained) = &budget
            && Some(retained.options) != options
        {
            return Err(incompatible(addr));
        }
        let bound = match state.entries.entry(addr) {
            Entry::Occupied(entry) => match entry.into_mut() {
                Slot::Open(open) => {
                    if open.budget.as_ref().map(|budget| budget.options) != options {
                        return Err(incompatible(addr));
                    }
                    let Entry::Vacant(member) = open.members.entry(ctx.scope.deployment) else {
                        return Err(VertxError::InvalidOptions(format!(
                            "deployment already listens on {addr}"
                        )));
                    };
                    member.insert(ctx.scope.task_token());
                    open.bound.clone()
                }
                Slot::Closing { done, .. } => return Ok(Registration::Closing(done.clone())),
            },
            Entry::Vacant(entry) => {
                let Some(socket) = socket else {
                    return Ok(Registration::Unbound);
                };
                if budget.is_none() {
                    budget = options.map(service::PortBudget::new).map(Arc::new);
                }
                let bound = Arc::new(BoundListener {
                    addr,
                    socket: Some(socket),
                    turn: Arc::new(AsyncMutex::new(())),
                    registry: Arc::downgrade(&self.state),
                });
                entry.insert(Slot::Open(OpenListener {
                    bound: bound.clone(),
                    members: HashMap::from([(ctx.scope.deployment, ctx.scope.task_token())]),
                    budget: budget.clone(),
                }));
                if let Some(budget) = &budget {
                    state.budgets.insert(addr, Arc::downgrade(budget));
                }
                bound
            }
        };
        Ok(Registration::Ready(bound, budget))
    }

    fn registered_listener(&self, ctx: &Context, addr: SocketAddr) -> Result<Arc<BoundListener>> {
        ctx.check_local()?;
        let _admission = ctx.scope.admit()?;
        let state = self.state.lock().unwrap();
        if state.closed {
            return Err(VertxError::VertxClosed);
        }
        match state.entries.get(&addr) {
            Some(Slot::Open(open)) if open.members.contains_key(&ctx.scope.deployment) => {
                Ok(open.bound.clone())
            }
            _ => Err(VertxError::DeploymentTerminated(ctx.scope.deployment)),
        }
    }

    fn unregister(&self, addr: SocketAddr, deployment: DeploymentId) -> Option<CancellationToken> {
        let (member, listener, done) = {
            let mut state = self.state.lock().unwrap();
            let slot = state.entries.get_mut(&addr)?;
            let Slot::Open(open) = slot else {
                return match slot {
                    Slot::Closing {
                        done,
                        deployment: owner,
                        ..
                    } if *owner == deployment => Some(done.clone()),
                    _ => None,
                };
            };
            let tracked = open.members.remove(&deployment)?;
            if open.members.is_empty() {
                let done = CancellationToken::new();
                let closing = Slot::Closing {
                    done: done.clone(),
                    tracked,
                    deployment,
                };
                (None, Some(std::mem::replace(slot, closing)), Some(done))
            } else {
                (Some(tracked), None, None)
            }
        };
        // A concurrent accept can still hold a reference during its current
        // poll. The last deployment is tracked until the socket actually drops.
        drop((member, listener));
        done
    }

    pub(crate) fn unregister_deployment(&self, deployment: DeploymentId) {
        let addresses: Vec<_> = self
            .state
            .lock()
            .unwrap()
            .entries
            .iter()
            .filter_map(|(&addr, slot)| match slot {
                Slot::Open(open) if open.members.contains_key(&deployment) => Some(addr),
                _ => None,
            })
            .collect();
        for addr in addresses {
            self.unregister(addr, deployment);
        }
    }

    pub(crate) fn close_admission(&self) {
        self.state.lock().unwrap().closed = true;
    }
}

fn incompatible(addr: SocketAddr) -> VertxError {
    VertxError::InvalidOptions(format!("incompatible TCP listener options for {addr}"))
}

fn bind_managed(addr: SocketAddr) -> io::Result<TcpListener> {
    let socket = match addr {
        SocketAddr::V4(_) => TcpSocket::new_v4()?,
        SocketAddr::V6(_) => TcpSocket::new_v6()?,
    };
    #[cfg(unix)]
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    socket.listen(128)
}

fn stopped(ctx: &Context) -> VertxError {
    if ctx.inner.cancel.is_cancelled() {
        VertxError::VertxClosed
    } else {
        VertxError::DeploymentTerminated(ctx.scope.deployment)
    }
}

fn io_error(operation: &'static str, addr: SocketAddr, error: io::Error) -> VertxError {
    VertxError::TcpIo {
        operation,
        addr,
        kind: error.kind(),
        message: error.to_string(),
    }
}

mod service;
#[cfg(test)]
mod tests;

pub use service::{TcpConnection, TcpServer, TcpServerOptions};
