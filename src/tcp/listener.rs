use crate::deployment::{ResourceId, ResourceRegistration};
use crate::{Context, DeploymentId, Result, VertxError};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::future::poll_fn;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context as TaskContext, Poll};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
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
    registration: ListenerRegistration,
    turn: Arc<AsyncMutex<()>>,
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
        let (incoming, _) = open_tcp(self, addr, ListenMode::Raw).await?;
        Ok(incoming)
    }
}

pub(super) async fn listen_managed(
    ctx: &Context,
    addr: SocketAddr,
    max_connections: NonZeroUsize,
) -> Result<(TcpIncoming, Arc<PortBudget>)> {
    let (incoming, budget) = open_tcp(ctx, addr, ListenMode::Managed(max_connections)).await?;
    Ok((
        incoming,
        budget.expect("managed listener has a connection budget"),
    ))
}

async fn open_tcp(
    ctx: &Context,
    addr: SocketAddr,
    mode: ListenMode,
) -> Result<(TcpIncoming, Option<Arc<PortBudget>>)> {
    ctx.check_local()?;
    if let ListenMode::Managed(limit) = mode
        && limit.get() > Semaphore::MAX_PERMITS
    {
        return Err(VertxError::InvalidOptions(
            "TCP connection limit exceeds Semaphore::MAX_PERMITS".into(),
        ));
    }
    tokio::select! {
        biased;
        _ = ctx.scope().cancel.cancelled() => Err(stopped(ctx)),
        incoming = ctx.tcp_listeners().listen(ctx, addr, mode) => incoming,
    }
}

impl TcpIncoming {
    /// Returns the bound address, including the OS-assigned port.
    pub fn local_addr(&self) -> SocketAddr {
        self.registration.addr
    }

    /// Receives the next connection in order of waiting calls.
    ///
    /// Poll this future in the owning Context and EventLoop. Cancelling this
    /// call gives up its position in line. The registration can accept again.
    /// If nobody is waiting, connections stay in the OS backlog.
    pub async fn accept(&mut self) -> Result<(TcpStream, SocketAddr)> {
        self.ctx.check_local()?;
        let registry = self.ctx.tcp_listeners();
        let accept = async {
            let _turn = self.turn.lock().await;
            let accepted = poll_fn(|cx| {
                // Keep the listener alive only while this poll can use it.
                registry
                    .registered_listener(&self.ctx, &self.registration)?
                    .poll_accept(cx)
            })
            .await?;
            // The OS may have accepted a connection while shutdown closed admission.
            registry.registered_listener(&self.ctx, &self.registration)?;
            Ok(accepted)
        };
        tokio::select! {
            biased;
            _ = self.ctx.scope().cancel.cancelled() => Err(stopped(&self.ctx)),
            accepted = accept => accepted,
        }
    }

    pub(super) fn unregister(&self) -> Option<CancellationToken> {
        let done = self.registration.unregister();
        self.ctx.scope().forget_resource(self.registration.id);
        done
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
    budgets: HashMap<SocketAddr, Weak<PortBudget>>,
}

enum Slot {
    Open(OpenListener),
    Closing {
        done: CancellationToken,
        tracked: TaskTrackerToken,
        id: ResourceId,
    },
}

struct OpenListener {
    bound: Arc<BoundListener>,
    members: HashMap<DeploymentId, Member>,
    budget: Option<Arc<PortBudget>>,
}

#[derive(Clone, Copy)]
enum ListenMode {
    Raw,
    Managed(NonZeroUsize),
}

impl ListenMode {
    fn connection_limit(self) -> Option<NonZeroUsize> {
        match self {
            Self::Raw => None,
            Self::Managed(limit) => Some(limit),
        }
    }
}

pub(super) struct PortBudget {
    limit: NonZeroUsize,
    capacity: Arc<Semaphore>,
}

impl PortBudget {
    fn new(limit: NonZeroUsize) -> Self {
        Self {
            limit,
            capacity: Arc::new(Semaphore::new(limit.get())),
        }
    }

    pub(super) async fn acquire(self: &Arc<Self>) -> PortPermit {
        let capacity = self
            .capacity
            .clone()
            .acquire_owned()
            .await
            .expect("the port capacity semaphore is never closed");
        PortPermit {
            _capacity: capacity,
            _budget: self.clone(),
        }
    }
}

pub(super) struct PortPermit {
    _capacity: OwnedSemaphorePermit,
    // Keep the address budget available to a replacement listener while this socket lives.
    _budget: Arc<PortBudget>,
}

struct Member {
    id: ResourceId,
    tracked: TaskTrackerToken,
}

#[derive(Clone)]
pub(crate) struct ListenerRegistration {
    registry: Weak<Mutex<State>>,
    addr: SocketAddr,
    deployment: DeploymentId,
    id: ResourceId,
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
    Ready {
        registration: ListenerRegistration,
        turn: Arc<AsyncMutex<()>>,
        budget: Option<Arc<PortBudget>>,
    },
    Unbound,
    Closing(CancellationToken),
}

impl TcpListeners {
    async fn listen(
        &self,
        ctx: &Context,
        requested: SocketAddr,
        mode: ListenMode,
    ) -> Result<(TcpIncoming, Option<Arc<PortBudget>>)> {
        loop {
            let binding = self.binding.lock().await;
            ctx.check_local()?;
            let registration = if requested.port() == 0 {
                Registration::Unbound
            } else {
                self.register(ctx, requested, mode, None)?
            };
            let registration = match registration {
                Registration::Unbound => {
                    let socket = match mode {
                        ListenMode::Raw => TcpListener::bind(requested).await,
                        ListenMode::Managed(_) => bind_managed(requested),
                    }
                    .map_err(|error| io_error("bind", requested, error))?;
                    let addr = socket
                        .local_addr()
                        .map_err(|error| io_error("bind", requested, error))?;
                    self.register(ctx, addr, mode, Some(socket))?
                }
                registration => registration,
            };
            drop(binding);
            match registration {
                Registration::Ready {
                    registration,
                    turn,
                    budget,
                } => {
                    let incoming = TcpIncoming {
                        ctx: ctx.clone(),
                        registration,
                        turn,
                        _local: PhantomData,
                    };
                    return Ok((incoming, budget));
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
        mode: ListenMode,
        socket: Option<TcpListener>,
    ) -> Result<Registration> {
        ctx.check_local()?;
        let scope = ctx.scope();
        let mut admission = scope.admit()?;
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(VertxError::VertxClosed);
        }
        state.budgets.retain(|_, budget| budget.strong_count() != 0);
        let mut budget = state.budgets.get(&addr).and_then(Weak::upgrade);
        let limit = mode.connection_limit();
        if let Some(retained) = &budget
            && Some(retained.limit) != limit
        {
            return Err(incompatible(addr));
        }
        let registration = ListenerRegistration {
            registry: Arc::downgrade(&self.state),
            addr,
            deployment: scope.deployment,
            id: ResourceId::next(),
        };
        let new_member = || Member {
            id: registration.id,
            tracked: scope.task_token(),
        };
        let turn = match state.entries.entry(addr) {
            Entry::Occupied(entry) => match entry.into_mut() {
                Slot::Open(open) => {
                    if open.budget.as_ref().map(|budget| budget.limit) != limit {
                        return Err(incompatible(addr));
                    }
                    let Entry::Vacant(member) = open.members.entry(scope.deployment) else {
                        return Err(VertxError::InvalidOptions(format!(
                            "deployment already listens on {addr}"
                        )));
                    };
                    member.insert(new_member());
                    open.bound.turn.clone()
                }
                Slot::Closing { done, .. } => return Ok(Registration::Closing(done.clone())),
            },
            Entry::Vacant(entry) => {
                let Some(socket) = socket else {
                    return Ok(Registration::Unbound);
                };
                if budget.is_none() {
                    budget = limit.map(PortBudget::new).map(Arc::new);
                }
                let turn = Arc::new(AsyncMutex::new(()));
                let bound = Arc::new(BoundListener {
                    addr,
                    socket: Some(socket),
                    turn: turn.clone(),
                    registry: Arc::downgrade(&self.state),
                });
                entry.insert(Slot::Open(OpenListener {
                    bound,
                    members: HashMap::from([(scope.deployment, new_member())]),
                    budget: budget.clone(),
                }));
                if let Some(budget) = &budget {
                    state.budgets.insert(addr, Arc::downgrade(budget));
                }
                turn
            }
        };
        admission.register_resource(
            registration.id,
            ResourceRegistration::Listener(registration.clone()),
        );
        Ok(Registration::Ready {
            registration,
            turn,
            budget,
        })
    }

    fn registered_listener(
        &self,
        ctx: &Context,
        registration: &ListenerRegistration,
    ) -> Result<Arc<BoundListener>> {
        ctx.check_local()?;
        let _admission = ctx.scope().admit()?;
        let state = self.state.lock().unwrap();
        if state.closed {
            return Err(VertxError::VertxClosed);
        }
        match state.entries.get(&registration.addr) {
            Some(Slot::Open(open))
                if open
                    .members
                    .get(&registration.deployment)
                    .is_some_and(|member| member.id == registration.id) =>
            {
                Ok(open.bound.clone())
            }
            _ => Err(VertxError::DeploymentTerminated(ctx.scope().deployment)),
        }
    }

    pub(crate) fn close_admission(&self) {
        self.state.lock().unwrap().closed = true;
    }
}

impl ListenerRegistration {
    pub fn unregister(&self) -> Option<CancellationToken> {
        let registry = self.registry.upgrade()?;
        let (member, listener, done) = {
            let mut state = registry.lock().unwrap();
            let slot = state.entries.get_mut(&self.addr)?;
            let Slot::Open(open) = slot else {
                return match slot {
                    Slot::Closing { done, id, .. } if *id == self.id => Some(done.clone()),
                    _ => None,
                };
            };
            let member = match open.members.entry(self.deployment) {
                Entry::Occupied(entry) if entry.get().id == self.id => entry.remove(),
                _ => return None,
            };
            if open.members.is_empty() {
                let done = CancellationToken::new();
                let closing = Slot::Closing {
                    done: done.clone(),
                    tracked: member.tracked,
                    id: self.id,
                };
                (None, Some(std::mem::replace(slot, closing)), Some(done))
            } else {
                (Some(member), None, None)
            }
        };
        // A concurrent accept can still hold a reference during its current
        // poll. The last deployment is tracked until the socket actually drops.
        drop((member, listener));
        done
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
    if ctx.instance_closed() {
        VertxError::VertxClosed
    } else {
        VertxError::DeploymentTerminated(ctx.scope().deployment)
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

#[cfg(test)]
mod tests;
