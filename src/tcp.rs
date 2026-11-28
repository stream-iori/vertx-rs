//! Shared TCP listeners and managed connection tasks on EventLoops.

mod listener;
mod server;

pub use listener::TcpIncoming;
pub(crate) use listener::{ListenerRegistration, TcpListeners};
pub use server::{TcpConnection, TcpServer, TcpServerOptions};
