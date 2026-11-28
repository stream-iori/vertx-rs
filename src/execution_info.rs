use crate::{ContextId, DeploymentId, EventLoopId, ThreadingModel};

/// Physical executor thread running the current operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutorThread {
    /// An EventLoop with a fixed OS thread.
    EventLoop(EventLoopId),
    /// A thread in the pool shared by Worker futures and blocking closures.
    Worker,
}

/// A read-only snapshot of the current managed execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionInfo {
    /// Process-unique logical context identity.
    pub context_id: ContextId,
    /// Owning deployment. Instance-level work has no deployment ID.
    pub deployment_id: Option<DeploymentId>,
    /// Logical execution model of the source context.
    pub threading_model: ThreadingModel,
    /// EventLoop assigned to the source context, when it has one.
    pub event_loop_id: Option<EventLoopId>,
    /// Physical thread type, which can differ from the logical model during blocking work.
    pub thread: ExecutorThread,
}

impl ExecutionInfo {
    /// Returns metadata for the current managed operation, regardless of its
    /// Vertx instance. Returns `None` outside managed execution.
    pub fn current() -> Option<Self> {
        crate::context::Context::current()?.execution_info()
    }
}
