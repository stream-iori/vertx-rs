/// Logical execution model reported by [`ExecutionInfo`](crate::ExecutionInfo).
/// A blocking closure retains its source model while executing on a Worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ThreadingModel {
    /// Pinned to a specific EventLoop thread.
    EventLoop,
    /// Runs on the multi-threaded Worker pool.
    Worker,
}
