/// Logical execution model of a framework [`Context`](crate::Context).
///
/// This describes the Context, not the currently executing OS thread.
/// Framework-external code has no current Context; it does not have its own model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ThreadingModel {
    /// Pinned to a specific EventLoop thread.
    EventLoop,
    /// Runs on the multi-threaded Worker pool.
    Worker,
}
