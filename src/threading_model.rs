/// Mirrors Vert.x's `io.vertx.core.ThreadingModel`.
///
/// Identifies what kind of execution environment a `Context` represents.
/// `VIRTUAL_THREAD` from Vert.x is intentionally omitted — we don't model it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ThreadingModel {
    /// Pinned to a specific EventLoop thread.
    EventLoop,
    /// Runs on the multi-threaded Worker pool.
    Worker,
    /// Thread not managed by this Vertx instance.
    External,
}
