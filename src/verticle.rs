use async_trait::async_trait;

use crate::{Context, Result};

/// A unit of deployment pinned to a single EventLoop.
///
/// `Verticle` is **not required to be `Send`** — it lives on one thread for
/// its entire lifetime, so it may hold `Rc`, `RefCell`, raw pointers, etc.
///
/// The trait has a single method `run(self, ctx)`. The verticle is moved
/// into the framework's task and **owns its own run loop**. Typical shape:
///
/// ```ignore
/// async fn run(mut self, ctx: Context) -> Result<()> {
///     let mut c = ctx.event_bus().consumer::<Req>("addr")?;
///     let cancel = ctx.cancellation_token();
///     loop {
///         tokio::select! {
///             _ = cancel.cancelled() => return Ok(()),
///             msg = c.recv() => {
///                 let Some(env) = msg else { return Ok(()) };
///                 // ... handle env ...
///             }
///         }
///     }
/// }
/// ```
///
/// To **deploy** a verticle, supply a factory closure to `Vertx::deploy`
/// that receives `Context` and returns `Self`. The factory runs synchronously
/// on the target EventLoop thread, so it's the right place to register
/// EventBus consumers before `run` starts polling them.
#[async_trait(?Send)]
pub trait Verticle: 'static {
    async fn run(self, ctx: Context) -> Result<()>;
}

/// A unit of deployment that runs on the multi-threaded worker pool.
///
/// Same single-method shape as `Verticle`, with `Send` required.
#[async_trait]
pub trait WorkerVerticle: Send + 'static {
    async fn run(self, ctx: Context) -> Result<()>;
}
