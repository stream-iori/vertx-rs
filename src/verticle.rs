use crate::{Context, Result};
use async_trait::async_trait;

/// A deployment whose construction, lifecycle, tasks and destruction stay on
/// one OS thread. Start message loops with `Context::spawn_local` in `start`.
///
/// ```
/// use async_trait::async_trait;
/// use vertx_rs::{Context, Result, Verticle};
/// struct Service;
/// #[async_trait(?Send)]
/// impl Verticle for Service {
///     async fn start(&mut self, ctx: Context) -> Result<()> {
///         ctx.event_bus().consumer::<String>("echo")?.handler(|env| async move {
///             let body = env.body().clone();
///             env.reply(body)
///         })?;
///         Ok(())
///     }
/// }
/// ```
#[async_trait(?Send)]
pub trait Verticle: 'static {
    async fn start(&mut self, ctx: Context) -> Result<()>;
    async fn stop(&mut self, _ctx: Context) -> Result<()> {
        Ok(())
    }
}
/// Worker executions may migrate threads, but ordinary polls sharing a
/// Context are serialized. Blocking closures are separately ordered.
#[async_trait]
pub trait WorkerVerticle: Send + 'static {
    async fn start(&mut self, ctx: Context) -> Result<()>;
    async fn stop(&mut self, _ctx: Context) -> Result<()> {
        Ok(())
    }
}
