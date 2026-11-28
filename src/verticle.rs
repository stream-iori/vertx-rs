use crate::{Context, Result};
use std::future::{Future, ready};

/// A deployment whose construction, lifecycle, tasks and destruction stay on
/// one OS thread. Start message loops with `Context::spawn_local` in `start`.
///
/// ```
/// use vertx_rs::{Context, Result, Verticle};
/// struct Service(std::rc::Rc<String>);
/// impl Verticle for Service {
///     async fn start(&mut self, ctx: Context) -> Result<()> {
///         let prefix = self.0.clone();
///         ctx.event_bus().consumer::<String>("greet")?.handler(async move |message| {
///             tokio::task::yield_now().await;
///             let greeting = format!("{prefix} {}", message.body());
///             message.reply(greeting)
///         })
///     }
/// }
/// ```
pub trait Verticle: 'static {
    /// Initializes the deployment. Successful return makes it ready.
    fn start(&mut self, ctx: Context) -> impl Future<Output = Result<()>>;

    /// Releases application resources after managed work has drained.
    fn stop(&mut self, _ctx: Context) -> impl Future<Output = Result<()>> {
        ready(Ok(()))
    }
}
/// Worker executions may migrate threads, but ordinary polls sharing a
/// Context are serialized. Blocking closures are separately ordered.
///
/// ```compile_fail,E0277
/// use vertx_rs::{Context, Result, WorkerVerticle};
/// struct Service;
/// impl WorkerVerticle for Service {
///     async fn start(&mut self, _: Context) -> Result<()> {
///         let local = std::rc::Rc::new(1);
///         tokio::task::yield_now().await;
///         assert_eq!(*local, 1);
///         Ok(())
///     }
/// }
/// ```
pub trait WorkerVerticle: Send + 'static {
    /// Initializes the deployment on the Worker pool. The future must be `Send`.
    fn start(&mut self, ctx: Context) -> impl Future<Output = Result<()>> + Send;

    /// Releases application resources after managed work has drained.
    fn stop(&mut self, _ctx: Context) -> impl Future<Output = Result<()>> + Send {
        ready(Ok(()))
    }
}
