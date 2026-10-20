//! vertx-rs 0.3: thread-affine deployments with asynchronous start/stop.
//!
//! See `DESIGN.md` for the design rationale.

mod context;
mod deployment;
mod deployment_options;
mod error;
mod eventbus;
mod eventloop;
mod threading_model;
mod timer;
mod verticle;
mod vertx;
mod worker_pool;

pub use context::Context;
pub use deployment::DeploymentId;
pub use deployment_options::DeploymentOptions;
pub use error::{Result, VertxError};
pub use eventbus::{Consumer, DeliveryOptions, Envelope, EventBus, Headers, ReplyHandle};
pub use eventloop::EventLoopId;
pub use threading_model::ThreadingModel;
pub use timer::{Periodic, Timer};
pub use verticle::{Verticle, WorkerVerticle};
pub use vertx::{Vertx, VertxBuilder};
pub use worker_pool::{TaskAbortHandle, TaskHandle};
