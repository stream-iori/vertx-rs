//! vertx-rs 0.2: EventLoop-affine Verticle + WorkerVerticle + Context.
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
pub use eventbus::{Consumer, DeliveryOptions, Envelope, EventBus, ReplyHandle};
pub use eventloop::EventLoopId;
pub use threading_model::ThreadingModel;
pub use timer::{Periodic, Timer};
pub use verticle::{Verticle, WorkerVerticle};
pub use vertx::{Vertx, VertxBuilder};
