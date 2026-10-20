//! vertx-rs 0.3: thread-affine deployments with asynchronous start/stop.
//!
//! See `DESIGN.md` for the design rationale.

mod context;
mod deployment;
mod deployment_options;
mod diagnostics;
mod error;
mod eventbus;
mod eventloop;
mod task;
mod threading_model;
mod timer;
mod verticle;
mod vertx;
mod worker_pool;

pub use context::{Context, ContextId};
pub use deployment::DeploymentId;
pub use deployment_options::DeploymentOptions;
pub use diagnostics::{
    DeploymentSnapshot, DiagnosticsOptions, DiagnosticsSnapshot, TaskSnapshot, TaskStage,
    ThreadSnapshot,
};
pub use error::{Result, VertxError};
pub use eventbus::{Consumer, DeliveryOptions, Envelope, EventBus, Headers, ReplyHandle};
pub use eventloop::EventLoopId;
pub use task::{TaskAbortHandle, TaskHandle, TaskId};
pub use threading_model::ThreadingModel;
pub use timer::{Periodic, Timer};
pub use verticle::{Verticle, WorkerVerticle};
pub use vertx::{Vertx, VertxBuilder};
