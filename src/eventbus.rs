//! Local typed messaging with bounded consumer queues.

mod bus;
mod consumer;
mod delivery;
mod envelope;
mod options;
mod queue;
mod route;
mod sink;

pub use bus::EventBus;
pub use consumer::Consumer;
pub use envelope::{Envelope, ReplyHandle};
pub use options::{ConsumerOptions, DeliveryOptions, Headers, RequestOptions};
pub(crate) use route::SubscriptionRegistration;
pub use sink::MessageSink;
