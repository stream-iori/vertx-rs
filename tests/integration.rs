//! Contract tests for the Vert.x 5-style execution core.
//! Filter a suite with `cargo test --test integration streams::`.
//! Each case owns its instance and explicitly awaits close; no shared runtime fixture.

#[path = "integration/blocking.rs"]
mod blocking;
#[path = "integration/configuration.rs"]
mod configuration;
#[path = "integration/context.rs"]
mod context;
#[path = "integration/diagnostics.rs"]
mod diagnostics;
#[path = "integration/errors.rs"]
mod errors;
#[path = "integration/eventbus.rs"]
mod eventbus;
#[path = "integration/lifecycle.rs"]
mod lifecycle;
#[path = "integration/shared_state.rs"]
mod shared_state;
#[path = "integration/shutdown.rs"]
mod shutdown;
#[path = "integration/streams.rs"]
mod streams;
#[path = "integration/support.rs"]
mod support;
#[path = "integration/tcp.rs"]
mod tcp;
#[path = "integration/tcp_service.rs"]
mod tcp_service;
#[path = "integration/timers.rs"]
mod timers;
#[path = "integration/tracing_propagation.rs"]
mod tracing_propagation;
#[path = "integration/worker.rs"]
mod worker;
