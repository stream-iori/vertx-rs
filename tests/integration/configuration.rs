//! Builder defaults and explicit overrides.

use super::support::{local, vx};
use rstest::rstest;
use std::time::Duration;
use vertx_rs::{DeploymentOptions, Vertx};

#[rstest]
#[case::default(Vertx::builder())]
#[case::explicit(Vertx::builder().event_loop_threads(1).worker_threads(2))]
#[tokio::test]
async fn fallible_builder_creates_a_closable_instance(#[case] builder: vertx_rs::VertxBuilder) {
    let vx = builder.try_build().unwrap();
    assert!(vx.event_loop_count() > 0);
    assert!(vx.worker_thread_count() > 0);
    vx.close().await.unwrap();
    assert_eq!(vx.diagnostics().close_stage, "closed");
}

#[rstest]
#[case::event_loop(0, 1, Duration::from_secs(1))]
#[case::worker(1, 0, Duration::from_secs(1))]
#[case::grace(1, 1, Duration::ZERO)]
#[test]
#[should_panic(expected = "failed to initialize Vertx")]
fn infallible_builder_keeps_panic_behavior(
    #[case] event_loops: usize,
    #[case] workers: usize,
    #[case] grace: Duration,
) {
    let _ = Vertx::builder()
        .event_loop_threads(event_loops)
        .worker_threads(workers)
        .shutdown_grace(grace)
        .build();
}

#[rstest]
#[tokio::test]
async fn zero_instances_keeps_one_instance_compatibility(vx: Vertx) {
    let ids = vx
        .deploy_with(
            |_, _| local(|_| async { Ok(()) }),
            DeploymentOptions::new().instances(0),
        )
        .await
        .unwrap();
    assert_eq!(ids.len(), 1);
    vx.close().await.unwrap();
}

#[tokio::test]
async fn default_worker_pool_size_is_20() {
    let vx = Vertx::builder().event_loop_threads(1).build();
    assert_eq!(vx.worker_thread_count(), 20);
    vx.close().await.unwrap();
}

#[tokio::test]
async fn builder_overrides() {
    let vx = Vertx::builder()
        .event_loop_threads(3)
        .worker_threads(4)
        .build();
    assert_eq!(vx.event_loop_count(), 3);
    assert_eq!(vx.worker_thread_count(), 4);
    vx.close().await.unwrap();
}
