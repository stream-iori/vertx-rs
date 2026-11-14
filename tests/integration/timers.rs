//! Absolute deadlines, paused time, cancellation and periodic Skip.

use super::support::{capture, vx};
use rstest::rstest;
use std::time::Duration;
use vertx_rs::{Vertx, VertxError};

#[rstest]
#[tokio::test(start_paused = true)]
async fn vertx_timer_fires_after_delay(vx: Vertx) {
    let timer = vx.timer(Duration::from_millis(100)).unwrap();
    tokio::time::advance(Duration::from_millis(200)).await;
    tokio::time::timeout(Duration::from_millis(1), timer)
        .await
        .unwrap()
        .unwrap();
    tokio::time::resume();
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn vertx_timer_explicit_cancel(vx: Vertx) {
    let timer = vx.timer(Duration::ZERO).unwrap();
    timer.cancel();
    assert!(timer.is_cancelled());
    assert!(matches!(timer.await, Err(VertxError::TimerCancelled)));
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test(start_paused = true)]
async fn vertx_periodic_ticks_multiple_times(vx: Vertx) {
    let mut periodic = vx
        .periodic(Duration::from_millis(10), Duration::from_millis(10))
        .unwrap();
    for _ in 0..3 {
        periodic.tick().await.unwrap();
    }
    periodic.cancel();
    assert!(periodic.is_cancelled());
    assert!(matches!(
        periodic.tick().await,
        Err(VertxError::TimerCancelled)
    ));
    tokio::time::resume();
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn bound_timer_cancels_on_undeploy(vx: Vertx) {
    let (id, ctx) = capture(&vx).await;
    let timer = ctx.timer(Duration::from_secs(99)).unwrap();
    let mut periodic = ctx
        .periodic(Duration::ZERO, Duration::from_secs(1))
        .unwrap();
    vx.undeploy(id).await.unwrap();
    assert!(timer.is_cancelled());
    assert!(periodic.is_cancelled());
    assert!(matches!(timer.await, Err(VertxError::TimerCancelled)));
    assert!(matches!(
        periodic.tick().await,
        Err(VertxError::TimerCancelled)
    ));
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn zero_period_is_invalid_and_cancel_wins_over_due_tick(vx: Vertx) {
    assert!(matches!(
        vx.periodic(Duration::ZERO, Duration::ZERO),
        Err(VertxError::InvalidOptions(_))
    ));
    let mut p = vx.periodic(Duration::ZERO, Duration::from_secs(1)).unwrap();
    p.cancel();
    assert!(matches!(p.tick().await, Err(VertxError::TimerCancelled)));
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test(start_paused = true)]
async fn periodic_skips_missed_ticks(vx: Vertx) {
    let mut timer = vx
        .periodic(Duration::ZERO, Duration::from_millis(10))
        .unwrap();
    timer.tick().await.unwrap();
    tokio::time::advance(Duration::from_millis(95)).await;
    timer.tick().await.unwrap();
    let before = tokio::time::Instant::now();
    timer.tick().await.unwrap();
    assert!(tokio::time::Instant::now() - before >= Duration::from_millis(5));
    tokio::time::resume();
    vx.close().await.unwrap();
}

#[test]
fn timers_can_be_constructed_without_an_entered_tokio_runtime() {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build();
    let timer = vx.timer(Duration::from_secs(1)).unwrap();
    let periodic = vx.periodic(Duration::ZERO, Duration::from_secs(1)).unwrap();
    timer.cancel();
    periodic.cancel();
    assert!(timer.is_cancelled());
    assert!(periodic.is_cancelled());
}

#[rstest]
#[tokio::test]
async fn completed_timer_does_not_become_cancelled_on_close(vx: Vertx) {
    let mut timer = vx.timer(Duration::ZERO).unwrap();
    (&mut timer).await.unwrap();
    vx.close().await.unwrap();
    timer.cancel();
    assert!(!timer.is_cancelled());
}
