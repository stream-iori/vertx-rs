//! Route lifecycle, reentrant wakeups and admission accounting.

use super::Router;
use crate::diagnostics::Diagnostics;
use crate::eventbus::envelope::{ErasedEnvelope, MessageTrace};
use crate::{ConsumerOptions, EventBus, Headers, VertxError};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio_util::sync::CancellationToken;

#[rstest::fixture]
fn bus() -> EventBus {
    EventBus::new(
        CancellationToken::new(),
        Diagnostics::new(0, Default::default()),
    )
}

#[rstest::rstest]
#[tokio::test]
async fn malformed_envelope_reports_conversion_failure_once(bus: EventBus) {
    let mut consumer = bus
        .consumer_with::<u32>("typed", ConsumerOptions::new().with_capacity(1))
        .unwrap();
    bus.router
        .select::<u32>("typed")
        .unwrap()
        .try_reserve()
        .unwrap()
        .send(ErasedEnvelope {
            trace: MessageTrace::new(&bus.router.diagnostics, "typed", None),
            body: Box::new("wrong type"),
            headers: Headers::new(),
            reply_to: None,
        })
        .unwrap();
    assert!(matches!(
        consumer.recv().await,
        Err(VertxError::TypeMismatch(_))
    ));
    assert!(consumer.recv().await.unwrap().is_none());
    assert!(bus.router.routes.is_empty());
}
#[rstest::rstest]
#[tokio::test]
async fn concurrent_last_unregister_and_new_register_preserve_new_route(bus: EventBus) {
    for _ in 0..100 {
        let old = bus.consumer::<u32>("race").unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let b = barrier.clone();
        let dropper = std::thread::spawn(move || {
            b.wait();
            drop(old);
        });
        let b = barrier.clone();
        let router = bus.clone();
        let creator = std::thread::spawn(move || {
            b.wait();
            router.consumer::<u32>("race").unwrap()
        });
        dropper.join().unwrap();
        let mut new = creator.join().unwrap();
        bus.try_send("race", 42u32).unwrap();
        assert_eq!(new.recv().await.unwrap().unwrap().into_body(), 42);
    }
    assert!(bus.router.routes.is_empty());
}

#[rstest::rstest]
#[test]
fn unregister_wakes_waiters_after_releasing_the_route_lock(bus: EventBus) {
    struct Reenter {
        router: Arc<Router>,
        wakes: std::sync::atomic::AtomicUsize,
    }
    impl std::task::Wake for Reenter {
        fn wake(self: Arc<Self>) {
            assert!(!matches!(
                self.router.routes.try_get("wake"),
                dashmap::try_result::TryResult::Locked
            ));
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }
    let consumer = bus
        .consumer_with::<u32>("wake", ConsumerOptions::new().with_capacity(1))
        .unwrap();
    bus.try_send("wake", 1u32).unwrap();
    let mut reservation = bus.router.select::<u32>("wake").unwrap().reserve();
    let reenter = Arc::new(Reenter {
        router: bus.router.clone(),
        wakes: std::sync::atomic::AtomicUsize::new(0),
    });
    let waker = std::task::Waker::from(reenter.clone());
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(reservation.poll_ready(&mut cx).is_pending());
    drop(consumer);
    assert!(reenter.wakes.load(Ordering::SeqCst) > 0);
    assert!(matches!(
        reservation.poll_ready(&mut cx),
        std::task::Poll::Ready(Err(VertxError::ChannelClosed(_)))
    ));
}

#[test]
fn concurrent_close_and_unregister_leave_no_queue_counts() {
    for _ in 0..100 {
        let bus = bus();
        let consumer = bus
            .consumer_with::<u32>("close-race", ConsumerOptions::new().with_capacity(1))
            .unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let gate = barrier.clone();
        let dropper = std::thread::spawn(move || {
            gate.wait();
            drop(consumer);
        });
        barrier.wait();
        bus.close();
        dropper.join().unwrap();
        let snapshot = bus.router.diagnostics.snapshot();
        assert_eq!(snapshot.active_consumers, 0);
        assert!(snapshot.queues.is_empty());
    }
}
