//! Typed routing, delivery, request/reply and bounded queue admission.

use super::support::{capture, echo, local, vx};
use rstest::rstest;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use vertx_rs::{DeliveryOptions, Vertx, VertxError};

#[rstest]
#[tokio::test]
async fn verticles_communicate_through_their_bound_buses(vx: Vertx) {
    echo(&vx, "upstream").await;
    vx.deploy(|_| {
        local(|ctx| async move {
            let bus = ctx.event_bus();
            bus.consumer::<String>("caller")?.handler(move |env| {
                let bus = bus.clone();
                async move {
                    let response: String = bus.request("upstream", env.body().clone()).await?;
                    env.reply(response)
                }
            })
        })
    })
    .await
    .unwrap();
    assert_eq!(
        vx.event_bus()
            .request::<_, String>("caller", "hello".to_owned())
            .await
            .unwrap(),
        "hello"
    );
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn bound_consumer_after_undeploy_returns_terminated(vx: Vertx) {
    let (id, ctx) = capture(&vx).await;
    vx.undeploy(id).await.unwrap();
    assert!(matches!(
        ctx.event_bus().consumer::<()>("late"),
        Err(VertxError::DeploymentTerminated(_))
    ));
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn undeploy_removes_only_its_own_subscribers(vx: Vertx) {
    let (first_id, first) = capture(&vx).await;
    let (_, second) = capture(&vx).await;
    let bus = vx.event_bus();
    let mut removed = first.event_bus().consumer::<u32>("shared").unwrap();
    let mut retained = second.event_bus().consumer::<u32>("shared").unwrap();
    let mut unbound = bus.consumer::<u32>("shared").unwrap();

    vx.undeploy(first_id).await.unwrap();

    // Check the registry before polling or dropping the removed receiver.
    assert_eq!(vx.diagnostics().active_consumers, 2);
    bus.publish("shared", 42u32).unwrap();
    assert_eq!(retained.recv().await.unwrap().unwrap().into_body(), 42);
    assert_eq!(unbound.recv().await.unwrap().unwrap().into_body(), 42);
    assert!(matches!(
        removed.recv().await,
        Err(VertxError::DeploymentTerminated(id)) if id == first_id
    ));
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn publish_delivers_to_all_subscribers(vx: Vertx) {
    let bus = vx.event_bus();
    let mut a = bus.consumer::<u32>("pub").unwrap();
    let mut b = bus.consumer::<u32>("pub").unwrap();
    bus.publish("pub", 7u32).unwrap();
    assert_eq!(a.recv().await.unwrap().unwrap().into_body(), 7);
    assert_eq!(b.recv().await.unwrap().unwrap().into_body(), 7);
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn send_with_no_consumer_errors(vx: Vertx) {
    assert!(matches!(
        vx.event_bus().send("missing", ()).await,
        Err(VertxError::NoConsumer(_))
    ));
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn request_timeout_fires(vx: Vertx) {
    let bus = vx.event_bus();
    let _consumer = bus.consumer::<()>("timeout").unwrap();
    assert!(matches!(
        bus.request_with::<_, ()>(
            "timeout",
            (),
            DeliveryOptions::new().with_timeout(Duration::from_millis(5))
        )
        .await,
        Err(VertxError::Timeout(_))
    ));
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn dropping_consumer_unregisters(vx: Vertx) {
    let consumer = vx.event_bus().consumer::<()>("drop").unwrap();
    drop(consumer);
    assert!(matches!(
        vx.event_bus().try_send("drop", ()),
        Err(VertxError::NoConsumer(_))
    ));
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn consumer_handler_dispatches_envelopes(vx: Vertx) {
    echo(&vx, "handler").await;
    for n in 0..10 {
        assert_eq!(
            vx.event_bus()
                .request::<_, String>("handler", n.to_string())
                .await
                .unwrap(),
            n.to_string()
        );
    }
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn consumer_handler_unregisters_on_undeploy(vx: Vertx) {
    let id = echo(&vx, "handler").await;
    vx.undeploy(id).await.unwrap();
    assert!(matches!(
        vx.event_bus().try_send("handler", "x".to_owned()),
        Err(VertxError::NoConsumer(_))
    ));
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn message_passing_between_handlers(vx: Vertx) {
    vx.deploy(|_| {
        local(|ctx| async move {
            ctx.event_bus()
                .consumer::<u32>("double")?
                .handler(|env| async move {
                    let n = *env.body() * 2;
                    env.reply(n)
                })?;
            let bus = ctx.event_bus();
            bus.consumer::<u32>("forward")?.handler(move |env| {
                let bus = bus.clone();
                async move {
                    let n: u32 = bus.request("double", *env.body()).await?;
                    env.reply(n)
                }
            })
        })
    })
    .await
    .unwrap();
    assert_eq!(
        vx.event_bus()
            .request::<_, u32>("forward", 21u32)
            .await
            .unwrap(),
        42
    );
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn delivery_options_headers_round_trip(vx: Vertx) {
    vx.deploy(|_| {
        local(|ctx| async move {
            ctx.event_bus()
                .consumer::<()>("headers")?
                .handler(|env| async move {
                    assert_eq!(
                        env.headers().get_all("tag").collect::<Vec<_>>(),
                        vec!["a", "b"]
                    );
                    env.reply_with(42u32, DeliveryOptions::new().header("reply", "yes"))
                })
        })
    })
    .await
    .unwrap();
    let reply = vx
        .event_bus()
        .request_message_with::<_, u32>(
            "headers",
            (),
            DeliveryOptions::new().header("tag", "a").header("tag", "b"),
        )
        .await
        .unwrap();
    assert_eq!(*reply.body(), 42);
    assert_eq!(reply.headers().get("reply").unwrap(), "yes");
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn request_timeout_includes_waiting_for_full_queue(vx: Vertx) {
    let bus = vx.event_bus();
    let _consumer = bus.consumer::<u32>("full").unwrap();
    for n in 0..256u32 {
        bus.try_send("full", n).unwrap();
    }
    assert!(matches!(
        bus.try_send("full", 999u32),
        Err(VertxError::QueueFull(_))
    ));
    let result = tokio::time::timeout(
        Duration::from_millis(200),
        bus.request_with::<_, ()>(
            "full",
            999u32,
            DeliveryOptions::new().with_timeout(Duration::from_millis(5)),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(VertxError::Timeout(_))));
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn recipient_failure_and_late_reply_are_explicit(vx: Vertx) {
    vx.deploy(|_| {
        local(|ctx| async move {
            ctx.event_bus()
                .consumer::<()>("business")?
                .handler(|env| async { env.fail(409, "conflict") })
        })
    })
    .await
    .unwrap();
    assert!(matches!(
        vx.event_bus().request::<_, ()>("business", ()).await,
        Err(VertxError::RecipientFailure { code: 409, .. })
    ));
    let bus = vx.event_bus();
    let mut consumer = bus.consumer::<()>("late").unwrap();
    let b = bus.clone();
    let job = tokio::spawn(async move {
        b.request_with::<_, ()>(
            "late",
            (),
            DeliveryOptions::new().with_timeout(Duration::from_millis(5)),
        )
        .await
    });
    let env = consumer.recv().await.unwrap().unwrap();
    assert!(matches!(job.await.unwrap(), Err(VertxError::Timeout(_))));
    assert!(matches!(env.reply(()), Err(VertxError::ReplyClosed)));
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn type_conflicts_are_rejected_at_registration(vx: Vertx) {
    let bus = vx.event_bus();
    let a = bus.consumer::<u32>("typed").unwrap();
    assert!(matches!(
        bus.consumer::<String>("typed"),
        Err(VertxError::TypeMismatch(_))
    ));
    assert!(matches!(
        bus.try_send("typed", "bad".to_owned()),
        Err(VertxError::TypeMismatch(_))
    ));
    drop(a);
    assert!(bus.consumer::<String>("typed").is_ok());
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn broadcast_drops_only_full_subscriber_and_send_round_robins(vx: Vertx) {
    let bus = vx.event_bus();
    let mut a = bus.consumer::<u32>("pub").unwrap();
    let mut b = bus.consumer::<u32>("pub").unwrap();
    for n in 0..256u32 {
        bus.publish("pub", n).unwrap();
        assert_eq!(b.recv().await.unwrap().unwrap().into_body(), n);
    }
    bus.publish("pub", 999u32).unwrap();
    assert_eq!(b.recv().await.unwrap().unwrap().into_body(), 999);
    for n in 0..256u32 {
        assert_eq!(a.recv().await.unwrap().unwrap().into_body(), n);
    }
    bus.send("pub", 1u32).await.unwrap();
    bus.send("pub", 2u32).await.unwrap();
    assert_eq!(a.recv().await.unwrap().unwrap().into_body(), 1);
    assert_eq!(b.recv().await.unwrap().unwrap().into_body(), 2);
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn closing_unblocks_senders_waiting_for_capacity(vx: Vertx) {
    let bus = vx.event_bus();
    let _c = bus.consumer::<()>("full").unwrap();
    for _ in 0..256 {
        bus.try_send("full", ()).unwrap();
    }
    let b = bus.clone();
    let send = tokio::spawn(async move { b.send("full", ()).await });
    vx.close().await.unwrap();
    assert!(matches!(send.await.unwrap(), Err(VertxError::VertxClosed)));
}

#[tokio::test]
async fn consumer_waits_for_message_future_completion_before_next_message() {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(2)
        .build();
    let (entered, started) = oneshot::channel();
    let mut entered = Some(entered);
    let (release, wait) = oneshot::channel();
    let mut wait = Some(wait);
    let (done, finished) = oneshot::channel();
    let mut done = Some(done);
    let order = Arc::new(Mutex::new(Vec::new()));
    let recorded = order.clone();
    vx.deploy(move |_| {
        local(move |ctx| async move {
            ctx.event_bus()
                .consumer::<u8>("serial-future")?
                .handler(move |env| {
                    let wait = if *env.body() == 1 { wait.take() } else { None };
                    let entered = entered.take();
                    let done = if *env.body() == 2 { done.take() } else { None };
                    let order = recorded.clone();
                    async move {
                        let value = *env.body();
                        order.lock().unwrap().push(value);
                        if let Some(entered) = entered {
                            entered.send(()).ok();
                        }
                        if let Some(wait) = wait {
                            wait.await.unwrap();
                        }
                        if let Some(done) = done {
                            done.send(()).ok();
                            Ok(())
                        } else {
                            env.reply(value)
                        }
                    }
                })
        })
    })
    .await
    .unwrap();
    let bus = vx.event_bus();
    let first_bus = bus.clone();
    let first = tokio::spawn(async move { first_bus.request::<_, u8>("serial-future", 1u8).await });
    started.await.unwrap();
    // Enqueue the second message, then run another task on the same EventLoop.
    bus.send("serial-future", 2u8).await.unwrap();
    let (_, ctx) = capture(&vx).await;
    ctx.spawn(async { Ok(()) }).unwrap().await.unwrap();
    assert_eq!(*order.lock().unwrap(), vec![1]);
    release.send(()).unwrap();
    assert_eq!(first.await.unwrap().unwrap(), 1);
    finished.await.unwrap();
    assert_eq!(*order.lock().unwrap(), vec![1, 2]);
    vx.close().await.unwrap();
}

#[rstest]
#[case::zero(0)]
#[case::above_semaphore_limit(usize::MAX)]
#[tokio::test]
async fn invalid_consumer_capacity_is_rejected(#[case] capacity: usize, vx: Vertx) {
    assert!(matches!(
        vx.event_bus().consumer_with::<()>(
            "invalid",
            vertx_rs::ConsumerOptions::new().with_capacity(capacity)
        ),
        Err(VertxError::InvalidOptions(_))
    ));
    vx.close().await.unwrap();
}

#[tokio::test]
async fn consumer_capacity_and_waiting_sender_accounting() {
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .diagnostics(vertx_rs::DiagnosticsOptions {
            queue_details: true,
            ..Default::default()
        })
        .build();
    let bus = vx.event_bus();
    let mut consumer = bus
        .consumer_with::<u32>("small", vertx_rs::ConsumerOptions::new().with_capacity(1))
        .unwrap();
    bus.try_send("small", 1u32).unwrap();
    let mut send = Box::pin(bus.send("small", 2u32));
    assert!(futures::poll!(send.as_mut()).is_pending());
    assert!(futures::poll!(send.as_mut()).is_pending());
    let snapshot = vx.diagnostics();
    assert_eq!(
        (
            snapshot.queued_messages,
            snapshot.reserved_slots,
            snapshot.waiting_senders,
            snapshot.queue_full_events
        ),
        (1, 0, 1, 1)
    );
    assert_eq!(snapshot.queues[0].capacity, 1);
    drop(send);
    assert_eq!(vx.diagnostics().waiting_senders, 0);
    assert_eq!(consumer.recv().await.unwrap().unwrap().into_body(), 1);
    assert_eq!(vx.diagnostics().queued_messages, 0);
    drop(consumer);
    assert!(vx.diagnostics().queues.is_empty());
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn publish_does_not_clone_for_a_full_consumer(vx: Vertx) {
    struct CountClone(Arc<AtomicUsize>);
    impl Clone for CountClone {
        fn clone(&self) -> Self {
            self.0.fetch_add(1, Ordering::SeqCst);
            Self(self.0.clone())
        }
    }
    let bus = vx.event_bus();
    let full = bus
        .consumer_with::<CountClone>("copies", vertx_rs::ConsumerOptions::new().with_capacity(1))
        .unwrap();
    let clones = Arc::new(AtomicUsize::new(0));
    bus.try_send("copies", CountClone(clones.clone())).unwrap();
    let mut available = bus
        .consumer_with::<CountClone>("copies", vertx_rs::ConsumerOptions::new().with_capacity(2))
        .unwrap();
    bus.publish("copies", CountClone(clones.clone())).unwrap();
    assert_eq!(clones.load(Ordering::SeqCst), 1);
    assert!(available.recv().await.unwrap().is_some());
    assert_eq!(vx.diagnostics().broadcast_dropped, 1);
    drop(full);
    drop(available);
    vx.close().await.unwrap();
}
