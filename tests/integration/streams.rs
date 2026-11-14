//! Pull consumption, readiness reservations, backpressure and pipe ending policies.

use super::support::{PipeProbe, ProbeSink, TraceOutput, capture, vx};
use rstest::rstest;
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::oneshot;
use vertx_rs::{Context, DeliveryOptions, Vertx, VertxError};

#[rstest]
#[tokio::test]
async fn message_sink_reserves_once_and_only_closes_its_adapter(vx: Vertx) {
    use futures::{Sink, SinkExt};
    let bus = vx.event_bus();
    let mut consumer = bus
        .consumer_with::<u32>("sink", vertx_rs::ConsumerOptions::new().with_capacity(1))
        .unwrap();
    let mut sink = bus
        .sink_with::<u32>("sink", DeliveryOptions::new().header("source", "sink"))
        .unwrap();
    for _ in 0..2 {
        futures::future::poll_fn(|cx| std::pin::Pin::new(&mut sink).poll_ready(cx))
            .await
            .unwrap();
    }
    assert_eq!(vx.diagnostics().reserved_slots, 1);
    assert!(matches!(
        bus.try_send("sink", 0u32),
        Err(VertxError::QueueFull(_))
    ));
    std::pin::Pin::new(&mut sink).start_send(7).unwrap();
    sink.flush().await.unwrap();
    let envelope = consumer.recv().await.unwrap().unwrap();
    assert_eq!(
        envelope.headers().get("source").map(String::as_str),
        Some("sink")
    );
    assert_eq!(envelope.into_body(), 7);
    futures::future::poll_fn(|cx| std::pin::Pin::new(&mut sink).poll_ready(cx))
        .await
        .unwrap();
    sink.close().await.unwrap();
    assert_eq!(vx.diagnostics().reserved_slots, 0);
    bus.send("sink", 8u32).await.unwrap();
    assert_eq!(consumer.recv().await.unwrap().unwrap().into_body(), 8);
    assert!(sink.send(9).await.is_err());
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn message_sink_waits_on_its_selected_consumer_and_drop_releases_wait(vx: Vertx) {
    use futures::{Sink, SinkExt};
    let bus = vx.event_bus();
    let options = vertx_rs::ConsumerOptions::new().with_capacity(1);
    let mut a = bus.consumer_with::<u32>("sticky", options.clone()).unwrap();
    let _b = bus.consumer_with::<u32>("sticky", options.clone()).unwrap();
    bus.try_send("sticky", 1u32).unwrap();
    bus.try_send("sticky", 2u32).unwrap();
    let mut sink = bus.sink::<u32>("sticky").unwrap();
    for _ in 0..2 {
        assert!(
            futures::poll!(futures::future::poll_fn(
                |cx| std::pin::Pin::new(&mut sink).poll_ready(cx)
            ))
            .is_pending()
        );
    }
    let _available = bus.consumer_with::<u32>("sticky", options).unwrap();
    assert!(
        futures::poll!(futures::future::poll_fn(
            |cx| std::pin::Pin::new(&mut sink).poll_ready(cx)
        ))
        .is_pending()
    );
    assert_eq!(vx.diagnostics().waiting_senders, 1);
    a.recv().await.unwrap().unwrap();
    sink.send(3).await.unwrap();
    // Adding a consumer changes subsequent round-robin indices. A is still
    // full here, so dropping the next pending send must release its waiter.
    assert!(
        futures::poll!(futures::future::poll_fn(
            |cx| std::pin::Pin::new(&mut sink).poll_ready(cx)
        ))
        .is_pending()
    );
    drop(sink);
    assert_eq!(a.recv().await.unwrap().unwrap().into_body(), 3);
    assert_eq!(vx.diagnostics().waiting_senders, 0);
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn stream_and_sink_observe_scope_termination_with_retained_consumer(vx: Vertx) {
    use futures::{Sink, StreamExt};
    let (id, ctx) = capture(&vx).await;
    let mut consumer = ctx.event_bus().consumer::<u32>("retained").unwrap();
    let mut sink = vx.event_bus().sink::<u32>("retained").unwrap();
    futures::future::poll_fn(|cx| std::pin::Pin::new(&mut sink).poll_ready(cx))
        .await
        .unwrap();
    vx.undeploy(id).await.unwrap();
    assert!(matches!(
        std::pin::Pin::new(&mut sink).start_send(1),
        Err(VertxError::ChannelClosed(_))
    ));
    assert!(
        matches!(consumer.next().await, Some(Err(VertxError::DeploymentTerminated(i))) if i == id)
    );
    assert!(consumer.next().await.is_none());
    assert!(consumer.recv().await.unwrap().is_none());
    assert_eq!(vx.diagnostics().reserved_slots, 0);
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn consumer_stream_supports_non_unpin_messages_and_global_cancel_wins(vx: Vertx) {
    use futures::{SinkExt, StreamExt};
    struct PinnedMessage {
        value: u32,
        _pin: std::marker::PhantomPinned,
    }
    let bus = vx.event_bus();
    let mut consumer = bus.consumer::<PinnedMessage>("pinned").unwrap();
    let mut sink = bus.sink::<PinnedMessage>("pinned").unwrap();
    sink.send(PinnedMessage {
        value: 42,
        _pin: std::marker::PhantomPinned,
    })
    .await
    .unwrap();
    assert_eq!(consumer.next().await.unwrap().unwrap().body().value, 42);
    sink.send(PinnedMessage {
        value: 43,
        _pin: std::marker::PhantomPinned,
    })
    .await
    .unwrap();
    vx.close().await.unwrap();
    assert!(matches!(
        consumer.next().await,
        Some(Err(VertxError::VertxClosed))
    ));
    assert!(consumer.next().await.is_none());
}

#[tokio::test]
async fn pipe_applies_backpressure_without_prefetch_and_flushes_before_close() {
    let probe = Arc::new(PipeProbe::default());
    let reads = Arc::new(AtomicUsize::new(0));
    let count = reads.clone();
    let source = futures::stream::poll_fn(move |_| {
        let n = count.fetch_add(1, Ordering::SeqCst);
        std::task::Poll::Ready((n < 3).then_some(Ok(n as u32)))
    });
    let mut pipe = Box::pin(vertx_rs::pipe_to(source, ProbeSink::new(probe.clone())));
    for _ in 0..3 {
        assert!(futures::poll!(pipe.as_mut()).is_pending());
    }
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    probe.ready.store(true, Ordering::SeqCst);
    probe.wake.lock().unwrap().take().unwrap().wake();
    pipe.await.unwrap();
    assert_eq!(*probe.values.lock().unwrap(), vec![0, 1, 2]);
    assert_eq!(*probe.calls.lock().unwrap(), vec!["flush", "close"]);
}

#[tokio::test]
async fn pipe_waits_for_flush_and_close_completion() {
    let probe = Arc::new(PipeProbe::default());
    let mut sink = ProbeSink::new(probe.clone());
    let (flush, flushed) = oneshot::channel();
    let (close, closed) = oneshot::channel();
    sink.flush_gate = Some(flushed);
    sink.close_gate = Some(closed);
    let mut pipe = Box::pin(vertx_rs::pipe_to(futures::stream::empty(), sink));
    assert!(futures::poll!(pipe.as_mut()).is_pending());
    assert!(probe.calls.lock().unwrap().is_empty());
    flush.send(()).unwrap();
    assert!(futures::poll!(pipe.as_mut()).is_pending());
    assert_eq!(*probe.calls.lock().unwrap(), vec!["flush"]);
    close.send(()).unwrap();
    pipe.await.unwrap();
    assert_eq!(*probe.calls.lock().unwrap(), vec!["flush", "close"]);
}

#[rstest]
#[case::success(None, false)]
#[case::close_error(None, true)]
#[case::read_error(Some("read"), false)]
#[case::read_and_close_errors(Some("read"), true)]
#[case::readiness_error(Some("ready"), false)]
#[case::readiness_and_close_errors(Some("ready"), true)]
#[case::write_error(Some("write"), false)]
#[case::write_and_close_errors(Some("write"), true)]
#[case::flush_error(Some("flush"), false)]
#[case::flush_and_close_errors(Some("flush"), true)]
#[tokio::test]
async fn pipe_preserves_primary_and_cleanup_failures(
    #[case] failure: Option<&'static str>,
    #[case] close_failure: bool,
) {
    let probe = Arc::new(PipeProbe::default());
    probe.ready.store(true, Ordering::SeqCst);
    let mut sink = ProbeSink::new(probe.clone());
    sink.fail = failure.filter(|stage| *stage != "read");
    sink.fail_close = close_failure;
    let item = if failure == Some("read") {
        Err(VertxError::Other("read".into()))
    } else {
        Ok(1)
    };
    let result = vertx_rs::pipe_to(futures::stream::iter([item]), sink).await;
    match (failure, close_failure, result) {
        (None, false, Ok(())) => {}
        (None, true, Err(VertxError::Other(error))) => assert_eq!(error, "close"),
        (Some(stage), false, Err(VertxError::Other(error))) => assert_eq!(error, stage),
        (Some(stage), true, Err(VertxError::LifecycleFailed { cause, cleanup })) => {
            assert!(matches!(*cause, VertxError::Other(error) if error == stage));
            assert!(matches!(&cleanup[..], [VertxError::Other(error)] if error == "close"));
        }
        (_, _, unexpected) => panic!("unexpected pipe outcome: {unexpected:?}"),
    }
    assert_eq!(probe.calls.lock().unwrap().last(), Some(&"close"));
}

#[rstest]
#[case::eof(false)]
#[case::read_error(true)]
#[tokio::test]
async fn pipe_ending_policy(
    #[case] read_error: bool,
    #[values(false, true)] end_on_success: bool,
    #[values(false, true)] end_on_failure: bool,
) {
    let probe = Arc::new(PipeProbe::default());
    probe.ready.store(true, Ordering::SeqCst);
    let item = if read_error {
        Err(VertxError::Other("read".into()))
    } else {
        Ok(1)
    };
    let result = vertx_rs::pipe_to_with(
        futures::stream::iter([item]),
        ProbeSink::new(probe.clone()),
        vertx_rs::PipeOptions {
            end_on_success,
            end_on_failure,
        },
    )
    .await;
    assert_eq!(result.is_err(), read_error);
    let should_close = if read_error {
        end_on_failure
    } else {
        end_on_success
    };
    let calls = probe.calls.lock().unwrap();
    assert_eq!(calls.contains(&"flush"), !read_error);
    assert_eq!(calls.contains(&"close"), should_close);
}

#[tokio::test]
async fn pipe_can_reuse_sink_when_automatic_ending_is_disabled() {
    use futures::SinkExt;
    let probe = Arc::new(PipeProbe::default());
    probe.ready.store(true, Ordering::SeqCst);
    let mut sink = ProbeSink::new(probe.clone());
    let options = vertx_rs::PipeOptions {
        end_on_success: false,
        end_on_failure: false,
    };
    vertx_rs::pipe_to_with(futures::stream::empty(), &mut sink, options)
        .await
        .unwrap();
    assert_eq!(*probe.calls.lock().unwrap(), vec!["flush"]);
    sink.send(2).await.unwrap();
    let result = vertx_rs::pipe_to_with(
        futures::stream::iter([Err(VertxError::Other("read".into()))]),
        &mut sink,
        options,
    )
    .await;
    assert!(result.is_err());
    assert_eq!(*probe.calls.lock().unwrap(), vec!["flush", "flush"]);
}

#[tokio::test]
async fn pipe_yields_for_always_ready_streams_and_sinks() {
    let probe = Arc::new(PipeProbe::default());
    probe.ready.store(true, Ordering::SeqCst);
    let source = futures::stream::iter((0..130u32).map(Ok));
    let mut pipe = Box::pin(vertx_rs::pipe_to(source, ProbeSink::new(probe.clone())));
    assert!(futures::poll!(pipe.as_mut()).is_pending());
    assert_eq!(probe.values.lock().unwrap().len(), 64);
    pipe.await.unwrap();
    assert_eq!(probe.values.lock().unwrap().len(), 130);
}

#[tokio::test]
async fn pipe_local_futures_keep_context_and_spans_across_interleaved_awaits() {
    use futures::{SinkExt, StreamExt};
    use tracing::Instrument;
    use tracing::instrument::WithSubscriber;
    let output = TraceOutput::default();
    let dispatch = output.dispatch();
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build();
    let (_, a) = capture(&vx).with_subscriber(dispatch.clone()).await;
    let (_, b) = capture(&vx).with_subscriber(dispatch.clone()).await;
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let mut tasks = Vec::new();
    for ctx in [a, b] {
        let span = tracing::dispatcher::with_default(
            &dispatch,
            || tracing::info_span!("local_pipe", owner = %ctx.id()),
        );
        let gate = barrier.clone();
        let target = ctx.clone();
        tasks.push(
            async {
                ctx.spawn(async move {
                    let child = {
                        let id = target.id();
                        let thread = std::thread::current().id();
                        let local = Rc::new(Cell::new(0));
                        let source_state = local.clone();
                        let source = futures::stream::iter([1u32, 2]).then(move |value| {
                            let (gate, state) = (gate.clone(), source_state.clone());
                            async move {
                                gate.wait().await;
                                assert_eq!(Context::current().unwrap().id(), id);
                                assert_eq!(std::thread::current().id(), thread);
                                state.set(state.get() + 1);
                                tracing::info!(probe_context = %id, "local pipe probe");
                                Ok(value)
                            }
                        });
                        let probe = Arc::new(PipeProbe::default());
                        probe.ready.store(true, Ordering::SeqCst);
                        let sink_state = local.clone();
                        let sink = ProbeSink::new(probe.clone()).with(move |value| {
                            let state = sink_state.clone();
                            async move {
                                assert_eq!(Context::current().unwrap().id(), id);
                                state.set(state.get() + 1);
                                Ok(value)
                            }
                        });
                        target.spawn_local(async move {
                            vertx_rs::pipe_to(source, sink).await?;
                            assert_eq!(local.get(), 4);
                            assert_eq!(*probe.values.lock().unwrap(), vec![1, 2]);
                            Ok(())
                        })?
                    };
                    child.await
                })
            }
            .instrument(span)
            .with_subscriber(dispatch.clone())
            .await
            .unwrap(),
        );
    }
    for task in tasks {
        task.await.unwrap();
    }
    vx.close().await.unwrap();
    let text = output.text();
    let lines: Vec<_> = text
        .lines()
        .filter(|line| line.contains("local pipe probe"))
        .collect();
    assert_eq!(lines.len(), 4, "{text}");
    for line in lines {
        let id = line.split("probe_context=").nth(1).unwrap();
        assert!(line.contains(&format!("context_id={id}")), "{line}");
    }
}

#[rstest]
#[tokio::test]
async fn pipe_abort_and_timeout_release_queue_waiters_and_drop_on_context(vx: Vertx) {
    use futures::StreamExt;
    struct DropContext {
        expected: vertx_rs::ContextId,
        dropped: Arc<AtomicUsize>,
    }
    impl Drop for DropContext {
        fn drop(&mut self) {
            assert_eq!(Context::current().unwrap().id(), self.expected);
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (_, ctx) = capture(&vx).await;
    let bus = vx.event_bus();
    let mut consumer = bus
        .consumer_with::<u32>(
            "blocked-pipe",
            vertx_rs::ConsumerOptions::new().with_capacity(1),
        )
        .unwrap();
    bus.try_send("blocked-pipe", 0u32).unwrap();
    let (send, receive) = oneshot::channel();
    let mut send = Some(send);
    let dropped = Arc::new(AtomicUsize::new(0));
    let probe = DropContext {
        expected: ctx.id(),
        dropped: dropped.clone(),
    };
    let source = futures::stream::iter([Ok(1u32)]).inspect(move |_| {
        let _ = &probe;
        send.take().unwrap().send(()).ok();
    });
    let sink = bus.sink("blocked-pipe").unwrap();
    let task = ctx.spawn(vertx_rs::pipe_to(source, sink)).unwrap();
    receive.await.unwrap();
    // This round trip runs after the sender's first synchronous poll.
    ctx.spawn(async { Ok(()) }).unwrap().await.unwrap();
    assert_eq!(vx.diagnostics().waiting_senders, 1);
    task.abort();
    assert!(matches!(task.await, Err(VertxError::TaskCancelled)));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(vx.diagnostics().waiting_senders, 0);
    let result = tokio::time::timeout(
        Duration::from_millis(5),
        vertx_rs::pipe_to(
            futures::stream::iter([Ok(2u32)]),
            bus.sink("blocked-pipe").unwrap(),
        ),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(vx.diagnostics().waiting_senders, 0);
    assert_eq!(consumer.recv().await.unwrap().unwrap().into_body(), 0);
    vx.close().await.unwrap();
}

#[rstest]
#[case::own_scope_cancelled(true)]
#[case::foreign_source_failed(false)]
#[tokio::test]
async fn pipe_distinguishes_own_shutdown_from_foreign_source_failure(
    #[case] same_scope: bool,
    vx: Vertx,
) {
    use futures::TryStreamExt;
    let (owner_id, owner) = capture(&vx).await;
    let (source_id, source_ctx) = if same_scope {
        (owner_id, owner.clone())
    } else {
        capture(&vx).await
    };
    let consumer = source_ctx.event_bus().consumer::<u32>("pipe-stop").unwrap();
    let probe = Arc::new(PipeProbe::default());
    probe.ready.store(true, Ordering::SeqCst);
    let calls = Arc::new(AtomicUsize::new(0));
    let hook = calls.clone();
    owner.set_exception_handler(move |_| {
        hook.fetch_add(1, Ordering::SeqCst);
    });
    let before = vx.diagnostics();
    let task = owner
        .spawn(vertx_rs::pipe_to(
            consumer.map_ok(|env| env.into_body()),
            ProbeSink::new(probe.clone()),
        ))
        .unwrap();
    owner.spawn(async { Ok(()) }).unwrap().await.unwrap();
    vx.undeploy(source_id).await.unwrap();
    assert!(matches!(task.await, Err(VertxError::DeploymentTerminated(id)) if id == source_id));
    assert_eq!(calls.load(Ordering::SeqCst), usize::from(!same_scope));
    assert_eq!(*probe.calls.lock().unwrap(), vec!["close"]);
    let after = vx.diagnostics();
    assert_eq!(after.failed - before.failed, u64::from(!same_scope));
    assert_eq!(after.cancelled - before.cancelled, u64::from(same_scope));
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn message_sink_global_close_releases_cached_and_pending_reservations(vx: Vertx) {
    use futures::{Sink, SinkExt};
    let bus = vx.event_bus();
    let _consumer = bus
        .consumer_with::<()>(
            "closing-sink",
            vertx_rs::ConsumerOptions::new().with_capacity(1),
        )
        .unwrap();
    let mut reserved = bus.sink::<()>("closing-sink").unwrap();
    futures::future::poll_fn(|cx| std::pin::Pin::new(&mut reserved).poll_ready(cx))
        .await
        .unwrap();
    let mut waiting = bus.sink::<()>("closing-sink").unwrap();
    assert!(
        futures::poll!(futures::future::poll_fn(|cx| std::pin::Pin::new(
            &mut waiting
        )
        .poll_ready(cx)))
        .is_pending()
    );
    vx.close().await.unwrap();
    assert!(matches!(
        std::pin::Pin::new(&mut reserved).start_send(()),
        Err(VertxError::VertxClosed)
    ));
    assert!(matches!(
        waiting.send(()).await,
        Err(VertxError::VertxClosed)
    ));
    assert_eq!(
        (
            vx.diagnostics().waiting_senders,
            vx.diagnostics().reserved_slots
        ),
        (0, 0)
    );
}
