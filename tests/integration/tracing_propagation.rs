//! Per-poll/drop Dispatch and Span propagation across Contexts and threads.

use super::support::{PipeProbe, ProbeSink, TraceOutput, capture, local, vx, worker};
use rstest::rstest;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use vertx_rs::{Context, RequestOptions, Result, Vertx, VertxError};

#[tokio::test]
async fn span_creation_can_reenter_deployment_admission() {
    use tracing_subscriber::prelude::*;

    if !super::support::in_test_subprocess(
        "tracing_propagation::span_creation_can_reenter_deployment_admission",
    ) {
        return;
    }

    struct Probe(Arc<AtomicUsize>);
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Probe {
        fn on_new_span(
            &self,
            attributes: &tracing::span::Attributes<'_>,
            _: &tracing::span::Id,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if attributes.metadata().name() == "context_task"
                && let Some(ctx) = Context::current()
            {
                assert!(ctx.is_alive());
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    let reentries = Arc::new(AtomicUsize::new(0));
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry().with(Probe(reentries.clone())),
    )
    .unwrap();
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build()
        .unwrap();
    vx.deploy(|_| local(|ctx| async move { ctx.spawn_local(async { Ok(()) })?.await }))
        .await
        .unwrap();
    assert_eq!(reentries.load(Ordering::Relaxed), 1);
    vx.close().await.unwrap();
}

#[tokio::test]
async fn scoped_tracing_follows_interleaved_contexts_without_span_leaks() {
    use tracing::Instrument;
    use tracing::instrument::WithSubscriber;
    let output = TraceOutput::default();
    let dispatch = output.dispatch();
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build()
        .unwrap();
    let (_, a) = capture(&vx).with_subscriber(dispatch.clone()).await;
    let (_, b) = capture(&vx).with_subscriber(dispatch.clone()).await;
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let alpha = tracing::dispatcher::with_default(&dispatch, || tracing::info_span!("alpha"));
    let beta = tracing::dispatcher::with_default(&dispatch, || tracing::info_span!("beta"));
    let gate = barrier.clone();
    let aid = a.id();
    let ah = async {
        a.spawn(async move {
            tracing::info!(probe = "alpha", "user probe before wait");
            gate.wait().await;
            assert_eq!(Context::current().unwrap().id(), aid);
            tracing::info!(probe = "alpha", "user probe after wait");
            Ok(())
        })
    }
    .instrument(alpha)
    .with_subscriber(dispatch.clone())
    .await
    .unwrap();
    let bid = b.id();
    let bh = async {
        b.spawn(async move {
            tracing::info!(probe = "beta", "user probe before wait");
            barrier.wait().await;
            assert_eq!(Context::current().unwrap().id(), bid);
            tracing::info!(probe = "beta", "user probe after wait");
            Ok(())
        })
    }
    .instrument(beta)
    .with_subscriber(dispatch.clone())
    .await
    .unwrap();
    ah.await.unwrap();
    bh.await.unwrap();
    vx.close().with_subscriber(dispatch).await.unwrap();
    let text = output.text();
    let lines: Vec<_> = text
        .lines()
        .filter(|line| line.contains("user probe"))
        .collect();
    assert_eq!(lines.len(), 4, "{text}");
    for line in lines {
        if line.contains("probe=\"alpha\"") {
            assert!(
                line.contains(&format!("context_id={aid}")) && !line.contains("beta:"),
                "{line}"
            );
        } else {
            assert!(
                line.contains(&format!("context_id={bid}")) && !line.contains("alpha:"),
                "{line}"
            );
        }
    }
    assert!(text.contains("task finished after user drop"));
    assert!(Context::current().is_none());
}

#[tokio::test]
async fn diagnostic_logs_separate_admission_queue_timeout_and_user_hook() {
    use tracing::instrument::WithSubscriber;
    let output = TraceOutput::default();
    let dispatch = output.dispatch();
    async {
        let vx = vx();
        let mut consumer = vx.event_bus().consumer::<()>("stages").unwrap();
        for _ in 0..256 {
            vx.event_bus().try_send("stages", ()).unwrap();
        }
        vx.event_bus().publish("stages", ()).unwrap();
        let reply: Result<()> = vx
            .event_bus()
            .request_with(
                "stages",
                (),
                RequestOptions::new().with_timeout(Duration::from_millis(5)),
            )
            .await;
        assert!(matches!(reply, Err(VertxError::Timeout(_))));
        assert_eq!(vx.diagnostics().request_timeouts, 1);
        assert_eq!(vx.diagnostics().broadcast_dropped, 1);
        let envelope = consumer.recv().await.unwrap().unwrap();
        drop(envelope);
        vx.event_bus().send("stages", ()).await.unwrap();
        let (_, ctx) = capture(&vx).await;
        let hook = Arc::new(AtomicUsize::new(0));
        let calls = hook.clone();
        ctx.set_exception_handler(move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
        });
        assert!(
            ctx.spawn(async { Err(VertxError::Other("diagnostic-hook-error".into())) })
                .unwrap()
                .await
                .is_err()
        );
        assert_eq!(hook.load(Ordering::SeqCst), 1);
        vx.close().await.unwrap();
    }
    .with_subscriber(dispatch)
    .await;
    let text = output.text();
    assert!(
        text.contains("admission_wait=") && text.contains("queue_residence="),
        "{text}"
    );
    assert!(
        text.lines()
            .any(|line| line.contains("request timed out") && line.contains("request_id="))
    );
    assert_eq!(
        text.lines()
            .filter(|line| line.contains("framework operation failed")
                && line.contains("diagnostic-hook-error"))
            .count(),
        1
    );
    assert!(text.contains("worker_join") && text.contains("event_loop_join"));
}

#[rstest]
#[tokio::test]
async fn scoped_tracing_follows_worker_migration_and_restores_context(vx: Vertx) {
    use tracing::Instrument;
    use tracing::instrument::WithSubscriber;
    let output = TraceOutput::default();
    let dispatch = output.dispatch();
    let (send, receive) = oneshot::channel();
    vx.deploy_worker(move |_| {
        worker(move |ctx| async move {
            send.send(ctx).ok();
            Ok(())
        })
    })
    .await
    .unwrap();
    let ctx = receive.await.unwrap();
    let (send, receive) = oneshot::channel();
    let mut send = Some(send);
    let mut first = None;
    let mut delivered = false;
    let context_id = ctx.id();
    let span = tracing::dispatcher::with_default(&dispatch, || tracing::info_span!("worker_probe"));
    let task = async {
        ctx.spawn(vertx_rs::pipe_to(
            futures::stream::poll_fn(move |cx| {
                if delivered {
                    return std::task::Poll::Ready(None);
                }
                assert_eq!(Context::current().unwrap().id(), context_id);
                let current = std::thread::current().id();
                tracing::info!(thread_id = ?current, "migration probe");
                if let Some(first) = first {
                    assert_ne!(first, current);
                    delivered = true;
                    std::task::Poll::Ready(Some(Ok(7u32)))
                } else {
                    first = Some(current);
                    send.take()
                        .unwrap()
                        .send((current, cx.waker().clone()))
                        .ok();
                    std::task::Poll::Pending
                }
            }),
            {
                let probe = Arc::new(PipeProbe::default());
                probe.ready.store(true, Ordering::SeqCst);
                ProbeSink::new(probe)
            },
        ))
    }
    .instrument(span)
    .with_subscriber(dispatch)
    .await
    .unwrap();
    let (first, wake) = receive.await.unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let (release, wait) = std::sync::mpsc::channel();
    let wait = Arc::new(Mutex::new(wait));
    let (blocked, blocking) = oneshot::channel();
    let blocked = Arc::new(Mutex::new(Some(blocked)));
    let mut jobs = Vec::new();
    for _ in 0..2 {
        let (ctx, barrier, wait, blocked) =
            (ctx.clone(), barrier.clone(), wait.clone(), blocked.clone());
        jobs.push(tokio::spawn(async move {
            ctx.execute_blocking_unordered(move || {
                barrier.wait();
                if std::thread::current().id() == first {
                    blocked.lock().unwrap().take().unwrap().send(()).ok();
                    wait.lock().unwrap().recv().unwrap();
                }
            })
            .await
        }));
    }
    blocking.await.unwrap();
    wake.wake();
    let result = tokio::time::timeout(Duration::from_secs(2), task).await;
    release.send(()).unwrap();
    result.unwrap().unwrap();
    for job in jobs {
        job.await.unwrap().unwrap();
    }
    vx.close().await.unwrap();
    let text = output.text();
    let lines: Vec<_> = text
        .lines()
        .filter(|line| line.contains("migration probe"))
        .collect();
    assert_eq!(lines.len(), 2, "{text}");
    for line in lines {
        assert!(
            line.contains("worker_probe:") && line.contains(&format!("context_id={context_id}")),
            "{line}"
        );
    }
    assert!(Context::current().is_none());
}

#[rstest]
#[tokio::test]
async fn handler_destructor_uses_message_dispatch_even_when_consumer_has_another_subscriber(
    vx: Vertx,
) {
    use tracing::instrument::WithSubscriber;
    struct DropProbe(vertx_rs::ContextId);
    impl Drop for DropProbe {
        fn drop(&mut self) {
            assert_eq!(Context::current().unwrap().id(), self.0);
            tracing::info!("message destructor probe");
        }
    }
    let receiver = TraceOutput::default();
    let sender = TraceOutput::default();
    let (send, ready) = oneshot::channel();
    vx.deploy(move |_| {
        local(move |ctx| async move {
            let mut send = Some(send);
            let id = ctx.id();
            ctx.event_bus()
                .consumer::<()>("drop-trace")?
                .handler(move |_| {
                    let send = send.take();
                    async move {
                        let _probe = DropProbe(id);
                        send.unwrap().send(()).ok();
                        futures::future::pending::<()>().await;
                        Ok(())
                    }
                })
        })
    })
    .with_subscriber(receiver.dispatch())
    .await
    .unwrap();
    vx.event_bus()
        .send("drop-trace", ())
        .with_subscriber(sender.dispatch())
        .await
        .unwrap();
    ready.await.unwrap();
    vx.close().await.unwrap();
    assert!(sender.text().contains("message destructor probe"));
    assert!(!receiver.text().contains("message destructor probe"));
}

#[rstest]
#[tokio::test]
async fn message_span_names_receiving_context_separately_from_sender_ancestry(vx: Vertx) {
    use tracing::instrument::WithSubscriber;
    let output = TraceOutput::default();
    let dispatch = output.dispatch();
    let (send, receive) = oneshot::channel();
    vx.deploy(move |_| {
        local(move |ctx| async move {
            let id = ctx.id();
            send.send(id).ok();
            ctx.event_bus()
                .consumer::<()>("trace-hop")?
                .handler(move |env| async move {
                    assert_eq!(Context::current().unwrap().id(), id);
                    tracing::info!("receiver execution probe");
                    env.reply(())
                })
        })
    })
    .with_subscriber(dispatch.clone())
    .await
    .unwrap();
    let receiver_id = receive.await.unwrap();
    let (_, source) = capture(&vx).with_subscriber(dispatch.clone()).await;
    assert_ne!(source.id(), receiver_id);
    let bus = source.event_bus();
    let task = async { source.spawn(async move { bus.request::<_, ()>("trace-hop", ()).await }) }
        .with_subscriber(dispatch)
        .await
        .unwrap();
    task.await.unwrap();
    vx.close().await.unwrap();
    let text = output.text();
    let line = text
        .lines()
        .find(|line| line.contains("receiver execution probe"))
        .unwrap();
    let handler_fields = line
        .split("message_handler{")
        .nth(1)
        .unwrap()
        .split('}')
        .next()
        .unwrap();
    assert!(
        handler_fields.contains(&format!("context_id={receiver_id}")),
        "{line}"
    );
    assert!(handler_fields.contains("task_id=task#"), "{line}");
}
