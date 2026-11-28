//! Type-safe request routing, backpressure and subscription lifecycle.

use super::support::{local, vx, worker};
use futures::{SinkExt, StreamExt};
use rstest::rstest;
use std::future::{Future, pending, poll_fn, ready};
use std::pin::pin;
use std::task::Poll;
use std::time::Duration;
use tokio::sync::oneshot;
use vertx_rs::{
    ConsumerOptions, Context, DeliveryOptions, IncomingRequest, RequestAddress, RequestOptions,
    Result, Vertx, VertxError,
};

#[rstest]
#[tokio::test]
async fn a_live_address_rejects_conflicting_protocols(vx: Vertx) {
    const LOOKUP: RequestAddress<u32, String> = RequestAddress::new("lookup");
    const WRONG_BODY: RequestAddress<u16, String> = RequestAddress::new("lookup");
    const WRONG_REPLY: RequestAddress<u32, bool> = RequestAddress::new("lookup");
    let bus = vx.event_bus();
    let address = LOOKUP.address();
    let subscription = LOOKUP.consumer(&bus).unwrap();
    assert_eq!(subscription.address(), address);
    assert!(matches!(
        WRONG_BODY.consumer(&bus),
        Err(VertxError::TypeMismatch(_))
    ));
    assert!(matches!(
        WRONG_REPLY.consumer(&bus),
        Err(VertxError::TypeMismatch(_))
    ));
    assert!(matches!(
        WRONG_BODY.request(&bus, 1).await,
        Err(VertxError::TypeMismatch(_))
    ));
    assert!(matches!(
        WRONG_REPLY.request(&bus, 1).await,
        Err(VertxError::TypeMismatch(_))
    ));
    assert!(matches!(
        bus.consumer::<u32>(address),
        Err(VertxError::TypeMismatch(_))
    ));
    assert!(matches!(
        bus.request::<_, String>(address, 1u32).await,
        Err(VertxError::TypeMismatch(_))
    ));
    assert!(matches!(
        bus.send(address, 1u32).await,
        Err(VertxError::TypeMismatch(_))
    ));
    assert!(matches!(
        bus.try_send(address, 1u32),
        Err(VertxError::TypeMismatch(_))
    ));
    assert!(matches!(
        bus.publish(address, 1u32),
        Err(VertxError::TypeMismatch(_))
    ));
    assert!(matches!(
        bus.sink::<u32>(address).unwrap().send(1).await,
        Err(VertxError::TypeMismatch(_))
    ));
    assert_eq!(vx.diagnostics().active_consumers, 1);
    assert_eq!(vx.diagnostics().queued_messages, 0);

    drop(subscription);
    assert!(matches!(
        LOOKUP.request(&bus, 1).await,
        Err(VertxError::NoConsumer(_))
    ));
    let replacement = bus.consumer::<u32>(address).unwrap();
    assert!(matches!(
        LOOKUP.consumer(&bus),
        Err(VertxError::TypeMismatch(_))
    ));
    assert!(matches!(
        LOOKUP.request(&bus, 1).await,
        Err(VertxError::TypeMismatch(_))
    ));
    drop(replacement);
    let _replacement = WRONG_REPLY.consumer(&bus).unwrap();
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn typed_subscribers_share_an_address_and_can_reply_or_fail(vx: Vertx) {
    const FIND: RequestAddress<u32, String> = RequestAddress::new("find");
    let bus = vx.event_bus();
    let mut first = FIND.consumer(&bus).unwrap();
    let mut second = FIND.consumer(&bus).unwrap();

    let caller = async {
        let response = FIND
            .request_message_with(&bus, 42, RequestOptions::new().header("query", "order"))
            .await
            .unwrap();
        assert_eq!(response.body(), "found");
        assert_eq!(response.headers().get("matched"), Some("yes"));
        let failure = FIND.request(&bus, 43).await;
        assert!(matches!(
            failure,
            Err(VertxError::RecipientFailure { code: 404, .. })
        ));
    };
    let consumers = async {
        let request = first.next().await.unwrap().unwrap();
        let (body, headers, reply) = request.into_parts();
        assert_eq!(body, 42);
        assert_eq!(headers.get("query"), Some("order"));
        reply
            .reply_with(
                "found".to_owned(),
                DeliveryOptions::new().header("matched", "yes"),
            )
            .unwrap();

        let request = second.recv().await.unwrap().unwrap();
        assert_eq!(*request.body(), 43);
        request.fail(404, "missing").unwrap();
    };
    tokio::join!(caller, consumers);
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test(start_paused = true)]
async fn typed_timeout_covers_admission_and_late_replies(vx: Vertx) {
    const FULL: RequestAddress<u32, ()> = RequestAddress::new("full");
    let bus = vx.event_bus();
    let mut consumer = FULL
        .consumer_with(&bus, ConsumerOptions::new().with_capacity(1))
        .unwrap();
    let reply_timeout = Duration::from_secs(1);
    let admission_timeout = Duration::from_millis(5);
    let mut first =
        pin!(FULL.request_with(&bus, 1, RequestOptions::new().with_timeout(reply_timeout)));
    let mut second = pin!(FULL.request_with(
        &bus,
        2,
        RequestOptions::new().with_timeout(admission_timeout)
    ));
    poll_fn(|cx| {
        assert!(first.as_mut().poll(cx).is_pending());
        assert!(second.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(vx.diagnostics().queued_messages, 1);
    assert_eq!(vx.diagnostics().waiting_senders, 1);
    assert!(
        matches!(second.await, Err(VertxError::Timeout(duration)) if duration == admission_timeout)
    );
    assert!(matches!(first.await, Err(VertxError::Timeout(duration)) if duration == reply_timeout));
    let request = consumer.recv().await.unwrap().unwrap();
    assert_eq!(*request.body(), 1);
    assert!(matches!(request.reply(()), Err(VertxError::ReplyClosed)));
    assert_eq!(vx.diagnostics().queued_messages, 0);
    tokio::time::resume();
    vx.close().await.unwrap();
}

struct DropNotice(Option<oneshot::Sender<()>>);
impl Drop for DropNotice {
    fn drop(&mut self) {
        if let Some(done) = self.0.take() {
            let _ = done.send(());
        }
    }
}

async fn echo(request: IncomingRequest<u32, u32>) -> Result<()> {
    let value = *request.body();
    request.reply(value)
}

#[rstest]
#[case::event_loop(false)]
#[case::worker(true)]
#[tokio::test]
async fn handler_handle_can_cancel_one_subscription(#[case] on_worker: bool, vx: Vertx) {
    const CANCEL: RequestAddress<(), ()> = RequestAddress::new("cancel");
    const KEEP: RequestAddress<u32, u32> = RequestAddress::new("keep");
    let (ready_tx, ready_rx) = oneshot::channel();
    let (started, entered) = oneshot::channel();
    let (destroyed, mut destruction) = oneshot::channel();
    let register = move |ctx: Context| {
        let bus = ctx.event_bus();
        let mut state = Some((started, DropNotice(Some(destroyed))));
        let wait = move |request: IncomingRequest<(), ()>| {
            let (started, notice) = state.take().unwrap();
            async move {
                let _notice = notice;
                let _request = request;
                started.send(()).unwrap();
                pending::<Result<()>>().await
            }
        };
        let keep = KEEP.consumer(&bus)?;
        let cancel = CANCEL.consumer(&bus)?;
        let handle = if on_worker {
            drop(keep.handler_worker(echo)?);
            cancel.handler_worker(wait)?
        } else {
            drop(keep.handler(echo)?);
            cancel.handler(wait)?
        };
        ready_tx
            .send((handle, ctx))
            .map_err(|_| VertxError::ReplyClosed)
    };
    if on_worker {
        vx.deploy_worker(move |_| worker(move |ctx| ready(register(ctx))))
            .await
            .unwrap();
    } else {
        vx.deploy(move |_| local(move |ctx| ready(register(ctx))))
            .await
            .unwrap();
    }
    let (handle, ctx) = ready_rx.await.unwrap();
    let bus = vx.event_bus();
    assert_eq!(KEEP.request(&bus, 1).await.unwrap(), 1);

    let b = bus.clone();
    let client = tokio::spawn(async move { CANCEL.request(&b, ()).await });
    tokio::time::timeout(Duration::from_secs(2), entered)
        .await
        .unwrap()
        .unwrap();
    assert!(!client.is_finished());
    handle.abort();
    assert!(matches!(handle.await, Err(VertxError::TaskCancelled)));
    destruction.try_recv().unwrap();
    assert!(matches!(
        client.await.unwrap(),
        Err(VertxError::ReplyClosed)
    ));
    assert!(matches!(
        CANCEL.request(&bus, ()).await,
        Err(VertxError::NoConsumer(_))
    ));
    assert!(ctx.accepts_work());
    assert_eq!(KEEP.request(&bus, 2).await.unwrap(), 2);
    vx.close().await.unwrap();
}
