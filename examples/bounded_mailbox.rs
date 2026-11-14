//! 满队列：try_send 报错，send 等待，publish 丢弃。
//! cargo run --example bounded_mailbox
//! 手动 poll 和接收控制时序；无需靠 sleep 观察背压。
mod support;
use vertx_rs::{ConsumerOptions, Result, Vertx, VertxError};

async fn run(vx: &Vertx) -> Result<()> {
    let bus = vx.event_bus();
    let mut inbox = bus.consumer_with::<u32>("mailbox", ConsumerOptions::new().with_capacity(1))?;
    bus.try_send("mailbox", 1u32)?;
    assert!(matches!(
        bus.try_send("mailbox", 2u32),
        Err(VertxError::QueueFull(_))
    ));
    println!("try_send: QueueFull");

    let mut send = Box::pin(bus.send("mailbox", 3u32));
    assert!(futures::poll!(send.as_mut()).is_pending());
    assert_eq!(vx.diagnostics().waiting_senders, 1);
    println!("send: Pending; waiting_senders=1");

    let dropped = vx.diagnostics().broadcast_dropped;
    bus.publish("mailbox", 99u32)?;
    assert_eq!(vx.diagnostics().broadcast_dropped, dropped + 1);
    println!("publish: dropped one full-queue delivery");

    assert_eq!(
        inbox
            .recv()
            .await?
            .expect("the first message was enqueued")
            .into_body(),
        1
    );
    send.await?;
    assert_eq!(
        inbox
            .recv()
            .await?
            .expect("the waiting send completed")
            .into_body(),
        3
    );
    assert_eq!(vx.diagnostics().waiting_senders, 0);
    assert_eq!(vx.diagnostics().queued_messages, 0);
    println!("received [1, 3]; send completed after capacity was freed");
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    support::init_tracing();
    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(2)
        .try_build()?;
    let outcome = support::bounded(run(&vx)).await;
    support::finish(&vx, outcome).await
}
