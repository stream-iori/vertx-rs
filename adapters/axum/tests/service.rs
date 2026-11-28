#[path = "../examples/support/mod.rs"]
mod support;

use async_trait::async_trait;
use axum::{
    Router,
    extract::{ConnectInfo, State},
    routing::get,
};
use std::{net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Semaphore,
};
use vertx_rs::{Context, Result, TcpServerOptions, Verticle, Vertx};

struct Echo;

#[async_trait(?Send)]
impl Verticle for Echo {
    async fn start(&mut self, ctx: Context) -> Result<()> {
        ctx.event_bus()
            .consumer::<()>("work")?
            .handler(|message| async move { message.reply("pong".to_owned()) })
    }
}

struct RequestState {
    entered: Semaphore,
    finish: Semaphore,
}

async fn graceful() {
    let vx = Vertx::builder()
        .event_loop_threads(2)
        .worker_threads(1)
        .build();
    vx.deploy(|_| Echo).await.unwrap();
    let options = TcpServerOptions {
        max_connections: NonZeroUsize::new(8).unwrap(),
    };
    let request_state = Arc::new(RequestState {
        entered: Semaphore::new(0),
        finish: Semaphore::new(0),
    });
    let router = Router::new()
        .route(
            "/",
            get(
                |ConnectInfo(peer): ConnectInfo<SocketAddr>,
                 State(state): State<Arc<RequestState>>| async move {
                    let before = Context::current().unwrap();
                    let thread = std::thread::current().id();
                    state.entered.add_permits(1);
                    state.finish.acquire().await.unwrap().forget();
                    let response: String = before.event_bus().request("work", ()).await.unwrap();
                    let after = Context::current().unwrap();
                    assert_eq!(before.id(), after.id());
                    assert_eq!(std::thread::current().id(), thread);
                    format!("{response}:{peer}:{:?}", after.event_loop_id().unwrap())
                },
            ),
        )
        .with_state(request_state.clone());
    let (first, ctx0) = support::deploy(&vx, support::loopback(), router, Some(options))
        .await
        .unwrap();
    let addr = first.local_addr();
    let mut client = TcpStream::connect(addr).await.unwrap();
    let peer = client.local_addr().unwrap();
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    request_state.entered.acquire().await.unwrap().forget();

    let router = Router::new().route(
        "/",
        get(|| async { format!("{:?}", Context::current().unwrap().event_loop_id().unwrap()) }),
    );
    let (second, ctx1) = support::deploy(&vx, addr, router, Some(options))
        .await
        .unwrap();
    assert_ne!(ctx0.event_loop_id(), ctx1.event_loop_id());
    let grace = Duration::from_secs(2);
    let mut closing = std::pin::pin!(first.shutdown(grace));
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(closing.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    request_state.finish.add_permits(1);

    let mut response = String::new();
    client.read_to_string(&mut response).await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200 "));
    let id = ctx0.event_loop_id().unwrap();
    assert!(response.ends_with(&format!("pong:{peer}:{id:?}")));
    closing.await.unwrap();

    let response = support::request(addr).await.unwrap();
    assert_eq!(response, format!("{:?}", ctx1.event_loop_id().unwrap()));
    second.shutdown(grace).await.unwrap();
    vx.close().await.unwrap();
}

#[tokio::test]
async fn http_drain_keeps_context_and_event_bus_alive() {
    tokio::time::timeout(Duration::from_secs(8), graceful())
        .await
        .unwrap();
}
