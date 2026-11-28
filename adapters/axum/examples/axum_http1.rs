//! 两个 EventLoop 共享 HTTP/1.1 端口，Axum 路由返回处理请求的 EventLoop ID。
//! 运行：`cargo run -p vertx-rs-axum --example axum_http1`。
//! 内置客户端验证同一地址能由两个 EventLoop 服务。
mod support;

use axum::{Router, extract::ConnectInfo, routing::get};
use std::{collections::HashSet, net::SocketAddr, time::Duration};
use vertx_rs::{ExecutionInfo, Result, Vertx, VertxError};

async fn demo(vx: &Vertx) -> Result<()> {
    let router = Router::new().route(
        "/",
        get(|ConnectInfo(peer): ConnectInfo<SocketAddr>| async move {
            assert!(peer.ip().is_loopback());
            let id = ExecutionInfo::current().unwrap().event_loop_id.unwrap();
            tokio::task::yield_now().await;
            assert_eq!(ExecutionInfo::current().unwrap().event_loop_id, Some(id));
            format!("{id:?}")
        }),
    );
    let first = vertx_rs_axum::serve_axum(vx, support::loopback(), router.clone()).await?;
    let addr = first.local_addr();
    let (second, deployment) = support::deploy(vx, addr, router, None).await?;
    let mut owners = HashSet::new();
    for _ in 0..16 {
        let response = support::request(addr)
            .await
            .map_err(|error| VertxError::Other(error.to_string()))?;
        if owners.insert(response.clone()) {
            println!("{addr} -> {response}");
        }
        if owners.len() == 2 {
            break;
        }
    }
    assert_eq!(owners.len(), 2);
    assert!(owners.contains(&format!("{:?}", deployment.event_loop_id.unwrap())));
    let grace = Duration::from_secs(1);
    first.request_shutdown(grace)?;
    second.request_shutdown(grace)?;
    tokio::try_join!(first.wait_closed(), second.wait_closed())?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let vx = Vertx::builder()
        .event_loop_threads(2)
        .worker_threads(1)
        .build()?;
    let result = demo(&vx).await;
    let closed = vx.close().await;
    result.and(closed)
}
