//! Axum routing on vertx-rs managed HTTP/1.1 connections.
use axum::{Extension, Router, extract::ConnectInfo};
use hyper::server::conn::http1;
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    service::TowerToHyperService,
};
use std::{net::SocketAddr, pin::pin, time::Duration};
use vertx_rs::{Context, Result, TcpConnection, TcpServer, TcpServerOptions, VertxError};

/// Serve a router on the owning EventLoop with the default TCP options.
///
/// ```
/// use axum::{Router, routing::get};
/// use std::net::SocketAddr;
/// use vertx_rs::{Context, Result, TcpServer};
///
/// async fn start(ctx: &Context, addr: SocketAddr) -> Result<TcpServer> {
///     let router = Router::new().route("/", get(|| async { "hello" }));
///     vertx_rs_axum::serve_axum(ctx, addr, router).await
/// }
/// ```
pub async fn serve_axum(ctx: &Context, addr: SocketAddr, router: Router) -> Result<TcpServer> {
    serve_axum_with_options(ctx, addr, router, TcpServerOptions::default()).await
}

/// Serve HTTP/1.1 with explicit TCP service options.
///
/// Await the returned server's shutdown before closing Vertx to keep application
/// services available while in-flight HTTP requests finish.
pub async fn serve_axum_with_options(
    ctx: &Context,
    addr: SocketAddr,
    router: Router,
    options: TcpServerOptions,
) -> Result<TcpServer> {
    ctx.serve_tcp(addr, options, async move |connection: TcpConnection<'_>| {
        let router = router
            .clone()
            .layer(Extension(ConnectInfo(connection.peer_addr())));
        let shutdown = connection.shutdown_token();
        let mut http = http1::Builder::new();
        http.timer(TokioTimer::new())
            .header_read_timeout(Duration::from_secs(30));
        let mut http =
            pin!(http.serve_connection(TokioIo::new(connection), TowerToHyperService::new(router)));
        let result = tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                http.as_mut().graceful_shutdown();
                http.await
            }
            result = http.as_mut() => result,
        };
        result.map_err(|error| VertxError::Other(format!("HTTP/1.1 connection: {error}")))
    })
    .await
}
