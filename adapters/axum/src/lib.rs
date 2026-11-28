//! Axum routing on vertx-rs managed HTTP/1.1 connections.
use axum::{Extension, Router, extract::ConnectInfo};
use hyper::server::conn::http1;
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    service::TowerToHyperService,
};
use std::{future::Future, net::SocketAddr, pin::pin, time::Duration};
use vertx_rs::{Result, TcpConnection, TcpServer, TcpServerOptions, VertxError, VertxHandle};

/// Serve a router on the owning EventLoop with the default TCP options.
///
/// ```
/// use axum::{Router, routing::get};
/// use std::net::SocketAddr;
/// use vertx_rs::{Result, TcpServer, Vertx};
///
/// async fn start(vertx: &Vertx, addr: SocketAddr) -> Result<TcpServer> {
///     let router = Router::new().route("/", get(|| async { "hello" }));
///     vertx_rs_axum::serve_axum(vertx, addr, router).await
/// }
/// ```
pub fn serve_axum(
    vertx: impl Into<VertxHandle>,
    addr: SocketAddr,
    router: Router,
) -> impl Future<Output = Result<TcpServer>> + Send + 'static {
    serve_axum_with_options(vertx, addr, router, TcpServerOptions::default())
}

/// Serve HTTP/1.1 with explicit TCP service options.
///
/// Await the returned server's shutdown before closing Vertx to keep application
/// services available while in-flight HTTP requests finish.
pub fn serve_axum_with_options(
    vertx: impl Into<VertxHandle>,
    addr: SocketAddr,
    router: Router,
    options: TcpServerOptions,
) -> impl Future<Output = Result<TcpServer>> + Send + 'static {
    vertx
        .into()
        .serve_tcp(addr, options, async move |connection: TcpConnection<'_>| {
            let router = router
                .clone()
                .layer(Extension(ConnectInfo(connection.peer_addr())));
            let shutdown = connection.shutdown_token();
            let mut http = http1::Builder::new();
            http.timer(TokioTimer::new())
                .header_read_timeout(Duration::from_secs(30));
            let mut http = pin!(
                http.serve_connection(TokioIo::new(connection), TowerToHyperService::new(router))
            );
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
}
