use axum::Router;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use vertx_rs::{Context, Result, TcpServer, TcpServerOptions, Verticle, Vertx, VertxError};
use vertx_rs_axum::{serve_axum, serve_axum_with_options};

struct Http {
    addr: SocketAddr,
    router: Router,
    options: Option<TcpServerOptions>,
    ready: Option<oneshot::Sender<(TcpServer, Context)>>,
}
impl Verticle for Http {
    async fn start(&mut self, ctx: Context) -> Result<()> {
        let server = match self.options {
            Some(options) => {
                serve_axum_with_options(&ctx, self.addr, self.router.clone(), options).await?
            }
            None => serve_axum(&ctx, self.addr, self.router.clone()).await?,
        };
        self.ready
            .take()
            .unwrap()
            .send((server, ctx))
            .map_err(|_| VertxError::ReplyClosed)
    }
}

pub fn loopback() -> SocketAddr {
    (Ipv4Addr::LOCALHOST, 0).into()
}

pub async fn deploy(
    vx: &Vertx,
    addr: SocketAddr,
    router: Router,
    options: Option<TcpServerOptions>,
) -> Result<(TcpServer, Context)> {
    let (ready, started) = oneshot::channel();
    vx.deploy(move |_| Http {
        addr,
        router,
        options,
        ready: Some(ready),
    })
    .await?;
    started.await.map_err(|_| VertxError::ReplyClosed)
}

pub async fn request(addr: SocketAddr) -> io::Result<String> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut stream = TcpStream::connect(addr).await?;
        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await?;
        let mut raw = String::new();
        stream.read_to_string(&mut raw).await?;
        raw.split_once("\r\n\r\n")
            .filter(|(headers, _)| headers.starts_with("HTTP/1.1 200 "))
            .map(|(_, body)| body.to_owned())
            .ok_or_else(|| io::Error::other("expected HTTP 200"))
    })
    .await
    .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
}
