//! 两个普通 Verticle 在各自的 EventLoop 上共享同一个 TCP 端口。
//! 第一个用端口 0，请系统分配地址；第二个用实际地址加入。
//! 内置客户端核对两个字节的回显，每条连接都会打印所属 EventLoop。
//! 运行：`cargo run --example tcp_shared_listener`。
mod support;
use std::io;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use vertx_rs::{Context, EventLoopId, Result, TaskHandle, Verticle, Vertx, VertxError};

struct Echo {
    addr: SocketAddr,
    ready: Option<oneshot::Sender<(SocketAddr, TaskHandle<()>)>>,
    served: Option<oneshot::Sender<EventLoopId>>,
}
impl Verticle for Echo {
    async fn start(&mut self, ctx: Context) -> Result<()> {
        let mut incoming = ctx.listen_tcp(self.addr).await?;
        let addr = incoming.local_addr();
        let served = self.served.take().expect("start called once");
        // start 完成监听注册，后续接收和读写由部署的受管任务负责。
        let task = ctx.spawn_local(async move {
            let (mut socket, peer) = incoming.accept().await?;
            let mut byte = [0];
            socket.read_exact(&mut byte).await.map_err(io_error)?;
            socket.write_all(&byte).await.map_err(io_error)?;
            let event_loop = Context::current().unwrap().event_loop_id().unwrap();
            println!("{addr} accepted {peer} on {event_loop:?}");
            served.send(event_loop).map_err(|_| VertxError::ReplyClosed)
        })?;
        self.ready
            .take()
            .expect("start called once")
            .send((addr, task))
            .map_err(|_| VertxError::ReplyClosed)
    }
}

async fn run(vx: &Vertx) -> Result<()> {
    let mut addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut servers = Vec::new();
    for _ in 0..2 {
        let (ready, bound) = oneshot::channel();
        let (served, event_loop) = oneshot::channel();
        vx.deploy(move |_| Echo {
            addr,
            ready: Some(ready),
            served: Some(served),
        })
        .await?;
        let (bound_addr, task) = bound.await.map_err(|_| VertxError::ReplyClosed)?;
        if addr.port() != 0 {
            assert_eq!(addr, bound_addr);
        }
        addr = bound_addr;
        servers.push((task, event_loop));
    }

    for byte in [41, 42] {
        let mut socket = TcpStream::connect(addr).await.map_err(io_error)?;
        socket.write_all(&[byte]).await.map_err(io_error)?;
        let mut echoed = [0];
        socket.read_exact(&mut echoed).await.map_err(io_error)?;
        assert_eq!(echoed, [byte]);
    }
    let mut loops = Vec::new();
    for (task, event_loop) in servers {
        task.await?;
        loops.push(event_loop.await.map_err(|_| VertxError::ReplyClosed)?);
    }
    assert_ne!(loops[0], loops[1]);
    Ok(())
}

fn io_error(error: io::Error) -> VertxError {
    VertxError::Other(error.to_string())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    support::init_tracing();
    let vx = Vertx::builder()
        .event_loop_threads(2)
        .worker_threads(1)
        .build()?;
    support::finish(&vx, support::bounded(run(&vx)).await).await
}
