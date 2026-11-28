//! 字符串地址上传输四字节大端整数，调用方和 handler 各自编解码。
mod support;

use vertx_rs::{Result, Verticle, Vertx, VertxError, VertxHandle};

const ADDRESS: &str = "codec.double";

fn encode(value: u32) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

fn decode(bytes: &[u8]) -> Option<u32> {
    let bytes: [u8; 4] = bytes.try_into().ok()?;
    Some(u32::from_be_bytes(bytes))
}

struct Double;
impl Verticle for Double {
    async fn start(&mut self, vertx: VertxHandle) -> Result<()> {
        vertx
            .event_bus()?
            .consumer::<Vec<u8>>(ADDRESS)?
            .handler(async |request| {
                let Some(value) = decode(request.body()) else {
                    return request.fail(400, "expected exactly four bytes");
                };
                let Some(doubled) = value.checked_mul(2) else {
                    return request.fail(422, "result exceeds u32");
                };
                request.reply(encode(doubled))
            })?;
        Ok(())
    }
}

async fn run(vertx: &Vertx) -> Result<()> {
    vertx.deploy(|| Double).await?;
    let bus = vertx.event_bus()?;
    let response: Vec<u8> = bus.request(ADDRESS, encode(21)).await?;
    assert_eq!(decode(&response), Some(42));

    let malformed = bus.request::<_, Vec<u8>>(ADDRESS, vec![1u8, 2, 3]).await;
    assert!(matches!(
        malformed,
        Err(VertxError::RecipientFailure { code: 400, .. })
    ));
    println!("21 encoded and decoded as 42; malformed input rejected");
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    support::init_tracing();
    let vertx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build()?;
    let outcome = support::bounded(run(&vertx)).await;
    support::finish(&vertx, outcome).await
}
