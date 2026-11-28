# 示例与共享状态

[返回文档索引](index.md)

核心示例可独立运行，无需外部服务，均通过可失败的 `build()` 创建实例，业务阶段最多等待 5 秒，
最后显式 `close().await`；业务和关闭同时失败时，通过 `LifecycleFailed` 保留两者。
三个综合示例使用两个 EventLoop 和两个 Worker，共享 TCP 示例使用两个 EventLoop 和一个 Worker。
核心示例共用 `support/mod.rs` 中的日志、超时和关闭处理。
成功关闭后，共用代码会检查部署、任务、消费者、队列及 blocking 活跃计数归零。

## 选择示例

### 入门与综合示例

| 示例 | 场景 | 重点 API |
|---|---|---|
| [ping_pong](../examples/ping_pong.rs) | 请求服务 + 周期心跳 + 两个观察者 | request/reply/fail、headers、publish、periodic |
| [pipeline](../examples/pipeline.rs) | 有限数据源 → 两个校验节点 → Worker 汇总 | deploy_with、强类型配置、pipe_to、Stream、blocking、diagnostics |
| [verticle_to_verticle](../examples/verticle_to_verticle.rs) | 类型化服务组合与独立取消 handler | RequestAddress、try_join_all、join_all、select_ok、TaskHandle |

### 消息与背压

| 示例 | 场景 | 重点 API |
|---|---|---|
| [bounded_mailbox](../examples/bounded_mailbox.rs) | 满队列下三种发送策略 | ConsumerOptions、try_send、send、publish、diagnostics |
| [deferred_reply](../examples/deferred_reply.rs) | 把类型化回复能力交给后台任务 | into_parts、TypedReplyHandle、spawn_local、reply_with |
| [eventbus_codec](../examples/eventbus_codec.rs) | 经由 EventBus 传输和校验字节 | Vec<u8>、to_be_bytes、from_be_bytes、fail |

### 生命周期与外部接入

| 示例 | 场景 | 重点 API |
|---|---|---|
| [deployment_rollback](../examples/deployment_rollback.rs) | 第二个实例启动失败，整批回滚 | deploy_with、stop、DeploymentFailed |
| [external_dispatch](../examples/external_dispatch.rs) | 外部 OS 线程派发实例级任务和指定部署的任务 | VertxHandle::spawn、Deployment::spawn、current_execution |
| [graceful_shutdown](../examples/graceful_shutdown.rs) | 内部发起关闭，外部超时后继续等清理 | request_close、close、wait_closed、execute_blocking |
| [error_handling](../examples/error_handling.rs) | 业务失败、异常及 panic 隔离 | fail、set_exception_handler、clear_exception_handler |

### TCP 与 HTTP 示例

| 示例 | 场景 | 重点 API |
|---|---|---|
| [tcp_shared_listener](../examples/tcp_shared_listener.rs) | 两个 EventLoop 共用一个 TCP 端口并回显 | listen_tcp、local_addr、accept、spawn_local |
| [axum_http1](../adapters/axum/examples/axum_http1.rs) | 应用与 Verticle 共用一个 HTTP/1.1 端口，路由返回 EventLoop ID | serve_axum、ConnectInfo、TcpServer::request_shutdown、wait_closed |

## 运行方式

在仓库根目录执行：

```sh
cargo run --example ping_pong
cargo run --example pipeline
cargo run --example verticle_to_verticle
cargo run --example bounded_mailbox
cargo run --example deployment_rollback
cargo run --example external_dispatch
cargo run --example graceful_shutdown
cargo run --example deferred_reply
cargo run --example eventbus_codec
cargo run --example error_handling
cargo run --example tcp_shared_listener
```

独立的 Axum 适配 crate 提供共享端口的 HTTP/1.1 示例。

```sh
cargo run -p vertx-rs-axum --example axum_http1
```

需要内部日志时，可以指定级别和输出格式：

```sh
RUST_LOG=vertx_rs=debug cargo run --example pipeline
RUST_LOG=vertx_rs=debug LOG_FORMAT=json cargo run --example ping_pong
```

`LOG_FORMAT=json` 控制 tracing 日志格式；示例的业务说明仍通过普通文本打印。
示例包含结果断言，出现非预期结果会报错，不需要人工从日志猜测是否成功。

## ping_pong：请求与心跳广播

```text
main ── request ──→ Pinger（Rc<Cell> 状态）── reply ──→ main
                         │
                     periodic × 3
                         │ publish
                    ┌────┴────┐
                  monitor    audit
```

- 两个观察者先注册，随后启动生产者，分别收集同样的 `[1, 2, 3]` 心跳序列。
- 在 start 中创建 timer 和 bus，再直接移入心跳任务。三次 tick 后显式取消 Periodic。该任务结束不会撤销 Pinger，之后仍能请求服务。
- 正常请求返回 `pong#1` 和响应 headers；非法输入返回 `RecipientFailure`。
- 慢请求触发 timeout；随后发送一个排在慢请求后面的请求，确认迟到 reply 路径实际执行。
- factory/start、handler 与 stop 的实际线程及 Context 保持一致，状态可用 `Rc<Cell>`。

广播是尽力投递。本例每订阅者默认容量为 256，仅发送三条消息；不代表满队列时也可靠投递。
超时不会撤回已入队消息，迟到回复的 `ReplyClosed` 由服务显式处理。

## pipeline：配置、多实例与背压

```text
有限源 1..=5 ── pipe_to ──→ Hasher × 2 ── send ──→ Worker Sink
                            共享 HashConfig        Stream.take(5)
                                   │
                       ordered / unordered blocking
                                   │
                            固定两个 OS Worker
```

- 使用 `DeploymentOptions::config` 共享强类型 salt 配置，一次部署两个 Hasher 实例。
- 每个消息队列容量为 1，Sink 接收慢时，等待沿流水线传回数据源。
- 同一个 seed 分别通过 `execute_blocking` 和 `execute_blocking_unordered` 计算校验和，并核对结果。
- Sink 用 `take(5)` 定义结束条件。收集结果按 seed 排序，再在调用方复算校验。
- 打印运行与关闭快照，展示流水线的活跃计数和队列状态。

预期看到 `deployed 2 hashers with shared salt=0x5a5a`、五组校验和和 `close=closed`。
不同消费者可以并行，因此不承诺处理完成顺序；ordered 只保证同 Context 的 blocking FIFO。
队列快照和满事件计数受实际调度影响，示例不要求每次运行都出现固定等待数量。

## verticle_to_verticle：任务管理与服务组合

```text
main ──→ Caller ── select_ok ──→ Upper / Reject
           │
      managed tasks
       ├─ 丢弃句柄 → 仍然执行
       └─ abort → 析构 → TaskCancelled
```

- 用 channel 放行任务，确认丢弃 `TaskHandle` 后，任务仍由部署管理并继续运行。
- 等待另一任务启动后调用 abort，等待 `TaskCancelled`，并通过析构通知确认用户状态已销毁。
- 单个任务结束或取消后，Caller 继续处理请求，返回 `HELLO`、`VERTX`、`RUST`。
- `try_join_all` 等待全部成功；`join_all` 同时保留 Upper 成功结果和 Reject 的业务失败。
- Caller 使用 `select_ok` 等待第一个成功，显式丢弃剩余请求 Future；服务允许预期的迟到回复。
- 用共享的 `RequestAddress` 声明三个服务的地址和响应类型，调用时由编译器推断返回类型。
- 单独取消 Caller 的 handler 并等待结束，再次请求 Caller 得到 `NoConsumer`。此时 Caller 部署仍接收工作，最后从外部调用 undeploy。

abort 只能在执行器重新取得执行权后生效，不能抢占同步死循环或中断已开始的 blocking 闭包。
取消时序由 channel 握手控制，没有依赖 sleep 猜测任务是否已经执行。

## bounded_mailbox：确定性观察满队列

```text
容量 1 的 mailbox 已有消息 1
 ├─ try_send(2) → QueueFull
 ├─ send(3) → Pending → 取出 1 → 完成入队
 └─ publish(99) → 满队列丢弃
最终接收 [1, 3]
```

消费者使用外部 pull 模式，不自动取消息。手动 poll send Future 后，检查 `waiting_senders=1`；
满队列时 publish 的返回成功不等于送达，广播丢弃计数增加 1。
取出消息后 send 才能完成。示例不使用 sleep，不依赖调度恰好出现队列满。

## deployment_rollback：失败实例也需要清理

```text
计划创建 3 个实例
  0：注册消费者 → start 成功 → 后续被回滚
  1：注册消费者 → start 失败 → 框架清理
  2：不再创建
```

第二个实例故意返回 `StartFailed`。检查外层 `DeploymentFailed` 的首错与标准 source 链，
确认 rollback 错误列表为空、只创建了两个实例、两者 stop 各执行一次、地址路由已注销。
预期看到启动失败 ERROR 日志和 `created=2, stopped=[0, 1], routes removed`，最后退出码为 0。

## external_dispatch 外部派发

```text
外部 OS 线程 ── VertxHandle::spawn ──→ 实例级 EventLoop 任务
             └─ Deployment::spawn ──→ 指定部署及其 EventLoop
```

外部线程提交两类任务，再把结果句柄交给异步调用方等待。实例级任务的部署 ID 为 `None`。
定向任务检查 Context ID、部署 ID 和实际 OS 线程，确认与启动时一致。停止部署后再向它派发会得到 `DeploymentTerminated`。
主线程在 Tokio runtime 外 join 提交线程，并保留业务、关闭及 join 的错误。

## graceful_shutdown：超时停止等待，不停止清理

```text
实例任务 request_close() → 任务收到取消 → stop → Worker 清理等待 gate
外部 close().await → ShutdownTimeout → 放行 gate → wait_closed().await
```

部署任务和实例级任务就绪后，实例任务通过 `VertxHandle::request_close()` 发出关闭请求。
`stop` 中的 blocking 闭包通过 channel 等待放行，外部调用 `close()` 得到 `ShutdownTimeout`。
等到协作任务退出且 blocking 已启动后，确认实例拒绝新业务。
随后放行 gate，确认清理结果为 42、`stop` 执行一次，并用 `wait_closed()` 确认全部执行器线程退出。

gate 的发送端在失败或业务超时时被丢弃，也能解除 Worker 的等待，不留下永久阻塞的清理操作。
这只是示例控制的可释放操作；框架不能强制中断任意 blocking 闭包。

## deferred_reply：显式移动响应能力

```text
三个请求 → handler.into_parts → 每请求一个受管任务
                                 → blocking 计算
                                 → TypedReplyHandle.reply_with → 请求方
```

handler 显式拆出消息体、headers 和 TypedReplyHandle，校验 `correlation-id`，
再把工作交给 `spawn_local` 任务。任务通过共享 Worker 池做有限校验和计算，返回大写文本及校验和。
使用 `request_message_with` 的调用方检查响应体和相同的关联 header；预期看到 `job-0` 至 `job-2` 的结果，输出顺序不固定。

headers 不会自动传播。handler 自身仍然逐消息串行，并发来自应用主动派发受管任务；
输入只有三个请求，本例不是一个无限并发、无限接收的后台任务服务。

## eventbus_codec 字节编解码

调用方把 `u32` 编码成四字节大端序的 `Vec<u8>`，handler 解码后将数值翻倍，再编码回复。示例断言 21 的回复解码为 42，并验证三字节的请求返回 `RecipientFailure`。编解码发生在应用层，EventBus 校验消息体类型并负责投递。

## error_handling：三种失败的不同去向

```text
业务拒绝 → RecipientFailure → 请求方；不触发异常 hook
handler Err → 部署 hook
清除 部署 hook → handler panic → 全局 hook
随后健康请求 → healthy
```

异常先交给部署 hook，清除它后会回退到全局 hook。handler 返回 Err 或 panic 没有回复时，
请求方得到 `ReplyClosed`，异常详情由 hook 接收；业务拒绝则直接返回结构化 `RecipientFailure`。
最后用正常请求作为串行处理屏障，检查消费者继续工作且没有重复异常通知。

默认 Rust panic hook 仍可能在 stderr 打印 `expected handler panic`，框架也会记录 ERROR 日志；
这些都是预期演示，最终程序应以退出码 0 结束。没有安装全局吞 panic 的 hook，也没有关闭诊断来隐藏失败。

## tcp_shared_listener：手动接收 TCP 连接

第一个普通 Verticle 绑定 `127.0.0.1:0`，第二个用返回的实际地址加入。两个 Verticle 各用一个 `spawn_local`
任务接收一条连接并回显一个字节。示例内置客户端，向同一端口发送 `41` 和 `42`，并核对回显。
预期输出同一个本地地址和两个不同的 `EventLoopId`，端口及处理顺序由实际调度决定。

## axum_http1：在受管任务中运行 Axum Router

应用直接用 `serve_axum(&vertx, ...)` 启动第一个服务，一个 Verticle 加入同一地址。内置客户端发起 HTTP/1.1 请求，
路由读取 `ConnectInfo<SocketAddr>` 获取对端地址，检查 yield 前后的 EventLoop，并用 EventLoop ID 作为响应。
预期看到同一个地址分别返回两个不同的 `EventLoopId`。示例用 `request_shutdown()` 向两个服务发出排空请求，用 `wait_closed()` 等待清理，再调用 `vx.close()`。

需要自定义连接上限时，可从应用或 Verticle 调用 `serve_axum_with_options`。

```rust
let options = TcpServerOptions {
    max_connections: std::num::NonZeroUsize::new(256).unwrap(),
};
let server = vertx_rs_axum::serve_axum_with_options(&vertx, addr, router, options).await?;
```

共享这个地址的服务必须使用相同的上限。常驻服务应把返回的 `TcpServer` 交给部署外的控制任务，用于等待关闭。
[连接限额与线程归属](execution-model.md#受管-tcp-服务) · [关闭顺序](lifecycle.md#关闭实例与服务)

## 共享状态

多 handler 共享状态优先级：单 handler 独占 → Rc<Cell> → 小作用域 Rc<RefCell> → 通过内部地址协调。
若 stop 需要访问任务使用的状态，通过这些明确的所有权结构共享；不要跨 await 保存 RefCell guard。

## 相关主题

[生命周期与关闭](lifecycle.md)、[EventBus](eventbus.md)、[背压传输](streams.md)、[原生 Worker 调度与 Future 组合](worker-and-futures.md)。
