# 示例与共享状态

[返回文档索引](index.md)

三个示例可独立运行，无需外部服务。每个示例使用两个 EventLoop 和两个 Worker，
通过 `try_build()` 创建实例，业务阶段最多等待 5 秒，最后显式 `close().await`。
业务和关闭同时失败时，两者通过 `LifecycleFailed` 保留。

## 选择示例

| 示例 | 场景 | 重点 API |
|---|---|---|
| [ping_pong](../examples/ping_pong.rs) | 请求服务 + 周期心跳 + 两个观察者 | request/reply/fail、headers、publish、periodic |
| [pipeline](../examples/pipeline.rs) | 有限数据源 → 两个校验节点 → Worker 汇总 | deploy_with、强类型配置、pipe_to、Stream、blocking、diagnostics |
| [verticle_to_verticle](../examples/verticle_to_verticle.rs) | 服务组合 + 受管任务 + 撤销 | try_join_all、join_all、select_ok、TaskHandle、abort、undeploy |

## 运行方式

在仓库根目录执行：

```sh
cargo run --example ping_pong
cargo run --example pipeline
cargo run --example verticle_to_verticle
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
- 三次 tick 后显式取消 Periodic。该任务结束不会撤销 Pinger，之后仍能请求服务。
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
- 打印运行与关闭快照，成功 close 后检查部署、任务及消费者活跃计数归零。

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
- 从部署外部执行 undeploy，等待 stop 与清理；再次请求 Caller 得到 `NoConsumer`，确认路由已移除。

abort 只能在执行器重新取得执行权后生效，不能抢占同步死循环或中断已开始的 blocking 闭包。
取消时序由 channel 握手控制，没有依赖 sleep 猜测任务是否已经执行。

## 共享状态

多 handler 共享状态优先级：单 handler 独占 → Rc<Cell> → 小作用域 Rc<RefCell> → 通过内部地址协调。
若 stop 需要访问任务使用的状态，通过这些明确的所有权结构共享；不要跨 await 保存 RefCell guard。

## 相关主题

[生命周期与关闭](lifecycle.md)、[EventBus](eventbus.md)、[背压传输](streams.md)、[原生 Worker 调度与 Future 组合](worker-and-futures.md)。
