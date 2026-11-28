# 与 Vert.x 5 的对照与范围

[返回文档索引](index.md)

| 能力 | 对齐情况 / Rust 选择 |
|---|---|
| EventLoop 线程亲和性 | 对齐；支持 !Send 状态 |
| 业务入口和 Context | 参考 Vert.x 的自动归属。Vertx 与 VertxHandle 提供业务入口，Context 在库内管理 |
| Worker 普通执行串行 | 对齐；允许线程迁移 |
| start/stop 就绪与关闭 | 异步钩子；框架增加受管任务取消及明确的等待超时 |
| executeBlocking ordered | 默认有序；与 Worker Future 共用固定 OS 线程池 |
| send/publish/request/reply/fail | 提供；send 的有界背压和 pull Consumer 是 Rust 扩展 |
| 固定请求地址 | 共享 RequestAddress，在编译期校验请求与回复类型 |
| Consumer 注册时点 | consumer 调用立即注册，支持 recv；与 Java 在 handler 注册后生效有差异 |
| ReadStream / WriteStream / Pipe | 使用 futures::Stream / Sink 和 pipe_to 表达拉取、背压、结束及失败；无需另造 handler/pause/fetch 门面 |
| TCP 共享监听 | 同实例内多部署共享同一绑定地址。普通 Verticle 按等待顺序领取原生 Tokio TCP 连接 |
| 受管 TCP 服务 | 同地址连接上限和服务级排空。HTTP/1.1 由独立 Axum 适配 crate 提供 |
| Timer / Periodic | Future / tick 拉模型；保留创建时间起点与取消语义 |
| 多实例 | 返回 Vec<Deployment>，失败时回滚本批 |
| Future/Promise | Rust Future、Result、oneshot；不重复实现 Java 类型 |
| 任意成功的 Future 组合 | futures::future::select_ok；tokio::select! 等待先完成，不能等同 Future.any |
| 配置 | factory 接收 Arc<C> 强类型配置；不提供动态 Context KV |
| 外部调用 | current_execution() 返回 None。可提交实例级任务，或通过 Deployment 显式选定部署 |
| SharedData | 使用 std/Tokio 共享原语；不提供框架门面 |

本轮不包含 TCP 客户端门面、TCP/WebSocket/FileSystem/DNS 流适配、公开 reserve、字节预算、批量传输、集群 EventBus、Codec、命名 WorkerExecutor、
Java 虚拟线程、动态部署、旧式按 ID 取消 Timer 和 metrics 服务端点。

官方语义依据：[Context](https://vertx.io/docs/apidocs/io/vertx/core/Context.html)、
[VerticleBase](https://vertx.io/docs/apidocs/io/vertx/core/VerticleBase.html)、
[Message](https://vertx.io/docs/apidocs/io/vertx/core/eventbus/Message.html)、
[Vertx.close](https://vertx.io/docs/apidocs/io/vertx/core/Vertx.html)。

## 相关主题

[执行模型与 Context](execution-model.md)、[示例与共享状态](examples.md)。
