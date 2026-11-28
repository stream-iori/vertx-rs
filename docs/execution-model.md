# 执行模型与 Context

[返回文档索引](index.md)

| 执行对象 | 线程保证 | 状态约束 |
|---|---|---|
| 普通 Verticle 的 factory/start/stop/析构 | 始终在部署时分配的同一个 OS 线程 | 不要求 Send，可持有 Rc/Cell/RefCell |
| EventLoop Context 的受管任务和 handler | 同一 EventLoop；await 后仍在原线程 | 支持 !Send Future |
| Worker Context 的普通执行 | 可以换线程；同 Context 的 poll 和任务析构串行 | Send |
| execute_blocking | 与 Worker Future 共用固定线程池；保留来源逻辑 Context | 闭包和结果要求 Send |
| 裸 tokio::spawn/spawn_local | 遵循 Tokio 自身规则，不继承逻辑 Context | 不受部署任务管理 |

亲和性指 **OS 线程**，不绑定 CPU 核心。线程可以由操作系统调度到不同 CPU。
每个 EventLoop 固定一个 OS 线程。每个实例默认创建 `2 × available_parallelism()` 个 EventLoop，可用 builder 覆盖。
单线程上的多个 async 任务仍会在 await 处交替执行，`RefCell` 借用不能跨 await。
跨 Verticle 的默认通信方式是 EventBus。Rust 也允许 Arc、原子变量及锁，框架不会强制禁止共享状态。

```text
Vertx
 ├─ EventLoop #0: OS T0 + current-thread runtime + LocalSet
 │    ├─ Verticle A / Context A
 │    └─ Verticle B / Context B
 ├─ EventLoop #1: OS T1 + current-thread runtime + LocalSet
 │    └─ Verticle C / Context C
 ├─ Worker pool：固定 N 个 OS 线程，Context 就绪队列串行派发 Future poll
 │    └─ execute_blocking 共用该池，默认每来源 Context FIFO
 └─ EventBus：进程内共享地址路由，每个消费者一个有界队列
```

## Context 隔离

`ContextFuture` 在**每次 poll** 前设置 CURRENT，返回 Pending/Ready 或 panic 时立即恢复。
ContextGuard 不可 Send，也不跨 await 保存。Future 取消时，在所属执行环境中设置 Context 后析构用户状态。
Worker 的 Context 就绪队列每次派发一个 poll；Pending 后释放执行权，析构也与普通 poll 互斥。
同 Context 的其他任务可以在该 Future 等待期间执行；同消费者仍等待当前消息 Future 完成再取下一条。

```text
poll(A): CURRENT=A → poll user A → restore
poll(B): CURRENT=B → poll user B → restore
poll(A): CURRENT=A → poll user A → restore
```

Context 与物理线程身份分开记录。`Context::threading_model()` 只返回 `EventLoop` 或 `Worker`；
框架外代码的 `Context::current()` 为 None，不自动创建外部 Context。调用 execute_blocking 时：

| 查询 | EventLoop 内 | 其 blocking 闭包内 | await 返回后 |
|---|---|---|---|
| Context::current() | 来源 Context | 同一来源 Context | 同一来源 Context |
| ctx.is_event_loop() | true | true | true |
| is_on_event_loop_thread() | true | false | true |
| is_on_worker_thread() | false | true | false |

EventLoop 在所属线程构造受管包装，并同时校验 poll 与 drop 的线程亲和性。线程私有执行依靠 LocalSet 串行。
Worker 的执行门覆盖同一 Context 的普通 poll 和 drop。用户析构 panic 时，先恢复 Context、Dispatch、Span 并释放执行门，再由执行器处理 panic。
部署、路由和监听等共享注册状态仍同步处理跨线程准入。

EventLoop 初始化记录实例 ID、EventLoop ID 和实际 ThreadId，受管 poll 校验物理位置。
Worker 原生线程启动/退出时设置和清除物理线程类型。

## EventLoop 直接派发

EventLoop 的 OS 线程、启动握手及退出回收由项目管理；每个线程保留 Tokio current-thread runtime + LocalSet。
不再使用 LocalPoolHandle 或等待本地 JoinHandle 的 Tokio 代理任务。

```text
跨线程派发 → 目标 EventLoop 命令队列 → 一个本地任务 → 用户 Future
同目标 EventLoop 的受管派发 ──────────→ 一个本地任务 → 用户 Future
```

- 跨线程命令传递 Send factory，!Send Future 在目标线程创建；每个任务只创建一个本地 Tokio 任务。
- `spawn_local` 和同目标 EventLoop 内的受管 `spawn` 直接创建本地任务。外部线程及裸 Tokio 任务走命令队列。用户 Future 始终延后执行。
- 跨线程 TaskHandle 只使用一个结果 oneshot，取消状态覆盖入队前、入队后及绑定本地 AbortHandle 时的竞争。
- 早到的 abort 不会丢失；取消、未首次 poll 的 Future 析构和 panic 清理都在目标线程完成。
- 命令每处理最多 64 条主动让出执行权。命令队列沿用无界方式，不增加背压 API。
- 创建实例等待全部 EventLoop 就绪，记录实际 ThreadId 和 runtime Handle；部分启动失败时回收已创建线程。
- `VertxBuilder::build() -> Result<Vertx>` 在创建线程前校验线程数与 shutdown_grace，零值返回 InvalidOptions。
  线程/runtime 创建失败以 Other 保留阶段与底层错误文本；后续回收失败使用 LifecycleFailed 保留首错与 cleanup。
  EventLoop 部分启动失败、或 Worker 初始化失败，均在返回错误前停止并 join 已创建线程。
  `Vertx::new() -> Result<Vertx>` 使用默认配置构建实例。
- 普通依赖不启用 Tokio rt-multi-thread；tokio-util 继续用于 CancellationToken、TaskTracker 等工具。
- 本轮只验证派发结构与执行语义，不提供未经测量的性能提升数字。

## 共享 TCP 监听

同一 Vertx 实例对同一个 `SocketAddr` 只绑定一次。普通 Verticle 用
`ctx.listen_tcp(addr).await` 注册，用 `incoming.accept().await` 领取连接。
端口 `0` 每次申请新地址，其他部署用 `incoming.local_addr()` 返回的地址加入。
同一部署在同一地址重复注册返回 `InvalidOptions`。

| 入口 | 适用场景 | 谁持有连接 |
|---|---|---|
| `Context::listen_tcp` | 需要自己控制何时 `accept()`、如何派发连接 | 应用拿到原生 `TcpStream`，可交给受管任务 |
| `Context::serve_tcp` | 需要统一的连接限额、关闭信号和服务句柄 | 框架为每条连接启动受管任务，处理函数借用 `TcpConnection` |
| `vertx_rs_axum::serve_axum` | 已有 Axum Router，想让 HTTP handler 在 Verticle 上运行 | TCP 任务持有连接，Hyper 驱动 HTTP/1.1，Axum 执行路由 |

```text
共享 TcpListener → FIFO 等待队列 → 最早等待的 EventLoop → 原生 TcpStream
```

- 等待者轮流独占一次接收。暂时忙的 Verticle 在下一次 `accept()` 时重新排队。没人等时，连接留在操作系统的监听队列。
- 首次绑定的 EventLoop 驱动监听 socket。领取连接的 EventLoop 驱动已接收的 socket。停止首个注册的部署后，仍注册的 Verticle 可以继续接收。
- 受管监听要求所属的普通 Verticle Context 和实际 EventLoop 线程。活跃的 Worker 或其他 Context 调用返回 `ContextMismatch`。接收器为 `!Send`，Worker 可直接使用 Tokio 网络 API。
- 取消一次 `accept()` 会释放等待位置，接收器仍可再次接收。绑定和接收的系统错误通过 `TcpIo` 返回，包含操作、地址及错误类别。

[可运行示例](../examples/tcp_shared_listener.rs) · [撤销与关闭](lifecycle.md#撤销与关闭)

### 受管 TCP 服务

`Context::serve_tcp` 为每条连接启动受管任务。处理函数通过 `TcpConnection` 借用
异步 I/O，可读对端地址和服务关闭信号，socket 由任务持有。它只在所属普通 Verticle 上运行。

- 同一实例的同一绑定地址共用一个连接上限，默认 1024。服务先拿到许可，再等 `accept()`，许可一直保留到 socket 析构。容量耗尽时，新连接留在系统监听队列。
- 受管监听调用 `listen(128)` 设置系统监听队列，请求的队列长度还受操作系统限制。`TcpServerOptions::max_connections` 控制框架的连接许可。
- 同地址的服务必须设置相同上限。原始 `listen_tcp` 和 `serve_tcp` 在同地址互斥，冲突返回 `InvalidOptions`。最后一份注册退出就释放监听 socket，旧连接仍占用该地址的预算，使用相同上限的服务可重新绑定并复用预算。
- 受管服务沿用同一监听器的 FIFO 等待顺序。领取连接的 EventLoop 负责该连接的 I/O 和处理任务。Axum 适配 crate 为请求提供 `ConnectInfo<SocketAddr>`，路由和中间件在该任务中运行。

[网络示例与运行命令](examples.md#tcp-与-http-示例) · [服务关闭](lifecycle.md#撤销与关闭)

## 相关主题

[生命周期与关闭](lifecycle.md)、[原生 Worker 调度与 Future 组合](worker-and-futures.md)。
