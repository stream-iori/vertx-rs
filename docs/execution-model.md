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

## 业务入口和资源归属

应用通过 `Vertx` 构建、部署和关闭实例，也可直接调用业务 API。Verticle 的 `start` 和 `stop` 接收 `VertxHandle`。
两者走同一套派发逻辑。把任务提交到指定部署时，使用部署返回的 `Deployment::spawn`。

| 调用位置 | 执行目标 | 资源随谁停止 |
|---|---|---|
| 同实例内的部署 | 当前部署的 EventLoop 或 Worker | 当前部署 |
| 同实例内的实例级任务 | 当前任务的执行环境 | 实例 |
| 框架外、裸 Tokio 任务或其他实例 | `spawn` 和 `serve_tcp` 选择 EventLoop。blocking 交给共享 Worker | 调用目标实例 |
| `Deployment::spawn` | 指定部署的 EventLoop 或 Worker | 指定部署 |

`Context` 是框架内部执行状态。`VertxHandle` 和 `Deployment` 可跨线程传递，分别通过弱引用指向实例和部署执行目标。
普通任务与端口监听通过作用域登记，关闭时统一清理。`execute_blocking` 和 TCP 启动 Future 在方法调用时确定归属，首次 poll 时准入。
同实例的部署进入停止阶段后，普通操作返回 `DeploymentTerminated`。实例关闭准入后返回 `VertxClosed`。
`spawn_local`、`serve_tcp_local` 和 `listen_tcp` 只可在本实例当前 EventLoop 内提交。

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

用 `vertx.current_execution()` 查询本实例的受管执行。框架外、裸 Tokio 任务和其他实例的执行返回 `None`。
`ExecutionInfo::current()` 可读取任意实例的当前执行，用于日志与诊断。blocking 闭包沿用来源 Context，物理线程在 Worker 池上。

| 字段 | EventLoop 任务 | 它的 blocking 闭包 | await 返回后 |
|---|---|---|---|
| `context_id` | 来源 ID | 同一 ID | 同一 ID |
| `threading_model` | EventLoop | EventLoop | EventLoop |
| `thread` | EventLoop | Worker | EventLoop |

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
- `VertxBuilder::build() -> Result<Vertx>` 检查线程数与 `shutdown_grace`。每个池在启动自己的线程前预留存储，零值或预留失败返回 `InvalidOptions`。
  线程或 runtime 创建失败时，`Other` 保留失败阶段和底层错误。回收也失败时，`LifecycleFailed` 保留原始错误和清理错误。
  EventLoop 部分启动失败或 Worker 初始化失败时，框架在返回错误前停止并 join 已创建的线程。
  `Vertx::try_new() -> Result<Vertx>` 使用默认配置构建实例。
- 普通依赖不启用 Tokio rt-multi-thread；tokio-util 继续用于 CancellationToken、TaskTracker 等工具。
- 本轮只验证派发结构与执行语义，不提供未经测量的性能提升数字。

## 共享 TCP 监听

同一 Vertx 实例对同一个 `SocketAddr` 只绑定一次。普通 Verticle 用
`vertx.listen_tcp(addr).await` 注册，用 `incoming.accept().await` 领取连接。
端口 `0` 每次申请新地址，其他部署用 `incoming.local_addr()` 返回的地址加入。
每个作用域在同一地址可以登记一次，重复登记返回 `InvalidOptions`。

| 入口 | 适用场景 | 谁持有连接 |
|---|---|---|
| `listen_tcp` | 需要自己控制何时 `accept()`、如何派发连接 | 应用拿到原生 `TcpStream`，可交给受管任务 |
| `serve_tcp` / `serve_tcp_local` | 需要统一的连接限额、关闭信号和服务句柄 | 框架为每条连接启动受管任务，处理函数借用 `TcpConnection` |
| `vertx_rs_axum::serve_axum` | 已有 Axum Router，需要在 EventLoop 提供 HTTP 服务 | TCP 任务持有连接，Hyper 驱动 HTTP/1.1，Axum 执行路由 |

```text
共享 TcpListener → FIFO 等待队列 → 最早等待的 EventLoop → 原生 TcpStream
```

- 等待者轮流独占一次接收。暂时忙的 Verticle 在下一次 `accept()` 时重新排队。没人等时，连接留在操作系统的监听队列。
- 首次绑定的 EventLoop 驱动监听 socket。领取连接的 EventLoop 驱动已接收的 socket。停止首个注册的部署后，仍注册的 Verticle 可以继续接收。
- 原始监听要求本实例当前 EventLoop。随后只能在登记时的 Context 里轮询 `accept()`，接收器为 `!Send`。线程或归属不匹配返回 `ContextMismatch`。
- `TcpIncoming` 保存登记 ID、取消信号和弱引用。保留它时，作用域仍可注销登记，EventLoop 仍可退出。
- 取消一次 `accept()` 会释放等待位置，接收器仍可再次接收。绑定和接收的系统错误通过 `TcpIo` 返回，包含操作、地址及错误类别。

[可运行示例](../examples/tcp_shared_listener.rs) · [停止部署](lifecycle.md#停止部署)

### 受管 TCP 服务

`tcp.rs` 导出 TCP 类型。`listener.rs` 管共享监听、作用域登记和按地址复用的连接预算。
`server.rs` 使用监听登记和连接许可来派发任务，并等待连接排空。

`serve_tcp` 从应用直接调用时选择实例的 EventLoop，从普通 Verticle 调用时选择其 EventLoop。处理器要求 `Send`，它产生的连接 Future 在目标线程创建和执行，可以持有 `Rc`。`serve_tcp_local` 可以直接捕获该线程的 `Rc`。两者都让连接任务持有 socket，处理器通过 `TcpConnection` 借用 I/O、读取对端地址及关闭信号。Worker Context 调用返回 `ContextMismatch`。

- 同一实例的同一绑定地址共用一个连接上限，默认 1024。服务先拿到许可，再等 `accept()`，许可一直保留到 socket 析构。容量耗尽时，新连接留在系统监听队列。
- 受管监听调用 `listen(128)` 设置系统监听队列，请求的队列长度还受操作系统限制。`TcpServerOptions::max_connections` 控制框架的连接许可。
- 同地址的服务必须设置相同上限。原始 `listen_tcp` 和 `serve_tcp` 在同地址互斥，冲突返回 `InvalidOptions`。最后一份注册退出就释放监听 socket，旧连接仍占用该地址的预算，使用相同上限的服务可重新绑定并复用预算。
- 受管服务沿用同一监听器的 FIFO 等待顺序。领取连接的 EventLoop 负责该连接的 I/O 和处理任务。Axum 适配 crate 为请求提供 `ConnectInfo<SocketAddr>`，路由和中间件在该任务中运行。
- 单个服务的连接登记表只由所属 EventLoop 读写，使用 `Rc<RefCell<_>>`。`TcpServer` 可以跨线程传递关闭期限和取消信号，并接收最终结果。同地址的监听登记和连接预算由多个 EventLoop 共享，仍使用同步原语。

[网络示例与运行命令](examples.md#tcp-与-http-示例) · [服务关闭](lifecycle.md#关闭实例与服务)

## 相关主题

[生命周期与关闭](lifecycle.md)、[原生 Worker 调度与 Future 组合](worker-and-futures.md)。
