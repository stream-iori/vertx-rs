# vertx-rs 开发约定

本文件适用于整个仓库。先阅读 [DESIGN.md](DESIGN.md)，再修改相关实现与测试。
用户本次明确提出的要求优先于本文件；发现文档与实现冲突时，先取证并明确修正对象。

## 项目定位

Rust Future + 固定 OS Worker 线程池 + Tokio IO 驱动实现 Vert.x 5 的单进程执行核心：Verticle、Context、Worker、EventBus、Timer、部署和关闭。
对齐执行语义，使用 Rust 原生 async/await、Result、强类型消息及所有权模型。

- 网络、文件、DNS、集群、Codec、SharedData 门面、命名 WorkerExecutor 和虚拟线程不在当前范围；仅在用户任务需要时扩展。
- 版本、Edition、MSRV 和依赖特性以 [Cargo.toml](Cargo.toml) 为准。不要为实现便利擅自提高 MSRV 或增加依赖。
- 保留现有目录、源文件名、测试 target 和示例名称，减少安全拦截及无关变更。
- 不为修复局部问题顺带改版、重构整个项目或调整无关配置。

## 代码导航

| 位置 | 职责 |
|---|---|
| `src/vertx.rs` | 实例配置、部署、批量回滚、撤销、全局关闭 |
| `src/verticle.rs` | 普通及 Worker Verticle 的 start/stop 接口 |
| `src/context.rs` | 逻辑 Context、物理线程身份、按 poll 隔离、受管任务、阻塞计算 |
| `src/eventloop.rs`、`src/worker_pool.rs` | 固定线程执行器、原生 Worker 线程池、Future 调度、统一任务句柄 |
| `src/deployment.rs` | 部署状态、资源准入、任务跟踪、取消和完成通知 |
| `src/eventbus/` | 类型路由、消息队列、消费者、请求回复、headers |
| `src/timer.rs` | 单次和周期定时、deadline、取消 |
| `src/error.rs`、`src/lib.rs` | 结构化错误和公共导出 |
| `tests/integration.rs` | 对外行为及并发回归；模块内单元测试覆盖内部不变量 |
| `examples/` | `ping_pong`、`pipeline`、`verticle_to_verticle`，均显式关闭实例 |

## 必须保持的执行语义

### 线程亲和性与 Context

| 对象 | 保证 |
|---|---|
| 普通 Verticle | factory、start、handler、受管任务、stop、析构位于所属的同一 OS 线程 |
| EventLoop Future | 支持 `!Send`；await 后仍在原 EventLoop |
| Worker Context | 允许换线程；普通 poll 和受管任务析构互斥，不同 Context 可以并行 |
| blocking 闭包 | 保留来源逻辑 Context，物理线程类型为 Worker |
| 框架外代码 | `Context::current()` 为 None；不自动构造外部 Context |

```text
一次 poll：校验运行位置 → 获取执行许可 → 设置 CURRENT
         → poll 用户 Future → 恢复 CURRENT → 释放许可
```

- ContextGuard 只能覆盖同步 poll，不能保存在跨 await 的 Future 状态里，也不能变成 Send。
- Pending、Ready、panic、取消和析构路径都要恢复原 Context；不能只修首次调用或正常返回路径。
- 逻辑 Context 与物理线程身份分别维护；不能用 `ctx.is_event_loop()` 推断当前一定在 EventLoop 线程。
- Worker Context 就绪队列的执行权不能跨用户 Future 的异步等待持有。一次派发一个 poll，再排到共享队尾。
- Worker Future 与 blocking 共用固定数量的 OS 线程，不重新引入 Tokio Worker runtime 或额外 blocking 池。
- Worker poll 通过所属 EventLoop runtime Handle 访问 IO/Timer 驱动；Handle::enter 不替代 Future 调度。
- wake 合并必须覆盖 poll 中通知与迟到通知；Pending 任务继续登记，取消和用户析构只在所属执行器执行。
- `spawn_local` 校验实例、所属 EventLoop、当前 Context 和存活状态；不能只判断线程类型。
- 框架内部派发统一走受管入口。裸 Tokio spawn 不继承 Context，也不自动受部署管理。
- 不在 EventLoop 中执行同步 IO、线程 sleep 或长 CPU 计算；使用受限的 blocking 执行入口。
- `Rc<RefCell<_>>` 的借用不得跨 await；跨 Verticle 默认通过 EventBus，确需共享时使用合适的 Rust 同步原语。

### 生命周期与关闭

```text
Starting → Running → Stopping → Stopped
    └─ 启动失败或取消 ───┘
```

- 当前主接口为异步 `start(&mut self, ctx)` / `stop(&mut self, ctx)`；不要重新引入旧的 owned-self run 主生命周期。
- deploy 成功表示异步 start 完成；start 返回或单个子任务结束不会自动撤销部署。
- 部署记录先登记再启动；失败、panic、调用方取消、多实例部分失败均必须清理或回滚。
- Stopping 关闭资源准入；检查、注册和登记形成同一事务，不能靠两次独立存活检查代替原子性。
- 受管任务的完成记录晚于用户状态析构；覆盖未首次 poll 就被 abort 的任务。
- undeploy 成功返回前，受管任务及其析构必须完成；stop 最多执行一次。
- close 幂等；等待方取消不撤销后台收尾；超时返回明确错误，实例继续拒绝新工作。
- 不把启动取消误报为清理失败。保留原始错误及回滚/关闭错误，不能仅写日志后返回成功。
- abort 无法抢占不让出执行权的同步代码，也不能中断已开始的 blocking 闭包；不能宣称它们已被强制终止。
- 等待自身结束必须拒绝；本部署内请求撤销使用 `request_undeploy()`。
- stop 的清理权限仅用于实际停止流程，不能让被保存的 stop Context 在外部重新准入工作。
- 不依赖 Drop 提供可等待的关闭保证；示例和成功路径显式 `close().await`。

### 消息、阻塞计算与定时器

- 同地址存活消费者类型一致；注册时拒绝冲突。删除最后一个消费者与新注册的竞争必须原子处理。
- Consumer 记录所属 Context，handler 校验归属；注销同时清理路由及作用域登记。
- send 成功代表入队；send 有背压，publish 满队列丢弃并记录，不能把两者混为可靠投递。
- request 的同一个 deadline 覆盖排队和回复；超时不会撤回已入队消息，迟到回复返回错误。
- 保留响应 headers、重复 header 值和结构化业务失败；不要把不同错误都压成 Other。
- `execute_blocking` 默认同来源 Context FIFO；无序路径与 Worker Future 共用 worker_threads 配置的固定 OS 线程池。
- blocking 跟踪覆盖排队、执行及调用方取消；实际操作结束前不能提前释放完成记录。
- Consumer 仅接受返回 Future 的处理函数；同消费者等待上一条 Future 完成后再取下一条，不擅自增加消息并发。
- Future 组合直接使用 futures；区分全成功、全结束、首次成功和首次完成，不重新实现 Future/Promise 门面。
- TaskHandle 的 await 输出为单层 Result；丢弃只分离等待者，abort 在所属执行器取消，完成状态晚于用户析构。
- 关闭协调在 EventLoop 执行；先完成部署、blocking 和 Worker 退出，再释放驱动。不能在 Worker 内等待回收自身。
- Timer 创建时确定绝对 deadline；延迟首次 poll 不重新计时。Periodic 保持 Skip 策略，零周期返回参数错误。
- 定时器取消涵盖自身、部署和全局关闭；取消与到期同轮就绪时取消优先。

## 取证与修改方式

- 遇到“为什么、根因、是否生效、看看日志、线上是否如此”等问题，先查可用证据，再下结论。
- 本地优先检查实际代码、测试、配置、依赖版本、日志和 git 历史；用 `rg` / `rg --files` 定位。
- 线上判断必须有对应环境的证据。本地测试通过不能证明线上已生效；证据不可访问时说明缺少什么。
- 把已确认事实、可复现问题和待验证推断分开，不能把理论可能性写成确定根因。
- 工作区可能包含用户或前序任务的未提交改动；先检查 git 状态，不覆盖、撤销或清理无关改动。
- 公共 API 或执行语义变化，同步更新导出、相关测试、示例及 DESIGN.md；明确破坏性变化和迁移方式。
- 不引入不必要的 unsafe、动态类型包装或依赖；新增依赖时说明现有 std/Tokio 能力为何不足。
- 调用用户回调及异常 handler 前释放注册表/配置锁；执行门只保留保证串行所需的同步范围。

## 验证要求

先运行与修改直接相关的测试。Rust 行为或公共 API 变更完成后执行以下检查；单纯文档修改只做相关链接、内容和差异检查。

```sh
cargo test --tests
cargo test --doc
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
git diff --check
```

修改生命周期、调度、EventBus、关闭或示例时，再实际运行相关示例；跨模块变更运行全部三个：

```sh
cargo run --example ping_pong
cargo run --example pipeline
cargo run --example verticle_to_verticle
```

- 需要验证全部 Cargo target 时使用 `cargo test --all-targets`；示例的零测试 harness 通过不能替代实际运行示例。
- 并发测试使用 channel、barrier、明确的就绪信号控制时序；不要仅靠 sleep 碰竞态。
- 亲和性比较实际 ThreadId，同时检查 Context；Worker 迁移用可控跨线程 poll 验证，不能假定 Tokio 必定迁移。
- Timer 优先用暂停时间测试；与独立运行时配合时注意虚拟时间，关闭前恢复时间或明确控制推进。
- 测试使用小线程池并显式清理；同时覆盖同 Context 串行和不同 Context 并行，避免仅测单实例正常路径。
- 回归覆盖失败、panic、取消、析构、满队列、超时、注册/清扫竞争及并发关闭等相关边界。
- 文档示例应实际编译；不通过标记 ignore、放宽断言或关闭 lint 掩盖问题。
- 已通过的检查不无故反复执行；新增改动、失败或仍未解决的疑点才触发补验。
- 区分安全拦截导致的 SIGKILL、测试断言失败和超时。记录准确命令、退出状态与进程路径；用户放行后复用原路径补验，不通过改名或移动绕过拦截。
- Cargo 正常生成的带哈希构建产物无需手工改名；避免为重试修改版本、target 名称或构建输出目录。
- 只报告实际执行的验证，不固化易过期的测试总数，不把当前工具链通过说成 MSRV 已验证。

## 沟通与交付

- 中文、大白话、结论先行；优先使用 Markdown 表格、ASCII 图，少写背景复述和长段文字。
- 直接指出错误路径并解释证据，不做角色扮演，不用未经测试的性能或完整兼容性承诺。
- 最终说明改了什么、为什么、如何验证，以及仍存在的限制；引用相关文件路径即可，不罗列全部修改文件。
- 不擅自提交、推送或发布；用户已明确授权的操作继续完成，不重复索要确认。
