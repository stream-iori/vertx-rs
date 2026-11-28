# 验收与测试组织

[返回文档索引](index.md)

## 验收

以下是整体行为要求，每次修改按涉及的风险选择回归场景；必跑检查见 [AGENTS.md](../AGENTS.md)。

- 同一个 EventLoop 上多 Context 交替 await 不串线；跨 OS 线程 poll 后 TLS 不残留。
- 普通 Verticle 的 factory/start/handler/受管任务/stop/析构始终位于所属 OS 线程与 Context。
- Worker 同 Context 最大普通并行 poll 数为 1，不同 Context 可以并行。
- blocking 的逻辑 Context 和物理 Worker 身份正确；有序 FIFO、无序并发由共用线程数限制。
- Worker 与 blocking 使用同一组实际 ThreadId；Pending 让出线程、跨线程恢复及 wake 合并不丢通知。
- Worker 内真实异步 IO、Timer、timeout 可用；取消析构和析构 panic 不破坏线程池或 Context。
- EventLoop 同线程派发不内联；跨线程早到取消、运行中取消及关闭入队竞争不遗失任务结果。
- 共享 TCP 监听验证同端口分流、等待取消和端口重绑。受管 TCP 服务验证端口级连接额度、排空超时和连接析构。
- 校验业务入口按调用时的实例和部署归属，外部派发、弱句柄、实例级资源清理及停止 blocking 权限都用实际 API 测试。
- TCP 和 Axum 覆盖应用直接启动，并验证未交付的 TCP 启动结果释放监听。Axum 适配测试校验 HTTP handler 在 await 前后保持 Context 和线程，排空期间在途请求仍能使用 EventBus。
- 初始化部分失败回收已创建线程；close 成功时全部 EventLoop 已 join，保留 VertxHandle、Deployment、任务句柄或 Waker 不阻止退出。
- start/stop 错误、panic、取消、关闭、批量回滚和自身撤销均有回归覆盖。
- 分别验证构造、poll 和析构生命周期 Future 的 panic，以及异常 hook 析构失败后仍完成清理和 join。
- 验证诊断回调重入准入、任务提交失败、abort 句柄迟到和 TCP 同地址重绑后的旧登记。
- Deployment::stop 成功返回前受管用户状态已析构；不可中断工作超时返回错误，随后可继续关闭。
- 路由注册/清扫/最后订阅注销竞争不残留路由；请求超时包含满队列等待。
- 业务失败、重复 headers、完整响应、迟到回复、广播满队列行为明确。
- Stream 终止错误只产生一次；Sink 重复就绪、排队取消及目标注销不泄漏许可；publish 先获许可再 Clone。
- pipe 验证背压、有限 EOF、异步 flush/close、主错与清理错误、!Send/!Unpin、取消析构及 Context/Span 隔离。
- 队列快照区分 queued/reserved/waiting，详情受限；并发关闭/注销后活跃计数归零。
- Timer 延后首次 poll、scope/global 取消、周期 Skip 均有测试。
- 全 workspace 的单测、集成测试、Rustdoc、Clippy 和格式检查通过。跨业务门面的改动实际运行全部核心和 Axum 示例。
- 不以未经测量的性能或完整 Vert.x Core 兼容率作承诺。

## 测试组织

继续使用标准 Rust test harness 和 Tokio 测试入口，用 [rstest](https://docs.rs/rstest/0.28.0/rstest/) 补充命名参数用例与 fixture。
参数化在编译时生成独立测试，支持 `--list`、按名称筛选及逐例失败报告；不要用一个循环代替多个独立场景。

| JUnit 5 中的需求 | 本项目做法 |
|---|---|
| 参数化、方法提供测试数据 | `#[rstest]` + `#[case::场景名(...)]`，每例独立创建资源 |
| 多维组合 | 独立维度使用 `#[values(...)]`；有关联的输入和预期使用显式 case，避免无效组合 |
| 每例初始化 | `#[fixture]` 提供独立 Vertx；有特殊线程数、诊断或关闭配置时在测试里显式 builder |
| 每例清理 | 成功路径显式 `close().await` / 池 shutdown；Drop 只能尽力清理，不能替代异步等待 |
| 职责分组 | 通过外部 `mod` 按文件拆分，保留单一 integration target 和测试名称 |
| 动态测试工厂 | 暂无运行时扫描文件或协议数据生成用例的需求；未来需要时单独评估自定义 harness |

```text
tests/
 ├─ integration.rs：仅声明职责模块，保留 integration target
 └─ integration/
      ├─ support.rs：独立 fixture、Verticle 适配、日志缓冲及可控 Sink
      ├─ configuration.rs / lifecycle.rs / shutdown.rs
      ├─ facade.rs / context.rs / worker.rs / blocking.rs
      ├─ eventbus.rs / shared_state.rs / timers.rs
      └─ errors.rs / diagnostics.rs / tracing_propagation.rs / streams.rs

src/* 的 #[cfg(test)]：内部不变量，保留在实现所在模块
 └─ EventLoop / Worker 内再按调度、取消、关闭及 panic 恢复分组
```

职责文件通过入口的 `#[path = "integration/…"] mod …;` 加载，不放在 tests 根目录作为独立 target。
各模块显式导入所需类型和 support 帮助，不依赖入口的通配导入；测试名称仍是 `streams::…` 等原路径，
现有 `cargo test --test integration streams::` 过滤命令不变。保留同一测试进程，避免重复编译辅助代码及改变 tracing 测试的进程隔离方式。

fixture 不使用 `#[once]` 共享 Vertx、Context、runtime 或当前 Dispatch；每例持有独立资源。
fixture 只消除重复创建，时序、消息、断言和清理保留在测试主体，公共帮助放 support；不增加统一动态类型场景执行器。
`#[rstest]` 放在 `#[tokio::test]` 之前，`start_paused` 等属性继续显式写在 Tokio 入口上。
保留 barrier/channel 握手、重复 wake 和注册竞争压力循环：它们构成同一个并发场景，不能拆成彼此独立的 case。
不为压缩行数把 !Send 测试改成 Send。

## 测试编写与验证

- 并发时序使用 channel、barrier 或明确的就绪信号，不仅靠 sleep 碰竞态。
- 可能造成同步死锁的回归在独立测试子进程执行，父进程设置期限并负责回收。
- 线程亲和性同时检查实际 `ThreadId` 和 Context；Worker 迁移使用可控的跨线程 poll 验证，不假定执行器必定迁移。
- Timer 优先用暂停时间测试；与独立 runtime 配合时明确控制虚拟时间推进，或在关闭前恢复时间。
- 测试使用小线程池，同时覆盖同 Context 串行与不同 Context 并行。
- 简单无参数用例可用原生 test，不必强制添加 rstest。
- 按修改风险补充失败、panic、取消、析构、满队列、超时及注册/清扫或关闭竞争的回归。
- 文档示例用 `cargo test --workspace --doc` 验证。公共库目标通过 `RUSTFLAGS="-D missing-docs" cargo check --workspace --lib`。
- `cargo test --all-targets` 可验证全部 Cargo target；示例零测试 harness 通过不能替代实际运行示例。
- 区分断言失败、超时与环境拦截，准确报告实际执行的命令和结果，不绕过验证。当前工具链通过不代表 MSRV 已验证。
- 已通过的检查只在新增改动、失败或未解决疑点需要时补验。

## 常用命令

名称过滤为子串匹配：

```sh
cargo test --test integration -- --list
cargo test --test integration streams::
cargo test --test integration tcp::
cargo test --test integration tcp_server::
cargo test -p vertx-rs-axum --test service
cargo test --test integration pipe_preserves_primary_and_cleanup_failures::case_04_read_and_close_errors
cargo test --lib eventloop::tests::cancellation::
```

网络示例分别用 `cargo run --example tcp_shared_listener` 和
`cargo run -p vertx-rs-axum --example axum_http1` 实际验证。
[示例说明](examples.md#tcp-与-http-示例)

## 性能基准

`cargo bench --bench runtime` 在 release 模式测量同时存活的 1,000 和 10,000 个任务。每组使用 1 或 2 个 EventLoop，分别开启和关闭诊断。
提交耗时包含所有任务到达首次 poll，清理耗时从发出完成信号到全部任务已析构并返回。快照耗时是在后台任务持续让出并重新排队时，采样 128 次得到的均值。
每组先预热两次，再取七次样本的中位数。前后对比在同一台机器使用相同参数。

在 Apple M4、macOS 27.2.1、Rust 1.100.0 上，对比基线 `158ec91` 与重构后的实现。

| EventLoop | 诊断 | 任务数 | 提交 ms | 清理 ms | 快照 µs |
|---:|---|---:|---:|---:|---:|
| 1 | 关 | 1,000 | 2.920 → 1.164 | 0.468 → 0.895 | 0.036 → 0.030 |
| 1 | 关 | 10,000 | 249.246 → 5.753 | 5.883 → 5.459 | 0.041 → 0.015 |
| 1 | 开 | 1,000 | 2.862 → 0.731 | 0.718 → 0.671 | 0.050 → 0.081 |
| 1 | 开 | 10,000 | 252.293 → 7.156 | 8.046 → 7.219 | 0.051 → 0.084 |
| 2 | 关 | 1,000 | 2.159 → 0.446 | 0.461 → 0.400 | 0.032 → 0.018 |
| 2 | 关 | 10,000 | 137.250 → 3.823 | 4.671 → 3.723 | 0.040 → 0.018 |
| 2 | 开 | 1,000 | 2.214 → 0.668 | 0.905 → 0.844 | 0.042 → 0.082 |
| 2 | 开 | 10,000 | 137.131 → 6.067 | 9.751 → 8.994 | 0.089 → 0.112 |

任务登记改为按 ID 插入、完成时删除，提交耗时随任务规模增长。单 EventLoop、诊断关闭、1,000 个任务的清理耗时增加。
诊断开启时快照有几十纳秒额外耗时，保留当前诊断锁与采样模型。

运行时动态生成独立用例可参考 [libtest-mimic](https://docs.rs/libtest-mimic/latest/libtest_mimic/)，需要自定义 harness，且不支持标准输出捕获。
当前用例在编译期可枚举，保留标准 harness 能继续直接使用现有 cargo/IDE 测试发现和日志捕获。

## 相关主题

[生命周期与关闭](lifecycle.md)、[错误与可观测性](diagnostics.md)。
