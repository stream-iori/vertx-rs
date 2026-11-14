# 验收与测试组织

[返回文档索引](index.md)

## 验收

- 同一个 EventLoop 上多 Context 交替 await 不串线；跨 OS 线程 poll 后 TLS 不残留。
- 普通 Verticle 的 factory/start/handler/受管任务/stop/析构始终位于所属 OS 线程与 Context。
- Worker 同 Context 最大普通并行 poll 数为 1，不同 Context 可以并行。
- blocking 的逻辑 Context 和物理 Worker 身份正确；有序 FIFO、无序并发由共用线程数限制。
- Worker 与 blocking 使用同一组实际 ThreadId；Pending 让出线程、跨线程恢复及 wake 合并不丢通知。
- Worker 内真实异步 IO、Timer、timeout 可用；取消析构和析构 panic 不破坏线程池或 Context。
- EventLoop 同线程派发不内联；跨线程早到取消、运行中取消及关闭入队竞争不遗失任务结果。
- 初始化部分失败回收已创建线程；close 成功时全部 EventLoop 已 join，保留 Context、任务句柄或 Waker 不阻止退出。
- start/stop 错误、panic、取消、关闭、批量回滚和自身撤销均有回归覆盖。
- undeploy 成功返回前受管用户状态已析构；不可中断工作超时返回错误，随后可继续关闭。
- 路由注册/清扫/最后订阅注销竞争不残留路由；请求超时包含满队列等待。
- 业务失败、重复 headers、完整响应、迟到回复、广播满队列行为明确。
- Stream 终止错误只产生一次；Sink 重复就绪、排队取消及目标注销不泄漏许可；publish 先获许可再 Clone。
- pipe 验证背压、有限 EOF、异步 flush/close、主错与清理错误、!Send/!Unpin、取消析构及 Context/Span 隔离。
- 队列快照区分 queued/reserved/waiting，详情受限；并发关闭/注销后活跃计数归零。
- Timer 延后首次 poll、scope/global 取消、周期 Skip 均有测试。
- cargo test、cargo clippy --all-targets -- -D warnings、三个示例运行及格式检查通过。
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
| 嵌套测试组 | 文件内 `mod` 分组，保留现有源文件和 integration target |
| 动态测试工厂 | 暂无运行时扫描文件或协议数据生成用例的需求；未来需要时单独评估自定义 harness |

```text
tests/integration.rs
 ├─ support：独立实例 fixture、Verticle 适配、日志缓冲及可控 Sink
 ├─ configuration / lifecycle / shutdown
 ├─ context / worker / blocking
 ├─ eventbus / shared_state / timers
 └─ errors / diagnostics / tracing_propagation / streams

src/* 的 #[cfg(test)]：内部不变量，保留在实现所在模块
 └─ EventLoop / Worker 内再按调度、取消、关闭及 panic 恢复分组
```

fixture 不使用 `#[once]` 共享 Vertx、Context、runtime 或当前 Dispatch；每例持有独立资源。
fixture 只消除重复创建，时序、消息、断言和清理保留在测试主体；不增加统一动态类型场景执行器。
`#[rstest]` 放在 `#[tokio::test]` 之前，`start_paused` 等属性继续显式写在 Tokio 入口上。
保留 barrier/channel 握手、重复 wake 和注册竞争压力循环：它们构成同一个并发场景，不能拆成彼此独立的 case。
不为压缩行数把 !Send 测试改成 Send，不用自动重试或放宽断言掩盖时序问题。

常用命令（名称过滤为子串匹配）：

```sh
cargo test --test integration -- --list
cargo test --test integration streams::
cargo test --test integration pipe_preserves_primary_and_cleanup_failures::case_04_read_and_close_errors
cargo test --lib eventloop::tests::cancellation::
```

运行时动态生成独立用例可参考 [libtest-mimic](https://docs.rs/libtest-mimic/latest/libtest_mimic/)，需要自定义 harness，且不支持标准输出捕获。
当前用例在编译期可枚举，保留标准 harness 能继续直接使用现有 cargo/IDE 测试发现和日志捕获。

## 相关主题

[生命周期与关闭](lifecycle.md)、[错误与可观测性](diagnostics.md)。
