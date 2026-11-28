# vertx-rs 0.3 设计文档索引

目标：用 Rust Future、固定 OS Worker 线程池和 Tokio IO 驱动实现 Vert.x 5 的**单进程执行核心**。
应用从 `Vertx` 调用服务。Verticle 通过 `VertxHandle` 使用同一组业务 API。框架在内部管理 Context、EventLoop、Worker 和资源归属。

```text
Vertx / VertxHandle → 当前受管执行或实例作用域 → EventLoop / Worker
Deployment::spawn  → 指定部署的执行环境
```

## 主题导航

| 文档 | 阅读用途 |
|---|---|
| [执行模型与 Context](execution-model.md) | 线程亲和性、Context 隔离、EventLoop 派发、共享 TCP 监听及初始化 |
| [生命周期与关闭](lifecycle.md) | start/stop、受管任务、部署、撤销与关闭 |
| [EventBus](eventbus.md) | 消息 API、消费者归属及路由竞态 |
| [Stream、Sink 与背压传输](streams.md) | Consumer Stream、MessageSink、pipe 与背压 |
| [Timer 与配置](timers-and-config.md) | Timer、默认配置、blocking 顺序及工具链依赖 |
| [错误与可观测性](diagnostics.md) | 错误聚合、诊断快照、日志、耗时及 panic 边界 |
| [示例与共享状态](examples.md) | 示例介绍及共享状态建议 |
| [与 Vert.x 5 的对照与范围](scope.md) | 能力对照、范围边界及官方语义依据 |
| [原生 Worker 调度与 Future 组合](worker-and-futures.md) | Worker 调度、wake 合并及 Future 组合语义 |
| [验收与测试组织](testing.md) | 验收要求、测试分组、fixture 及常用命令 |

## 架构图

- [单进程执行架构](../docs_site/vertx-rs-architecture.html)
- [EventBus 请求与回复流程](../docs_site/vertx-rs-struct-workflow.html)
- [实例与部署结构体关系](../docs_site/vertx-struct-relations.html)

## 建议阅读顺序

- **初次了解**：[范围与对照](scope.md) → [执行模型](execution-model.md) → [生命周期](lifecycle.md) → [消息通信](eventbus.md) → [示例与共享状态](examples.md)。
- **开发维护**：按下表进入对应主题，再核对[验收与测试要求](testing.md)。

| 修改内容 | 优先阅读 |
|---|---|
| Context、线程与派发 | [执行模型](execution-model.md)、[Worker 调度](worker-and-futures.md) |
| 部署、任务与关闭 | [生命周期](lifecycle.md) |
| 路由、消费者与消息队列 | [EventBus](eventbus.md) |
| Stream、Sink 与 pipe | [背压传输](streams.md) |
| TCP 监听、连接限额与 HTTP 接入 | [共享 TCP 监听](execution-model.md#共享-tcp-监听)、[网络示例](examples.md#tcp-与-http-示例) |
| Timer、blocking 与构建配置 | [Timer 与配置](timers-and-config.md)、[执行模型](execution-model.md) |
| 错误、日志与诊断 | [错误与可观测性](diagnostics.md) |

## 维护约定

- 设计正文按主题维护，公共 API 或执行语义变化时同步更新对应主题。
- 项目当前没有使用者，不承担历史版本兼容和迁移要求；文档只描述当前设计与用法。
- 版本、Edition、MSRV 与依赖特性以 [Cargo.toml](../Cargo.toml) 为准。
- 修改实现前先阅读[开发约定](../AGENTS.md)和相关主题，测试安排见[验收与测试组织](testing.md)。
