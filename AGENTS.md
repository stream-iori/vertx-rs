# vertx-rs 开发约定

适用于整个仓库。修改前先阅读 [设计索引](docs/index.md)及相关主题；用户本次明确要求优先。

## 项目边界与文档入口

Rust Future + 固定 OS Worker 线程池 + Tokio IO 驱动实现 Vert.x 5 的单进程执行核心。

- 范围及模块设计见 [docs/index.md](docs/index.md)，仅按任务需要扩展能力。
- 版本、Edition、MSRV 和依赖特性以 [Cargo.toml](Cargo.toml) 为准，不擅自提高 MSRV。
- 项目当前没有使用者，不承担历史 API 兼容或版本迁移要求，不为旧 API 保留兼容层。

## 修改原则

- 先检查 git 状态，保护已有未提交改动，不覆盖或撤销无关内容。
- 先查代码、测试、配置、日志或历史再判断；区分事实与推断，缺少证据时明确说明。文档与实现冲突时先确定修正对象。
- 避免无关改动；必要的结构调整应有明确收益，不因历史约定固定文件名或目录。
- 优先使用现有 std/Tokio 能力；新增依赖、unsafe 或动态类型包装须说明必要性。
- 公共 API 或执行语义变化时，同步更新导出、相关测试、示例及对应主题文档。

## 关键执行底线

- EventLoop 上的普通 Verticle 保持 OS 线程亲和性，支持 `!Send` 状态与 Future。
- Context、Span、Dispatch 按每次 poll/drop 设置和恢复，覆盖失败与取消路径，guard 不跨 await；区分逻辑 Context 与物理线程。
- Worker Future 与 blocking 共用固定线程池；同 Context 的普通 poll/drop 互斥，异步等待释放执行权。EventLoop 不执行同步阻塞或长 CPU 计算。
- 停止与资源准入原子协调；完成通知晚于用户状态析构。部署失败、取消及回滚均需清理并保留原始与清理错误。
- 显式等待关闭，成功返回前完成线程 join；拒绝等待自身。取消不能抢占同步代码或中断已开始的 blocking，Drop 不替代异步清理。
- 调用用户回调、取消 token 或释放可能触发 Waker 的资源前释放注册表锁；执行门仅保持保证串行所需的同步范围。

具体语义见 [执行模型](docs/execution-model.md)、[生命周期](docs/lifecycle.md)、[消息通信](docs/eventbus.md)、[流传输](docs/streams.md)和[诊断](docs/diagnostics.md)。

## 测试与验证

先运行相关测试，按修改风险补回归用例；编写规则见 [测试指南](docs/testing.md)。
Rust 行为或公共 API 变更完成后执行：

```sh
cargo build --lib
cargo test --tests
cargo test --doc
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
git diff --check
```

修改生命周期、调度、EventBus、关闭或示例时，实际运行相关示例；跨模块变更运行全部三个：

```sh
cargo run --example ping_pong
cargo run --example pipeline
cargo run --example verticle_to_verticle
```

纯文档修改只检查内容、链接和差异，不重复运行 Cargo 检查。

## 沟通与提交

- 中文、大白话、结论先行，优先表格或 ASCII 图；交付简述改动、原因、实际验证及限制，不作未经验证的承诺。
- 提交、推送和发布遵循用户授权；已有授权继续执行，不重复询问。
