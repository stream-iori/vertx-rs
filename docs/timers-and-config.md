# Timer 与配置

[返回文档索引](index.md)

Timer 创建时记录绝对 deadline，首次 poll 再晚也不会重新计算延迟。
Periodic 创建时同样记录起点，首次 tick 才初始化 Tokio interval；错过 tick 使用 Skip 策略。
取消检查优先于同轮到期，is_cancelled 同时反映显式取消和作用域取消。
完成的一次性 Timer 不因后续关闭变为已取消。
Context Timer 随 scope 取消；全局 Timer 随 close 取消。
创建接口返回 Result；零周期为 InvalidOptions，关闭后创建返回相应生命周期错误。
等待 Timer/tick 需要 Tokio 时间驱动；Timer 对象不会把调用方 Future 派发到其他 Context。

| 配置 | 默认 |
|---|---|
| EventLoop 线程数 | 2 × available_parallelism()，builder 可覆盖 |
| Worker 与 blocking 共用的 OS 线程数 | 20，worker_threads 可覆盖 |
| shutdown_grace | 1 秒 |
| 消费者容量 | 256；consumer_with 可覆盖 |
| 请求 timeout | 30 秒 |

有序 blocking 使用每 Context 的公平锁保持 FIFO，跟踪记录覆盖排队、实际执行及调用方取消。
普通 Context 执行门不用于 blocking 闭包。等待有序锁或尚未开始的操作在作用域关闭后不再执行用户闭包，已开始执行的操作必须自然结束。
阻塞任务提交后，等待方取消不撤回操作；跟踪记录保持到实际闭包结束。
池内线程被同步操作占满时，其他 Worker 工作等待；关闭超时检测在 EventLoop/调用方上继续工作。

## 工具链与依赖


最低 Rust 1.96、Edition 2024、Cargo resolver 3；库版本保持 0.3.0。
VertxBuilder、DeploymentOptions、DeliveryOptions、ConsumerOptions 标记 must_use，避免丢弃配置结果。
本轮只新增 try_build 与 execute_blocking_unordered；不为已有枚举/公开字段结构体添加 non_exhaustive，
保留旧签名、结构体构造和穷尽匹配的兼容性。
直接依赖最低版本为 Tokio 1.54.0、tokio-util 0.7.20、async-trait 0.1.92、thiserror 2.0.21、
futures 0.3.34、dashmap 6.2.1、tracing 0.1.44；示例/测试使用 tracing-subscriber 0.3.23 的 env-filter/json。
普通依赖仍只启用 Tokio rt/sync/macros/time；开发依赖启用 full/test-util。保留 async-trait。
测试使用 rstest 0.28.0，关闭默认特性；参数化和 fixture 只影响开发依赖，不向库引入额外计时器或执行器。
Cargo.lock 沿用库项目的忽略策略；这里记录最低版本，不承诺后续解析始终锁定到同一组补丁版本。

## 相关主题

[执行模型与 Context](execution-model.md)、[原生 Worker 调度与 Future 组合](worker-and-futures.md)。
