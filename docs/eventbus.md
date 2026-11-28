# EventBus

[返回文档索引](index.md)

`eventbus.rs` 导出消息类型。`bus.rs` 按消费者注册时的执行位置确定作用域。
地址路由、投递、消费和队列各自在对应子模块实现。

```text
address → Route { protocol, subscribers, round-robin cursor }
                       ↓
             每消费者 mpsc 队列，默认容量 256，可单独配置
                       ↓
                 Envelope<T>
                       ↓ 可选
             oneshot<Result<ErasedEnvelope>>
```

| API | 行为 |
|---|---|
| consumer<T> | 同地址存活消费者类型必须一致；冲突在注册时返回 TypeMismatch |
| consumer_with<T> | ConsumerOptions 配置消息数量上限；容量必须在 1..=Semaphore::MAX_PERMITS 内 |
| Consumer::recv / Stream | recv 返回 Result<Option<Envelope<T>>>；Stream 的 Item 为 Result<Envelope<T>> |
| send / send_with | 轮询选择一个消费者；等待该队列空位；成功表示入队 |
| try_send | 不等待；区分 QueueFull、ChannelClosed、NoConsumer |
| publish | 向所有消费者尝试取得许可，成功后才 Clone；满队列丢弃该次投递并记录丢弃数 |
| sink / sink_with | 地址绑定的 MessageSink<T>，实现 futures::Sink；后者配置固定 headers |
| request / request_with | 返回响应体；默认 30 秒；一个 deadline 覆盖入队和回复 |
| request_message / request_message_with | 返回完整响应 Envelope，可读取响应 headers |
| RequestAddress<Req, Resp> | 共享固定地址及请求和回复类型。通过地址本身请求和订阅 |
| handler / handler_worker | 返回 TaskHandle<()>，可以单独取消该订阅并等待清理 |
| reply / reply_with | 一次性响应，后者支持响应 headers |
| fail(code, message) | 返回 RecipientFailure |
| into_parts | 拆出 body、Headers 和可跨任务移动的 ReplyHandle |

`DeliveryOptions` 给发送、广播、Sink 和回复配置 headers。请求用 `RequestOptions`，其中 timeout 覆盖入队和回复等待。
请求取消或超时后，已入队的消息仍可被消费。迟到 reply 返回 `ReplyClosed`。
Headers 按插入顺序保存，键大小写敏感且可重复。`get` 返回第一个值的 `&str`，`get_all` 遍历所有匹配的值。

## 固定请求地址

```rust
use vertx_rs::RequestAddress;

const LOOKUP: RequestAddress<u64, String> = RequestAddress::new("lookup");
```

调用方用 `LOOKUP.request(&bus, id).await` 获取 `String`，需要响应 headers 时用 `LOOKUP.request_message(&bus, id)`。
服务端用 `LOOKUP.consumer(&vertx.event_bus()?)?` 取得 `RequestConsumer`，可拉取 `IncomingRequest<u64, String>` 或启动 handler。
`reply` 消耗请求并接受 `String`。需要延迟回复时，`into_parts` 可拆出保留同一类型约束的 `TypedReplyHandle<String>`。

首个存活的订阅确定地址协议。普通字符串地址记录消息体类型，固定请求地址同时记录请求和回复类型。
在同一实例中，把同一个字符串用于两种入口，或者独立声明了不同请求或回复类型，都会在注册或入队前得到 `TypeMismatch`。
最后一个订阅注销后可以重新绑定地址。队列和请求超时使用同一套规则，等待超时后已入队的消息仍可被消费。

字节协议的编码和解码可以由应用完成。可运行示例把 `Vec<u8>` 作为普通消息体，并显式检查字节长度。 [eventbus_codec](examples.md#eventbus_codec-字节编解码)

## 归属与竞态

应用和 Verticle 都用 `vertx.event_bus()?` 获取同一实例的路由句柄。注册时决定消费者的归属，发送按地址查表。

| 注册位置 | 归属 | 可用入口 |
|---|---|---|
| 同实例的部署任务 | 当前部署 | `recv`、Stream、当前执行类型对应的 handler |
| 同实例的实例级任务 | 实例 | `recv`、Stream、EventLoop handler |
| 框架外、裸 Tokio 任务或其他实例 | 目标实例路由 | `recv`、Stream |

- `handler` 在所属 EventLoop 逐条等待异步闭包完成。用 `async move |env|` 把状态交给回调，同一个回调可反复调用，也可以跨 `.await` 可变借用自己持有的 `!Send` 状态。
- `handler_worker` 使用 `FnMut -> Future`，闭包和返回的 Future 都要求 `Send`。两个入口都校验调用 Context。
- 丢弃 handler 返回的 `TaskHandle` 会分离等待者，任务继续由原作用域管理。调用 `abort()` 后等待句柄，确认当前处理 Future 已析构且订阅已注销。取消在执行器取得执行权后生效。
- EventBus 路由表拥有订阅和队列。受管消费者在作用域登记地址和注册 ID，生命周期可以按登记准确注销。
- 注册受管消费者时，在同一次准入内写入路由和作用域登记。进入 Stopping 后拒绝新订阅。
- 撤销部署只处理属于它的登记。关闭 EventBus 清除所有路由，包括框架外的注册。发送始终按地址查表。
- Consumer Drop 按地址和订阅 ID 注销单条记录。每次移除都和路由更新原子进行，移除地址下最后一个订阅时同时清除轮询状态。
- 注销先在路由锁内标记失活、移除目标并更新计数，再释放相关注册表锁，取消并释放 sender；Waker 可重入路由。

## 相关主题

[Stream、Sink 与背压传输](streams.md)、[生命周期与关闭](lifecycle.md)。
