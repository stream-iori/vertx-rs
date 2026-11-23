# 把 Arc 放在真正需要共享的那一层

**规则：每个使用方有自己的身份、归属或配置时，用一个小结构体保存这些差异，在里面用 `Arc` 共享公共资源。**

## 我的体会

`MerchantId` 绑定的是具体的商户客户端句柄。商户 A 和 B 各有自己的句柄，释放 A 的句柄，B 仍然能用。这种「有自己的身份、可以单独使用和释放」的东西，可以用一个小结构体包装起来。

它们共同依赖的网关客户端是技术层的资源，代表对同一个网关的访问，可以复用连接池。所以共享的是 `Arc<GatewayClient>`，每个句柄自己带上 `MerchantId`。

```rust
struct PaymentClient {
    merchant_id: MerchantId,
    gateway: Arc<GatewayClient>,
}
```

这个例子假设商户身份和鉴权信息按请求传入。如果客户端在创建时已经绑定了商户凭据，共享范围就应限于使用同一套凭据的调用方。支付单号、金额这类每次都不同的数据，由调用方作为方法参数传入。

## 在 vertx-rs 里怎么用

| 对比 | 句柄自己保存 | 用 `Arc` 共享 |
|---|---|---|
| 支付客户端 | `MerchantId` | 网关客户端和连接池 |
| `EventBus` | `owner: Option<Context>`，决定新消费者归哪个 Context | `EventBusInner`，包含实例共用的路由表 |

`ctx.event_bus()` 创建一个绑定当前 Context 的句柄，里面的 `Arc<EventBusInner>` 仍然指向原来的路由表。[绑定实现](../src/eventbus/mod.rs#L32) · [消费者注册](../src/eventbus/mod.rs#L201)

判断时问自己：**多个调用方需要同一个带身份的对象，还是各自带身份、共用底层资源？** 同一家商户的多个任务可以共享整个 `Arc<PaymentClient>`。不同商户可以各自持有 `PaymentClient`，再共享里面的网关客户端。
