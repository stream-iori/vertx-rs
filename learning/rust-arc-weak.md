# 用 Weak 打破 Arc 强引用环

两个对象需要互相访问时，可以让拥有方用 `Arc` 持有另一方，另一方用 `Weak` 回指。这样既能找到拥有方，也允许它在最后一份强引用释放后执行 `Drop`。

| 类型或方法 | 行为 |
|---|---|
| `Arc<T>` | 共享所有权。最后一份强引用被释放时，`T` 会被析构。 |
| `Arc::downgrade(&arc)` | 创建一个 `Weak<T>`，强引用计数保持不变。 |
| `weak.upgrade()` | 对象还活着时返回 `Some(Arc<T>)`，对象已析构时返回 `None`。 |

如果两个对象互相持有 `Arc`，即使外部句柄全部释放，两个对象仍会各自持有对方的一份强引用，引用计数都到不了零。把回指改为 `Weak`，就能打破这个强引用环。

## vertx-rs 的例子

```text
VertxInner ── Arc ──→ ContextShared
    ↑                       │
    └──────── Weak ─────────┘
```

- `VertxInner` 通过 `Arc<ContextShared>` 持有实例级资源，各个 `Context` 也能共享这些资源。
- `ContextShared` 保存 `Weak<VertxInner>`。构建时，`Arc::new_cyclic()` 把这个弱引用交给初始化闭包，供它写入 `ContextShared`。
- `Context::owner()` 需要实例句柄时才调用 `upgrade()`。即使某个 `Context` 还持有 `ContextShared`，这个回指也不会延长 `VertxInner` 的生命周期。

[共享字段](../src/context.rs#L38) · [获取 owner](../src/context.rs#L143) · [构建实例](../src/vertx.rs#L674)

## Vertx 的 #[derive(Clone)]

`#[derive(Clone)]` 让编译器为 `Vertx` 生成 `Clone` 实现，依次克隆它的每个字段。目前唯一的字段是 `inner: Arc<VertxInner>`，所以生成的行为等价于下面的手写实现。[Vertx 定义](../src/vertx.rs#L32)

```rust
impl Clone for Vertx {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}
```

`Arc::clone(&self.inner)` 增加强引用计数，返回一个指向同一份 `VertxInner` 的 `Arc`。所以 `vertx.clone()` 得到的是同一个运行时的另一个句柄，两个句柄共用线程池、EventBus 和部署表。通过任意一个句柄关闭实例，其他句柄也会看到同一次关闭。

`VertxInner` 本身无需实现 `Clone`。`Arc<VertxInner>` 这个字段类型决定了共享所有权，`#[derive(Clone)]` 则让外层 `Vertx` 句柄也能调用 `.clone()`。
