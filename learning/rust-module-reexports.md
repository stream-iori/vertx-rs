# Rust 模块可见性与重导出

**看一个名字能从哪里用，要同时看可见性和调用路径。** `pub use` 在当前模块增加一个公开路径，`pub(crate) use` 增加一个只对当前 crate 可见的路径。两者指向的仍是同一个类型。

## vertx-rs 的 TCP 例子

```rust
// src/tcp.rs
mod listener;
mod server;

pub use listener::TcpIncoming;
pub(crate) use listener::{ListenerRegistration, TcpListeners};

// src/lib.rs
mod tcp;
pub use tcp::{TcpConnection, TcpIncoming, TcpServer, TcpServerOptions};
```

两次 `pub use` 把 `TcpIncoming` 从实现模块一路导出到库根。

```text
listener::TcpIncoming
    → crate::tcp::TcpIncoming    tcp.rs 在 tcp 模块这一层重导出
    → crate::TcpIncoming         lib.rs 在库根再次重导出
    → vertx_rs::TcpIncoming      外部 crate 使用库根的公开路径
```

| 路径 | 谁能用 |
|---|---|
| `crate::tcp::TcpIncoming`、`crate::TcpIncoming` | `vertx-rs` 库内部。两条路径指向同一个类型。 |
| `vertx_rs::TcpIncoming` | 外部 crate 可以使用，这是 `lib.rs` 提供的公开入口。 |
| `vertx_rs::tcp::TcpIncoming` | 外部不可访问，因为库根的 `mod tcp;` 是私有模块声明。 |
| `crate::tcp::TcpListeners`、`crate::tcp::ListenerRegistration` | `vertx-rs` 库内部可以使用。它们由 `pub(crate) use` 暴露在 `tcp` 这一层。 |

`crate` 指当前正在编译的 crate。Axum 适配器和 `tests/` 下的集成测试各自编译为独立 crate，即使位于同一仓库，也只能通过库的公开路径访问类型。库源码内部的 `#[cfg(test)]` 单测模块仍可访问该库的 `pub(crate)` 项。

[TCP 模块入口](../src/tcp.rs#L3) · [库根重导出](../src/lib.rs#L36) · [库内导入示例](../src/context.rs#L11)
