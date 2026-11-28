use super::route::RouteKind;
use super::{
    Consumer, ConsumerOptions, DeliveryOptions, Envelope, EventBus, Headers, ReplyHandle,
    RequestOptions,
};
use crate::{Result, TaskHandle, VertxError};
use futures::{Stream, StreamExt, stream::FusedStream};
use std::future::Future;
use std::marker::PhantomData;
use std::ops::AsyncFnMut;
use std::pin::Pin;
use std::task::{Context, Poll};

// The route is registered against the exact TypeIds of both parameters.
type RequestTypes<Req, Resp> = PhantomData<fn(Req, Resp) -> (Req, Resp)>;

/// A fixed address shared by the requester and the handler, with one request
/// and reply type. Use these methods to access the address while its typed
/// protocol is registered. Independent declarations are checked at runtime.
///
/// ```
/// use vertx_rs::{RequestAddress, Result, Vertx};
///
/// const LENGTH: RequestAddress<String, usize> = RequestAddress::new("length");
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<()> {
/// let vertx = Vertx::builder().event_loop_threads(1).worker_threads(1).build()?;
/// let bus = vertx.event_bus();
/// let mut consumer = LENGTH.consumer(&bus)?;
/// let (length, handled) = tokio::join!(LENGTH.request(&bus, String::from("rust")), async {
///     let request = consumer.recv().await?.unwrap();
///     let length = request.body().len();
///     request.reply(length)
/// });
/// handled?;
/// assert_eq!(length?, 4);
/// vertx.close().await
/// # }
/// ```
///
/// ```compile_fail,E0308
/// use vertx_rs::{EventBus, RequestAddress};
///
/// const LENGTH: RequestAddress<String, usize> = RequestAddress::new("length");
///
/// async fn wrong_request(bus: &EventBus) {
///     let _ = LENGTH.request(bus, 42u32).await;
/// }
/// ```
pub struct RequestAddress<Req, Resp> {
    address: &'static str,
    types: RequestTypes<Req, Resp>,
}

impl<Req: Send + 'static, Resp: Send + 'static> RequestAddress<Req, Resp> {
    /// Declares an address without registering a consumer.
    pub const fn new(address: &'static str) -> Self {
        Self {
            address,
            types: PhantomData,
        }
    }

    /// Returns the configured address.
    pub fn address(&self) -> &'static str {
        self.address
    }

    /// Registers a request consumer with the default capacity.
    pub fn consumer(&self, bus: &EventBus) -> Result<RequestConsumer<Req, Resp>> {
        self.consumer_with(bus, ConsumerOptions::default())
    }

    /// Registers a request consumer with the given capacity and the bus's
    /// usual Context ownership. Conflicting live protocols return `TypeMismatch`.
    pub fn consumer_with(
        &self,
        bus: &EventBus,
        options: ConsumerOptions,
    ) -> Result<RequestConsumer<Req, Resp>> {
        let consumer =
            bus.consumer_for(self.address, options, RouteKind::request::<Req, Resp>())?;
        Ok(RequestConsumer {
            consumer,
            types: PhantomData,
        })
    }

    /// Waits for the reply body. The default timeout covers admission and reply.
    pub async fn request(&self, bus: &EventBus, body: Req) -> Result<Resp> {
        self.request_with(bus, body, RequestOptions::default())
            .await
    }

    /// Waits for the reply body with custom headers and a total request timeout.
    pub async fn request_with(
        &self,
        bus: &EventBus,
        body: Req,
        options: RequestOptions,
    ) -> Result<Resp> {
        Ok(self
            .request_message_with(bus, body, options)
            .await?
            .into_body())
    }

    /// Waits for the full reply, including headers, with the default timeout.
    pub async fn request_message(&self, bus: &EventBus, body: Req) -> Result<Envelope<Resp>> {
        self.request_message_with(bus, body, RequestOptions::default())
            .await
    }

    /// Waits for the full reply with custom headers and a total request timeout.
    pub async fn request_message_with(
        &self,
        bus: &EventBus,
        body: Req,
        options: RequestOptions,
    ) -> Result<Envelope<Resp>> {
        bus.request_for(
            self.address,
            RouteKind::request::<Req, Resp>(),
            body,
            options,
        )
        .await
    }
}

/// A request subscription with a fixed reply type. Dropping it unregisters
/// the subscription. Handler tasks are managed by the owning deployment.
pub struct RequestConsumer<Req, Resp> {
    consumer: Consumer<Req>,
    types: RequestTypes<Req, Resp>,
}

impl<Req: Send + 'static, Resp: Send + 'static> RequestConsumer<Req, Resp> {
    /// Returns the address of this subscription.
    pub fn address(&self) -> &str {
        self.consumer.address()
    }

    /// Pulls the next request. A terminal failure is returned once.
    pub async fn recv(&mut self) -> Result<Option<IncomingRequest<Req, Resp>>> {
        self.next().await.transpose()
    }

    /// Attaches a serial async handler on the owning EventLoop.
    /// Abort and await the returned task to stop this subscription.
    pub fn handler(
        self,
        mut handler: impl AsyncFnMut(IncomingRequest<Req, Resp>) -> Result<()> + 'static,
    ) -> Result<TaskHandle<()>> {
        self.consumer.handler(async move |envelope| {
            let request = IncomingRequest::from_envelope(envelope)?;
            handler(request).await
        })
    }

    /// Attaches a serial Worker handler. The closure and its future must be `Send`.
    /// Abort and await the returned task to stop this subscription.
    ///
    /// ```compile_fail,E0277
    /// use vertx_rs::{RequestConsumer, Result, TaskHandle};
    ///
    /// fn register(consumer: RequestConsumer<(), usize>) -> Result<TaskHandle<()>> {
    ///     consumer.handler_worker(|request| async move {
    ///         let value = std::rc::Rc::new(42usize);
    ///         tokio::task::yield_now().await;
    ///         request.reply(*value)
    ///     })
    /// }
    /// ```
    pub fn handler_worker<F, Fut>(self, mut handler: F) -> Result<TaskHandle<()>>
    where
        F: FnMut(IncomingRequest<Req, Resp>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.consumer.handler_worker(move |envelope| {
            let future = IncomingRequest::from_envelope(envelope).map(&mut handler);
            async move { future?.await }
        })
    }
}

impl<Req: Send + 'static, Resp: Send + 'static> Stream for RequestConsumer<Req, Resp> {
    type Item = Result<IncomingRequest<Req, Resp>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.consumer).poll_next(cx).map(|next| {
            next.map(|result| {
                let request = IncomingRequest::from_envelope(result?);
                if request.is_err() {
                    self.consumer.terminate();
                }
                request
            })
        })
    }
}

impl<Req: Send + 'static, Resp: Send + 'static> FusedStream for RequestConsumer<Req, Resp> {
    fn is_terminated(&self) -> bool {
        self.consumer.is_terminated()
    }
}

/// An incoming typed request with a one-shot reply of type `Resp`.
///
/// ```compile_fail,E0308
/// use vertx_rs::IncomingRequest;
///
/// fn wrong_reply(request: IncomingRequest<String, usize>) {
///     let _ = request.reply(String::from("wrong type"));
/// }
/// ```
pub struct IncomingRequest<Req, Resp> {
    body: Req,
    headers: Headers,
    reply: TypedReplyHandle<Resp>,
}

impl<Req, Resp: Send + 'static> IncomingRequest<Req, Resp> {
    fn from_envelope(envelope: Envelope<Req>) -> Result<Self> {
        let (body, headers, reply) = envelope.into_parts();
        let reply =
            reply.ok_or_else(|| VertxError::Other("typed request has no reply channel".into()))?;
        Ok(Self {
            body,
            headers,
            reply: TypedReplyHandle {
                reply,
                response: PhantomData,
            },
        })
    }

    /// Borrows the request body.
    pub fn body(&self) -> &Req {
        &self.body
    }

    /// Returns the request headers.
    pub fn headers(&self) -> &Headers {
        &self.headers
    }

    /// Consumes the request and replies with the declared response type.
    pub fn reply(self, response: Resp) -> Result<()> {
        self.reply.reply(response)
    }

    /// Consumes the request and replies with explicit headers.
    pub fn reply_with(self, response: Resp, options: DeliveryOptions) -> Result<()> {
        self.reply.reply_with(response, options)
    }

    /// Consumes the request and sends an application failure.
    pub fn fail(self, code: i32, message: impl Into<String>) -> Result<()> {
        self.reply.fail(code, message)
    }

    /// Extracts the body, headers and reply handle for independent use.
    pub fn into_parts(self) -> (Req, Headers, TypedReplyHandle<Resp>) {
        (self.body, self.headers, self.reply)
    }
}

/// A one-shot typed reply capability that can be moved to another task.
/// Dropping it without replying closes the request's reply channel.
///
/// ```compile_fail,E0382
/// use vertx_rs::TypedReplyHandle;
///
/// fn reply_twice(reply: TypedReplyHandle<usize>) {
///     let _ = reply.reply(1);
///     let _ = reply.reply(2);
/// }
/// ```
pub struct TypedReplyHandle<Resp> {
    reply: ReplyHandle,
    response: PhantomData<fn(Resp) -> Resp>,
}

impl<Resp: Send + 'static> TypedReplyHandle<Resp> {
    /// Sends a reply with empty headers. A late reply returns `ReplyClosed`.
    pub fn reply(self, response: Resp) -> Result<()> {
        self.reply.reply(response)
    }

    /// Sends a reply with explicit headers.
    pub fn reply_with(self, response: Resp, options: DeliveryOptions) -> Result<()> {
        self.reply.reply_with(response, options)
    }

    /// Sends a `RecipientFailure` to the requester.
    pub fn fail(self, code: i32, message: impl Into<String>) -> Result<()> {
        self.reply.fail(code, message)
    }
}
