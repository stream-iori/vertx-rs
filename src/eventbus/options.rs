use std::collections::HashMap;
use std::time::Duration;

/// Ordered message headers. Keys are case-sensitive and can have multiple values.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Headers(Vec<(String, String)>);
impl Headers {
    /// Creates an empty header collection.
    pub fn new() -> Self {
        Self::default()
    }
    /// Appends a value, retaining any earlier entries with the same key.
    pub fn append(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.0.push((key.into(), value.into()));
    }
    /// Replaces all values for the key and appends the replacement at the end.
    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let key = key.into();
        self.0.retain(|(k, _)| k != &key);
        self.append(key, value);
    }
    /// Returns the first value for a case-sensitive key.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
    /// Iterates over values for a case-sensitive key in insertion order.
    pub fn get_all<'a>(&'a self, key: &'a str) -> impl Iterator<Item = &'a str> + 'a {
        self.0
            .iter()
            .filter(move |(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
    /// Iterates over all header entries in insertion order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }
    /// Returns the entry count, including duplicate keys.
    pub fn len(&self) -> usize {
        self.0.len()
    }
    /// Reports whether there are any header entries.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
impl From<HashMap<String, String>> for Headers {
    fn from(m: HashMap<String, String>) -> Self {
        Self(m.into_iter().collect())
    }
}
impl FromIterator<(String, String)> for Headers {
    fn from_iter<T: IntoIterator<Item = (String, String)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}
/// Headers attached to a message sent without awaiting a reply, or to a reply.
#[derive(Debug, Clone, Default)]
#[must_use = "options take effect only when passed to an operation"]
pub struct DeliveryOptions {
    headers: Headers,
}
impl DeliveryOptions {
    /// Creates delivery options with no headers.
    pub fn new() -> Self {
        Self::default()
    }
    /// Appends a header without replacing existing values.
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.append(key, value);
        self
    }
    /// Replaces the entire set of headers.
    pub fn with_headers(mut self, headers: impl Into<Headers>) -> Self {
        self.headers = headers.into();
        self
    }
    /// Returns the headers to attach to the message.
    pub fn headers(&self) -> &Headers {
        &self.headers
    }
    pub(crate) fn into_headers(self) -> Headers {
        self.headers
    }
}

/// Headers and the total deadline for a request, including admission and reply.
#[derive(Debug, Clone)]
#[must_use = "options take effect only when passed to a request"]
pub struct RequestOptions {
    timeout: Duration,
    delivery: DeliveryOptions,
}

impl Default for RequestOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            delivery: DeliveryOptions::default(),
        }
    }
}

impl RequestOptions {
    /// Creates request options with the default 30-second timeout and empty headers.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the time budget for both queue admission and receiving a reply.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Appends a request header, allowing duplicate keys.
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.delivery = self.delivery.header(key, value);
        self
    }

    /// Replaces all request headers.
    pub fn with_headers(mut self, headers: impl Into<Headers>) -> Self {
        self.delivery = self.delivery.with_headers(headers);
        self
    }

    /// Returns the total request timeout.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Returns the headers to send with the request.
    pub fn headers(&self) -> &Headers {
        self.delivery.headers()
    }

    pub(crate) fn into_headers(self) -> Headers {
        self.delivery.into_headers()
    }
}

/// Per-consumer hard queue capacity, measured in messages, not bytes.
#[derive(Debug, Clone)]
#[must_use = "options take effect only when passed to an operation"]
pub struct ConsumerOptions {
    capacity: usize,
}
impl Default for ConsumerOptions {
    fn default() -> Self {
        Self { capacity: 256 }
    }
}
impl ConsumerOptions {
    /// Creates options with the default capacity of 256 messages.
    pub fn new() -> Self {
        Self::default()
    }
    /// Sets the capacity, validated at registration against Tokio's permit bounds.
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }
    /// Returns the configured maximum number of queued or reserved messages.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    pub(crate) fn validate(&self) -> crate::Result<()> {
        if self.capacity == 0 || self.capacity > tokio::sync::Semaphore::MAX_PERMITS {
            Err(crate::VertxError::InvalidOptions(
                "consumer capacity must be in 1..=Semaphore::MAX_PERMITS".into(),
            ))
        } else {
            Ok(())
        }
    }
}
