use std::collections::HashMap;
use std::time::Duration;

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
/// Ordered message headers. Keys are case-sensitive and can have multiple values.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Headers(Vec<(String, String)>);
impl Headers {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn append(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.0.push((key.into(), value.into()));
    }
    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let key = key.into();
        self.0.retain(|(k, _)| k != &key);
        self.append(key, value);
    }
    pub fn get(&self, key: &str) -> Option<&String> {
        self.0.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }
    pub fn get_all<'a>(&'a self, key: &'a str) -> impl Iterator<Item = &'a str> + 'a {
        self.0
            .iter()
            .filter(move |(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
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
#[derive(Debug, Clone, Default)]
pub struct DeliveryOptions {
    timeout: Option<Duration>,
    headers: Headers,
}
impl DeliveryOptions {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.append(key, value);
        self
    }
    pub fn with_headers(mut self, headers: impl Into<Headers>) -> Self {
        self.headers = headers.into();
        self
    }
    pub fn timeout(&self) -> Duration {
        self.timeout.unwrap_or(DEFAULT_TIMEOUT)
    }
    pub fn headers(&self) -> &Headers {
        &self.headers
    }
    pub(crate) fn take_headers(self) -> Headers {
        self.headers
    }
}
