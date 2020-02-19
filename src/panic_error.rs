use std::{any::Any, error::Error, fmt};

/// Thread panic error.
pub struct PanicError {
    inner: Box<dyn Any + Send + 'static>,
}

impl fmt::Display for PanicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.as_str(), f)
    }
}

impl fmt::Debug for PanicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

impl PanicError {
    pub(crate) fn new(inner: Box<dyn Any + Send + 'static>) -> Self {
        PanicError { inner }
    }

    fn as_str(&self) -> &str {
        self.inner
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| self.inner.downcast_ref::<&'static str>().copied())
            .unwrap_or("Box<dyn Any>")
    }
}

impl Error for PanicError {}
