//! A wrapper about `IpcError` that implement `Error` and stuff.

use ipc_channel::ipc::IpcError;
use std::{error::Error, fmt};

/// IPC error wrapper.
pub struct IpcErrorWrapper(
    /// Original IPC error.
    pub IpcError,
);

impl IpcErrorWrapper {
    /// Checks whether this error represents a "disconnected" error.
    pub fn is_disconnected(&self) -> bool {
        if let IpcError::Disconnected = self.0 {
            true
        } else {
            false
        }
    }
}

impl From<IpcError> for IpcErrorWrapper {
    fn from(e: IpcError) -> Self {
        Self(e)
    }
}

impl fmt::Display for IpcErrorWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            IpcError::Io(io) => write!(f, "I/O error in IPC: {}", io),
            IpcError::Bincode(err) => write!(f, "Bincode error in IPC: {}", err),
            IpcError::Disconnected => write!(f, "IPC endpoint disconnected"),
        }
    }
}

impl fmt::Debug for IpcErrorWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for IpcErrorWrapper {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.0 {
            IpcError::Io(err) => Some(err),
            IpcError::Bincode(err) => Some(err),
            IpcError::Disconnected => None,
        }
    }
}
