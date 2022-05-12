use std::{
    array::TryFromSliceError,
    convert::Infallible,
    fmt::{Debug, Display},
};

use backtrace::Backtrace;
use parking_lot::{Mutex, MutexGuard};
use thiserror::Error;

use crate::AbortError;

/// An error from Nebari as well as an associated backtrace.
pub struct Error {
    /// The error that occurred.
    pub kind: ErrorKind,

    backtrace: Mutex<Backtrace>,
}

impl Error {
    pub(crate) fn data_integrity(error: impl Into<Self>) -> Self {
        Self {
            kind: ErrorKind::DataIntegrity(Box::new(error.into())),
            backtrace: Mutex::new(Backtrace::new_unresolved()),
        }
    }

    /// Returns the backtrace of where this error was created.
    pub fn backtrace(&self) -> MutexGuard<'_, Backtrace> {
        let mut backtrace = self.backtrace.lock();
        backtrace.resolve();
        backtrace
    }

    fn format_backtrace_frames(&self) -> Vec<String> {
        let mut backtrace = self.backtrace.lock();
        backtrace.resolve();
        backtrace
            .frames()
            .iter()
            .filter_map(|frame| frame.symbols().first())
            .enumerate()
            .map(|(index, symbol)| {
                let mut line = format!("{index}: ");
                if let Some(name) = symbol.name() {
                    line.push_str(&name.to_string());
                    line.push(' ');
                } else if let Some(addr) = symbol.addr() {
                    line.push_str(&format!("{:x}", addr as usize));
                    line.push(' ');
                } else {
                    // Give up on formatting this one.
                    line.push_str(&format!("{symbol:?}"));
                    return line;
                }

                if let Some(file) = symbol.filename() {
                    if let Some(file) = file.to_str() {
                        line.push_str("at ");
                        line.push_str(file);
                    } else {
                        line.push_str(&format!("at {file:?}"));
                    }

                    if let Some(lineno) = symbol.lineno() {
                        line.push(':');
                        line.push_str(&lineno.to_string());
                        if let Some(col) = symbol.colno() {
                            line.push(':');
                            line.push_str(&col.to_string());
                        }
                    }
                }
                line
            })
            .collect()
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.kind.source()
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.kind, f)?;

        #[cfg(debug_assertions)]
        {
            f.write_str("\nstack backtrace:")?;

            for (index, frame) in self.format_backtrace_frames().into_iter().enumerate() {
                write!(f, "{index}: {frame}")?;
            }
        }

        Ok(())
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let frames = self.format_backtrace_frames();
        f.debug_struct("Error")
            .field("kind", &self.kind)
            .field("backtrace", &&frames[..])
            .finish()
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self {
            kind,
            backtrace: Mutex::new(Backtrace::new_unresolved()),
        }
    }
}

impl From<AbortError<Infallible>> for Error {
    fn from(ae: AbortError<Infallible>) -> Self {
        ae.infallible()
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self {
            kind: ErrorKind::from(err),
            backtrace: Mutex::new(Backtrace::new_unresolved()),
        }
    }
}

impl From<&'static str> for Error {
    fn from(message: &'static str) -> Self {
        Self {
            kind: ErrorKind::message(message),
            backtrace: Mutex::new(Backtrace::new_unresolved()),
        }
    }
}

impl From<flume::RecvError> for Error {
    fn from(_err: flume::RecvError) -> Self {
        Self {
            kind: ErrorKind::Internal(InternalError::InternalCommunication),
            backtrace: Mutex::new(Backtrace::new_unresolved()),
        }
    }
}

impl<T> From<flume::SendError<T>> for Error {
    fn from(_err: flume::SendError<T>) -> Self {
        Self {
            kind: ErrorKind::Internal(InternalError::InternalCommunication),
            backtrace: Mutex::new(Backtrace::new_unresolved()),
        }
    }
}

impl From<String> for Error {
    fn from(message: String) -> Self {
        Self {
            kind: ErrorKind::message(message),
            backtrace: Mutex::new(Backtrace::new_unresolved()),
        }
    }
}

impl From<TryFromSliceError> for Error {
    fn from(_: TryFromSliceError) -> Self {
        Self {
            kind: ErrorKind::Internal(InternalError::IncorrectByteLength),
            backtrace: Mutex::new(Backtrace::new_unresolved()),
        }
    }
}

/// An error from Nebari.
#[derive(Debug, Error)]
#[error(transparent)]
pub enum ErrorKind {
    /// An error has occurred. The string contains human-readable error message.
    /// This error is only used in situations where a user is not expected to be
    /// able to recover automatically from the error.
    #[error("{0}")]
    Message(String),
    /// An error occurred while performing IO.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// An unrecoverable data integrity error was encountered.
    #[error("an unrecoverable error with the data on disk has been found: {0}")]
    DataIntegrity(Box<Error>),
    /// An invalid tree name was provided.
    ///
    /// Valid characters are:
    ///
    /// - `'a'..='z'`
    /// - `'A'..='Z'`
    /// - `'0'..='9'`
    /// - `'-'` (Hyphen)
    /// - `'_'` (Underscore)
    /// - `'.'` (Period)
    #[error("tree name not valid")]
    InvalidTreeName,
    /// A key was too large.
    #[error("key too large")]
    KeyTooLarge,
    /// A value was too large.
    #[error("value too large")]
    ValueTooLarge,
    /// A multi-key operation did not have its keys ordered.
    #[error("multi-key operation did not have its keys ordered")]
    KeysNotOrdered,
    /// An internal error occurred. These errors are not intended to be
    /// recoverable and represent some internal error condition.
    #[error("an internal error occurred: {0}")]
    Internal(InternalError),
    /// The underlying tree file has been compacted, and the request cannot
    /// be completed. Reopen the file and try again.
    #[error("the file has been compacted. reopen the file and try again")]
    TreeCompacted,
    /// An error ocurred in the vault.
    #[error("a vault error occurred: {0}")]
    Vault(Box<dyn SendSyncError>),
    /// An transaction was pushed to the log out of order.
    #[error("transaction pushed out of order")]
    TransactionPushedOutOfOrder,
}

pub trait SendSyncError: std::error::Error + Send + Sync + 'static {}
impl<T> SendSyncError for T where T: std::error::Error + Send + Sync + 'static {}

impl ErrorKind {
    /// Returns a new [`Error::Message`] instance with the message provided.
    pub(crate) fn message<S: Display>(message: S) -> Self {
        Self::Message(message.to_string())
    }

    pub(crate) fn data_integrity(error: impl Into<Error>) -> Self {
        Self::DataIntegrity(Box::new(error.into()))
    }

    pub(crate) fn is_file_not_found(&self) -> bool {
        matches!(self, Self::Io(err) if err.kind() == std::io::ErrorKind::NotFound)
    }
}

impl From<&'static str> for ErrorKind {
    fn from(message: &'static str) -> Self {
        Self::message(message)
    }
}

impl From<flume::RecvError> for ErrorKind {
    fn from(_err: flume::RecvError) -> Self {
        Self::Internal(InternalError::InternalCommunication)
    }
}

impl<T> From<flume::SendError<T>> for ErrorKind {
    fn from(_err: flume::SendError<T>) -> Self {
        Self::Internal(InternalError::InternalCommunication)
    }
}

impl From<String> for ErrorKind {
    fn from(message: String) -> Self {
        Self::message(message)
    }
}

/// An internal database error.
#[derive(Debug, Error)]
pub enum InternalError {
    #[error("the b-tree header is too large")]
    HeaderTooLarge,
    #[error("the transaction manager has stopped")]
    TransactionManagerStopped,
    #[error("an error on an internal channel has occurred")]
    InternalCommunication,
    #[error("an unexpected byte length was encountered")]
    IncorrectByteLength,
}
