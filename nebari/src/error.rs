use std::fmt::Display;

use backtrace::Backtrace;
use thiserror::Error;

#[derive(Debug)]
pub struct Error {
    pub kind: ErrorKind,

    pub backtrace: Backtrace,
}

impl Error {
    pub(crate) fn data_integrity(error: impl Into<Self>) -> Self {
        Self {
            kind: ErrorKind::DataIntegrity(Box::new(error.into())),
            backtrace: Backtrace::new(),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.kind.source()
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.kind.fmt(f)?;

        #[cfg(debug_assertions)]
        {
            f.write_str("\nstack backtrace:")?;
            for (index, frame) in self.backtrace.frames().iter().enumerate() {
                write!(f, "\n#{}: {:?}", index, frame)?;
            }
        }

        Ok(())
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self {
            kind,
            backtrace: Backtrace::new(),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self {
            kind: ErrorKind::from(err),
            backtrace: Backtrace::new(),
        }
    }
}

impl From<&'static str> for Error {
    fn from(message: &'static str) -> Self {
        Self {
            kind: ErrorKind::message(message),
            backtrace: Backtrace::new(),
        }
    }
}

impl From<flume::RecvError> for Error {
    fn from(_err: flume::RecvError) -> Self {
        Self {
            kind: ErrorKind::Internal(InternalError::InternalCommunication),
            backtrace: Backtrace::new(),
        }
    }
}

impl<T> From<flume::SendError<T>> for Error {
    fn from(_err: flume::SendError<T>) -> Self {
        Self {
            kind: ErrorKind::Internal(InternalError::InternalCommunication),
            backtrace: Backtrace::new(),
        }
    }
}

impl From<String> for Error {
    fn from(message: String) -> Self {
        Self {
            kind: ErrorKind::message(message),
            backtrace: Backtrace::new(),
        }
    }
}

/// An error from [`Roots`](crate::Roots).
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
    /// A key was too large.
    #[error("key too large")]
    KeyTooLarge,
    /// A value was too large.
    #[error("value too large")]
    ValueTooLarge,
    /// A multi-key operation did not have its keys ordered.
    #[error("multi-key operation did not have its keys ordered")]
    KeysNotOrdered,
    /// A document ID was too many bytes.
    #[error("document id too large")]
    IdTooLarge,
    /// An internal error occurred. These errors are not intended to be
    /// recoverable and represent some internal error condition.
    #[error("an internal error occurred: {0}")]
    Internal(InternalError),
    /// The underlying database file has been compacted, and the request cannot
    /// be completed. Reopen the file and try again.
    #[error("the file has been compacted. reopen the file and try again")]
    DatabaseCompacted,
}

impl ErrorKind {
    /// Returns a new [`Error::Message`] instance with the message provided.
    pub(crate) fn message<S: Display>(message: S) -> Self {
        Self::Message(message.to_string())
    }

    pub(crate) fn data_integrity(error: impl Into<Error>) -> Self {
        Self::DataIntegrity(Box::new(error.into()))
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
}
