use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
};

use super::btree::KeyOperation;
use crate::{error::Error, transaction::TransactionId, ArcBytes, ErrorKind};

/// A tree modification.
#[derive(Debug)]
pub struct Modification<'a, T, Index> {
    /// The transaction ID to store with this change.
    pub persistence_mode: PersistenceMode,
    /// The keys to operate upon.
    pub keys: Vec<ArcBytes<'a>>,
    /// The operation to perform on the keys.
    pub operation: Operation<'a, T, Index>,
}

impl<'a, T, Index> Modification<'a, T, Index> {
    /// Prepares this modification for efficient operation, and ensures that the
    /// keys are properly ordered.
    ///
    /// After calling this function, the keys and values (if applicable) are
    /// reversed so that keys and values can be removed by calling [`Vec::pop`].
    pub fn prepare(&mut self) -> Result<(), Error> {
        if self.keys.windows(2).all(|w| w[0] < w[1]) {
            self.keys.reverse();
            if let Operation::SetEach(values) = &mut self.operation {
                values.reverse();
            }
            Ok(())
        } else {
            Err(Error::from(ErrorKind::KeysNotOrdered))
        }
    }
}

/// Controls the persistence guarantees of write operations.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum PersistenceMode {
    /// Transactional writes are always fully synchronized, which means that
    /// data written is persisted to the best of the abilities provided by the
    /// operating system hosting Nebari. Additionally, the written data is
    /// tagged as belonging to this `Transactionid`.
    Transactional(TransactionId),
    /// Data written is persisted to the best of the abilities provided by the
    /// operating system hosting Nebari.
    Sync,
    /// Data written is not buffered by Nebari, but may still be in
    /// operating-system level caches. In the event of a power loss or sudden
    /// application failure, the data written may not be available when the tree
    /// is reopened. Nebari will automatically recover to the last write that
    /// was fully synchronized to disk, but writing using this persistence mode
    /// moves the control of synchronization from Nebari to the operating
    /// system.
    Flush,
}

impl PersistenceMode {
    /// Returns the transaction ID for writes performed with this mode, if applicable.
    #[must_use]
    pub const fn transaction_id(&self) -> Option<TransactionId> {
        if let Self::Transactional(id) = self {
            Some(*id)
        } else {
            None
        }
    }

    /// Returns true if writes should be fully synchronized before control is
    /// returned to the caller.
    #[must_use]
    pub const fn should_synchronize(&self) -> bool {
        matches!(self, Self::Transactional(_) | Self::Sync)
    }
}

impl From<TransactionId> for PersistenceMode {
    fn from(id: TransactionId) -> Self {
        Self::Transactional(id)
    }
}

impl From<Option<TransactionId>> for PersistenceMode {
    fn from(maybe_transactional: Option<TransactionId>) -> Self {
        maybe_transactional.map_or(Self::Sync, Self::Transactional)
    }
}

/// An operation that is performed on a set of keys.
pub enum Operation<'a, T, Index> {
    /// Sets all keys to the value.
    Set(T),
    /// Sets each key to the corresponding entry in this value. The number of
    /// keys must match the number of values.
    SetEach(Vec<T>),
    /// Removes the keys.
    Remove,
    /// Executes the `CompareSwap`. The original value (or `None` if not
    /// present) is the only argument.
    CompareSwap(CompareSwap<'a, T, Index>),
}

impl<'a, T: Debug, Index> Debug for Operation<'a, T, Index> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Set(arg0) => f.debug_tuple("Set").field(arg0).finish(),
            Self::SetEach(arg0) => f.debug_tuple("SetEach").field(arg0).finish(),
            Self::Remove => write!(f, "Remove"),
            Self::CompareSwap(_) => f.debug_tuple("CompareSwap").finish(),
        }
    }
}

/// A function that is allowed to check the current value of a key and determine
/// how to operate on it. The first parameter is the key, and the second
/// parameter is the current value, if present.
pub type CompareSwapFn<'a, T, Index> =
    dyn FnMut(&ArcBytes<'a>, Option<&Index>, Option<T>) -> KeyOperation<T> + 'a;

/// A wrapper for a [`CompareSwapFn`].
pub struct CompareSwap<'a, T, Index>(&'a mut CompareSwapFn<'a, T, Index>);

impl<'a, T, Index> CompareSwap<'a, T, Index> {
    /// Returns a new wrapped callback.
    pub fn new<F: FnMut(&ArcBytes<'_>, Option<&Index>, Option<T>) -> KeyOperation<T> + 'a>(
        callback: &'a mut F,
    ) -> Self {
        Self(callback)
    }
}

impl<'a, T, Index> Debug for CompareSwap<'a, T, Index> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CompareSwap(dyn FnMut)")
    }
}

impl<'a, T, Index> Deref for CompareSwap<'a, T, Index> {
    type Target = CompareSwapFn<'a, T, Index>;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a, T, Index> DerefMut for CompareSwap<'a, T, Index> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
    }
}
