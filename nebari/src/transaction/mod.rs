//! ACID-compliant transaction log and manager.

mod log;
mod manager;
mod state;

use std::fmt::Display;

pub use self::{
    log::{LogEntry, TransactionLog},
    manager::*,
    state::*,
};

/// A unique identifier of a transaction within a transaction log.
#[derive(Default, Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct TransactionId(pub u64);

impl TransactionId {
    #[must_use]
    pub(crate) const fn valid(self) -> bool {
        self.0 > 0
    }
}

impl Display for TransactionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
