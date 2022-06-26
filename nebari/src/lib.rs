#![doc = include_str!("./crate-docs.md")]
#![forbid(unsafe_code)]
#![warn(
    clippy::cargo,
    missing_docs,
    // clippy::missing_docs_in_private_items,
    clippy::nursery,
    clippy::pedantic,
    future_incompatible,
    rust_2018_idioms,
)]
#![cfg_attr(doc, deny(rustdoc::all))]
#![allow(
    clippy::missing_errors_doc, // TODO clippy::missing_errors_doc
    clippy::option_if_let_else,
    clippy::module_name_repetitions,
)]

#[macro_use]
pub mod io;
mod error;
// mod roots;
pub mod transaction;
pub mod tree;
mod vault;

mod chunk_cache;
mod context;
pub mod storage;
#[cfg(test)]
mod test_util;

pub use arc_bytes::ArcBytes;
pub use sediment;

pub use self::{
    chunk_cache::{CacheEntry, ChunkCache},
    context::Context,
    error::{AbortError, CompareAndSwapError, Error, ErrorKind, InternalError},
    vault::{AnyVault, Vault},
};

// roots::{
//     AbortError, CompareAndSwapError, Config, ExecutingTransaction, LockedTransactionTree,
//     Roots, ThreadPool, TransactionTree, Tree, UnlockedTransactionTree,
// },
