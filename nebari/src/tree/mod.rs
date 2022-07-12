//! Append-only B-Tree implementation
//!
//! The file format is loosely inspired by
//! [Couchstore](https://github.com/couchbase/couchstore). Nebari is not
//! compatible with Couchstore in any way.
//!
//! ## Numbers and Alignment
//!
//! - All numbers are encoded in big-endian format/network byte order.
//! - All values are tightly packed. There is no padding or alignment that isn't
//!   explicitly included.
//!
//! ## File Organization
//!
//! There is no way to read this file format starting at byte 0 and iterating
//! forward. The contents of any given byte offset are unknown until the file's
//! current root header has been found.
//!
//! When writing data to the file, it will be appended to the end of the file.
//! When a tree is committed, all of the changed nodes will be appended to the
//! end of the file, except for the Root.
//!
//! Before writing the Root, the file is padded to a multiple of
//! [`PAGE_SIZE`]. A 3-byte magic code is written, followed by a byte for the
//! [`PageHeader`].
//!
//! The Root is then serialized and written as a chunk.
//!
//! To locate the most recent header, take the file's length and find the
//! largest multiple of [`PAGE_SIZE`]. Check the first three bytes at that
//! offset for the magic code. If found, attempt to read a chunk. If successful,
//! attempt to deserialize the Root.
//!
//! If any step fails, loop back through the file at each [`PAGE_SIZE`] offset
//! until a valid header is found.
//!
//! ## Chunks
//!
//! Each time a value, B-Tree node, or header is written, it is written as a
//! chunk. If a [`Vault`](crate::Vault) is in-use, each chunk will be
//! pre-processed by the vault before a `CRC-32-BZIP2` checksum is calculated. A
//! chunk is limited to 4 gigabytes of data (2^32).
//!
//! The chunk is written as:
//!
//! - `u32` - Data length, excluding the header.
//! - `u32` - CRC
//! - `[u8]` - Contents

use std::{
    cell::RefCell,
    collections::HashMap,
    fmt::{Debug, Display},
    hash::BuildHasher,
    iter,
    marker::PhantomData,
    ops::{Bound, Deref, DerefMut, Range, RangeBounds},
    sync::Arc,
};

use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};
use sediment::{
    database::PendingCommit,
    format::GrainId,
    io::{self, paths::PathId},
};

use crate::{
    chunk_cache::CacheEntry,
    error::{CompareAndSwapError, Error},
    storage::{sediment::SedimentFile, BlobStorage},
    transaction::TransactionManager,
    tree::{
        btree::{BTreeNode, Indexer, KeyOperation, Reducer, ScanArgs},
        state::{AnyTreeState, CommitStateGuard},
    },
    vault::AnyVault,
    AbortError, ArcBytes, ChunkCache, Context, ErrorKind,
};

// CompareAndSwapError, transaction::{CommittedTreeState, ManagedTransaction, TransactionManager},

/// B+Tree types
pub mod btree;
mod by_id;
mod by_sequence;
mod interior;
mod key_entry;
mod modify;
pub(crate) mod root;
mod serialization;
pub(crate) mod state;
mod unversioned;
mod versioned;

pub(crate) const DEFAULT_MAX_ORDER: usize = 1000;

pub use self::{
    by_id::{ByIdIndexer, ByIdStats, UnversionedByIdIndex, VersionedByIdIndex},
    by_sequence::{BySequenceIndex, BySequenceReducer, BySequenceStats, SequenceId},
    interior::{Interior, Pointer},
    key_entry::{KeyEntry, PositionIndex},
    modify::{CompareSwap, CompareSwapFn, Modification, Operation, PersistenceMode},
    root::{AnyTreeRoot, Root, TreeRoot},
    serialization::BinarySerialization,
    state::{ActiveState, State},
    unversioned::{Unversioned, UnversionedTreeRoot},
    versioned::{KeySequence, SequenceEntry, SequenceIndex, Versioned, VersionedTreeRoot},
};

/// The header byte for a tree file's page.
#[derive(Eq, PartialEq)]
pub enum PageHeader {
    /// A [`VersionedTreeRoot`] header.
    VersionedHeader = 2,
    /// An [`UnversionedTreeRoot`] header.
    UnversionedHeader = 3,
}

impl TryFrom<u8> for PageHeader {
    type Error = ErrorKind;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            2 => Ok(Self::VersionedHeader),
            3 => Ok(Self::UnversionedHeader),
            _ => Err(ErrorKind::data_integrity(format!(
                "invalid block header: {}",
                value
            ))),
        }
    }
}

/// An append-only tree file.
///
/// ## Generics
/// - `File`: An [`ManagedFile`] implementor.
#[derive(Debug)]
pub struct TreeFile<Root: root::Root, File: io::FileManager> {
    /// The file handle the tree is stored within.
    pub file: SedimentFile<File>,
    /// The state of the file.
    pub state: State<Root>,
    /// The vault used to encrypt/decrypt chunks.
    pub vault: Option<Arc<dyn AnyVault>>,
    /// The cache used to cache chunks from the file.
    pub cache: Option<ChunkCache>,
    scratch: Vec<u8>,
}

impl<Root: root::Root, File: io::FileManager> TreeFile<Root, File> {
    /// Returns a tree as contained in `file`.
    ///
    /// `state` should already be initialized using [`Self::initialize_state`] if the file exists.
    pub fn new(
        file: SedimentFile<File>,
        state: State<Root>,
        vault: Option<Arc<dyn AnyVault>>,
        cache: Option<ChunkCache>,
    ) -> Result<Self, Error> {
        Ok(Self {
            file,
            state,
            vault,
            cache,
            scratch: Vec::new(),
        })
    }

    /// Opens a tree file
    pub fn open<Path: AsRef<std::path::Path> + ?Sized>(
        path: &Path,
        state: State<Root>,
        context: &Context<File>,
        transactions: Option<&TransactionManager<File>>,
    ) -> Result<Self, Error> {
        let mut file = SedimentFile::open(path, true, context.file_manager.clone())?;
        Self::initialize_state(&mut file, &state, context.vault(), transactions)?;
        Self::new(file, state, context.vault.clone(), context.cache.clone())
    }

    /// Opens a tree file using an existing [`SedimentFile`].
    pub fn open_sediment_file(
        mut file: SedimentFile<File>,
        state: State<Root>,
        context: &Context<File>,
        transactions: Option<&TransactionManager<File>>,
    ) -> Result<Self, Error> {
        Self::initialize_state(&mut file, &state, context.vault(), transactions)?;
        Self::new(file, state, context.vault.clone(), context.cache.clone())
    }

    /// Attempts to load the last saved state of this tree into `state`.
    pub fn initialize_state(
        storage: &mut SedimentFile<File>,
        state: &State<Root>,
        vault: Option<&dyn AnyVault>,
        transaction_manager: Option<&TransactionManager<File>>,
    ) -> Result<(), Error> {
        {
            let read_state = state.read();
            if read_state.initialized() {
                return Ok(());
            }
        }

        let mut active_state = state.lock();
        if active_state.initialized() {
            return Ok(());
        }
        active_state.file_id = Some(storage.unique_id().id);

        let header_bytes = storage.read_header()?;
        if header_bytes.is_empty() {
            active_state.root.initialize_default();
            active_state.publish(None, state);
            return Ok(());
        }

        #[allow(clippy::match_on_vec_items)]
        match (&header_bytes[0..3], PageHeader::try_from(header_bytes[3])) {
            (b"Nbr", Ok(header)) => {
                if header != Root::HEADER {
                    return Err(Error::data_integrity(format!(
                        "Tree {:?} contained another header type",
                        storage.unique_id()
                    )));
                }

                let contents = header_bytes.slice(4..);

                let contents = if let Some(vault) = vault {
                    ArcBytes::from(vault.decrypt(&contents)?)
                } else {
                    contents
                };

                let root = Root::deserialize(contents, active_state.root.reducer().clone())
                    .map_err(|err| ErrorKind::DataIntegrity(Box::new(err)))?;

                if let Some(transaction_manager) = transaction_manager {
                    if root.transaction_id().valid()
                        && !transaction_manager.transaction_was_successful(root.transaction_id())?
                    {
                        // The transaction wasn't written successfully, so
                        // we cannot trust the data present.
                        todo!("need rollback")
                        // if block_start == 0 {
                        //     // No data was ever fully written.
                        //     active_state.root.initialize_default();
                        //     return Ok(());
                        // }
                        // block_start -= PAGE_SIZE as u64;
                        // continue;
                    }
                }
                active_state.root = root;

                active_state.publish(None, state);
                Ok(())
            }
            (_, Ok(_) | Err(_)) => Err(Error::data_integrity("invalid header encountered")),
        }
    }

    /// Sets a key/value pair. Replaces any previous value if set. If you wish
    /// to retrieve the previously stored value, use
    /// [`replace()`](Self::replace) instead.
    ///
    /// Returns the new/updated index for this key.
    pub fn set(
        &mut self,
        persistence_mode: impl Into<PersistenceMode>,
        key: impl Into<ArcBytes<'static>>,
        value: impl Into<Root::Value>,
    ) -> Result<Root::Index, Error> {
        Ok(TreeModifier {
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            modification: Some(Modification {
                persistence_mode: persistence_mode.into(),
                keys: vec![key.into()],
                operation: Operation::Set(value.into()),
            }),
            scratch: &mut self.scratch,
        }
        .modify(&mut self.file)?
        .into_iter()
        .next()
        .expect("always produces a single result")
        .index
        .expect("modification always produces a new index"))
    }

    /// Executes a modification. Returns a list of modified keys and their
    /// updated indexes, if the keys are still present.
    pub fn modify(
        &mut self,
        modification: Modification<'_, Root::Value, Root::Index>,
    ) -> Result<Vec<ModificationResult<Root::Index>>, Error> {
        TreeModifier {
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            modification: Some(modification),
            scratch: &mut self.scratch,
        }
        .modify(&mut self.file)
    }

    /// Compares the value of `key` against `old`. If the values match, key will
    /// be set to the new value if `new` is `Some` or removed if `new` is
    /// `None`.
    pub fn compare_and_swap<Old>(
        &mut self,
        key: &[u8],
        old: Option<&Old>,
        mut new: Option<Root::Value>,
        persistence_mode: impl Into<PersistenceMode>,
    ) -> Result<(), CompareAndSwapError<Root::Value>>
    where
        Old: PartialEq + ?Sized,
        Root::Value: AsRef<Old> + Clone,
    {
        let mut result = Ok(());
        self.modify(Modification {
            persistence_mode: persistence_mode.into(),
            keys: vec![ArcBytes::from(key)],
            operation: Operation::CompareSwap(CompareSwap::new(&mut |_key,
                                                                     _index,
                                                                     value: Option<
                Root::Value,
            >| {
                if old == value.as_ref().map(AsRef::as_ref) {
                    match new.take() {
                        Some(new) => KeyOperation::Set(new),
                        None => KeyOperation::Remove,
                    }
                } else {
                    result = Err(CompareAndSwapError::Conflict(value));
                    KeyOperation::Skip
                }
            })),
        })?;
        result
    }

    /// Removes `key` and returns the existing value and index, if present.
    pub fn remove(
        &mut self,
        key: &[u8],
        persistence_mode: impl Into<PersistenceMode>,
    ) -> Result<Option<TreeValueIndex<Root>>, Error> {
        let mut existing_value = None;
        self.modify(Modification {
            persistence_mode: persistence_mode.into(),
            keys: vec![ArcBytes::from(key)],
            operation: Operation::CompareSwap(CompareSwap::new(
                &mut |_key, index: Option<&Root::Index>, value| {
                    existing_value = if let (Some(index), Some(value)) = (index, value) {
                        Some(ValueIndex {
                            value,
                            index: index.clone(),
                        })
                    } else {
                        None
                    };
                    KeyOperation::Remove
                },
            )),
        })?;
        Ok(existing_value)
    }

    /// Sets `key` to `value`. Returns a tuple containing two elements:
    ///
    /// - The previously stored value, if a value was already present.
    /// - The new/updated index for this key.
    #[allow(clippy::missing_panics_doc)]
    pub fn replace(
        &mut self,
        key: impl Into<ArcBytes<'static>>,
        value: impl Into<Root::Value>,
        persistence_mode: impl Into<PersistenceMode>,
    ) -> Result<(Option<Root::Value>, Root::Index), Error> {
        let mut existing_value = None;
        let mut value = Some(value.into());
        let result = self
            .modify(Modification {
                persistence_mode: persistence_mode.into(),
                keys: vec![key.into()],
                operation: Operation::CompareSwap(CompareSwap::new(
                    &mut |_, _index, stored_value| {
                        existing_value = stored_value;
                        KeyOperation::Set(value.take().unwrap())
                    },
                )),
            })?
            .into_iter()
            .next()
            .unwrap();

        Ok((existing_value, result.index.unwrap()))
    }

    /// Gets the value stored for `key`.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn get(&mut self, key: &[u8], in_transaction: bool) -> Result<Option<Root::Value>, Error> {
        let mut buffer = None;
        TreeGetter {
            from_transaction: in_transaction,
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            keys: KeyRange::new(std::iter::once(key)),
            key_reader: |_key, value, _index| {
                buffer = Some(value);
                Ok(())
            },
            key_evaluator: |_, _| ScanEvaluation::ReadData,
        }
        .get(&mut self.file)?;
        Ok(buffer)
    }

    /// Gets the index stored for `key`.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn get_index(
        &mut self,
        key: &[u8],
        in_transaction: bool,
    ) -> Result<Option<Root::Index>, Error> {
        let mut found_index = None;
        TreeGetter {
            from_transaction: in_transaction,
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            keys: KeyRange::new(std::iter::once(key)),
            key_reader: |_, _, _| unreachable!(),
            key_evaluator: |_key, index| {
                found_index = Some(index.clone());
                ScanEvaluation::Skip
            },
        }
        .get(&mut self.file)?;
        Ok(found_index)
    }

    /// Gets the value and index stored for `key`.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn get_with_index(
        &mut self,
        key: &[u8],
        in_transaction: bool,
    ) -> Result<Option<TreeValueIndex<Root>>, Error> {
        let mut buffer = None;
        let mut found_index = None;
        TreeGetter {
            from_transaction: in_transaction,
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            keys: KeyRange::new(std::iter::once(key)),
            key_reader: |_key, value, index| {
                buffer = Some(value);
                found_index = Some(index);
                Ok(())
            },
            key_evaluator: |_, _| ScanEvaluation::ReadData,
        }
        .get(&mut self.file)?;
        if let (Some(value), Some(index)) = (buffer, found_index) {
            Ok(Some(ValueIndex { value, index }))
        } else {
            Ok(None)
        }
    }

    /// Gets the values stored in `keys`. Does not error if a key is missing.
    /// Returns key/value pairs in an unspecified order. Keys are required to be
    /// pre-sorted.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self, keys)))]
    pub fn get_multiple<'keys, KeysIntoIter, KeysIter>(
        &mut self,
        keys: KeysIntoIter,
        in_transaction: bool,
    ) -> Result<Vec<(ArcBytes<'static>, Root::Value)>, Error>
    where
        KeysIntoIter: IntoIterator<Item = &'keys [u8], IntoIter = KeysIter>,
        KeysIter: Iterator<Item = &'keys [u8]> + ExactSizeIterator,
    {
        let keys = keys.into_iter();
        let mut buffers = Vec::with_capacity(keys.len());
        TreeGetter {
            from_transaction: in_transaction,
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            keys: KeyRange::new(keys),
            key_reader: |key, value, _| {
                buffers.push((key, value));
                Ok(())
            },
            key_evaluator: |_, _| ScanEvaluation::ReadData,
        }
        .get(&mut self.file)?;
        Ok(buffers)
    }

    /// Gets the indexes stored in `keys`. Does not error if a key is missing.
    /// Returns key/value pairs in an unspecified order. Keys are required to be
    /// pre-sorted.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self, keys)))]
    pub fn get_multiple_indexes<'keys, KeysIntoIter, KeysIter>(
        &mut self,
        keys: KeysIntoIter,
        in_transaction: bool,
    ) -> Result<Vec<(ArcBytes<'static>, Root::Index)>, Error>
    where
        KeysIntoIter: IntoIterator<Item = &'keys [u8], IntoIter = KeysIter>,
        KeysIter: Iterator<Item = &'keys [u8]> + ExactSizeIterator,
    {
        let keys = keys.into_iter();
        let mut buffers = Vec::with_capacity(keys.len());
        TreeGetter {
            from_transaction: in_transaction,
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            keys: KeyRange::new(keys),
            key_reader: |key, _value, index| {
                buffers.push((key, index));
                Ok(())
            },
            key_evaluator: |_, _| ScanEvaluation::ReadData,
        }
        .get(&mut self.file)?;
        Ok(buffers)
    }

    /// Gets the values and indexes stored in `keys`. Does not error if a key is
    /// missing. Returns key/value pairs in an unspecified order. Keys are
    /// required to be pre-sorted.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self, keys)))]
    pub fn get_multiple_with_indexes<'keys, KeysIntoIter, KeysIter>(
        &mut self,
        keys: KeysIntoIter,
        in_transaction: bool,
    ) -> Result<Vec<TreeEntry<Root>>, Error>
    where
        KeysIntoIter: IntoIterator<Item = &'keys [u8], IntoIter = KeysIter>,
        KeysIter: Iterator<Item = &'keys [u8]> + ExactSizeIterator,
    {
        let keys = keys.into_iter();
        let mut buffers = Vec::with_capacity(keys.len());
        TreeGetter {
            from_transaction: in_transaction,
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            keys: KeyRange::new(keys),
            key_reader: |key, value, index| {
                buffers.push(Entry { key, value, index });
                Ok(())
            },
            key_evaluator: |_, _| ScanEvaluation::ReadData,
        }
        .get(&mut self.file)?;
        Ok(buffers)
    }

    /// Retrieves all keys and values for keys that are contained by `range`.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn get_range<'keys, KeyRangeBounds>(
        &mut self,
        range: &'keys KeyRangeBounds,
        in_transaction: bool,
    ) -> Result<Vec<(ArcBytes<'static>, Root::Value)>, Error>
    where
        KeyRangeBounds: RangeBounds<&'keys [u8]> + Debug + ?Sized,
    {
        let mut results = Vec::new();
        self.scan(
            range,
            true,
            in_transaction,
            |_, _, _| ScanEvaluation::ReadData,
            |_, _| ScanEvaluation::ReadData,
            |key, _index, value| {
                results.push((key, value));
                Ok(())
            },
        )?;
        Ok(results)
    }

    /// Retrieves all keys and indexes for keys that are contained by `range`.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn get_range_indexes<'keys, KeyRangeBounds>(
        &mut self,
        range: &'keys KeyRangeBounds,
        in_transaction: bool,
    ) -> Result<Vec<(ArcBytes<'static>, Root::Index)>, Error>
    where
        KeyRangeBounds: RangeBounds<&'keys [u8]> + Debug + ?Sized,
    {
        let mut results = Vec::new();
        self.scan(
            range,
            true,
            in_transaction,
            |_, _, _| ScanEvaluation::ReadData,
            |key, index| {
                results.push((key.clone(), index.clone()));
                ScanEvaluation::Skip
            },
            |_key, _index, _value| unreachable!(),
        )?;
        Ok(results)
    }

    /// Retrieves all keys and values and indexes for keys that are contained by
    /// `range`.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn get_range_with_indexes<'keys, KeyRangeBounds>(
        &mut self,
        range: &'keys KeyRangeBounds,
        in_transaction: bool,
    ) -> Result<Vec<TreeEntry<Root>>, Error>
    where
        KeyRangeBounds: RangeBounds<&'keys [u8]> + Debug + ?Sized,
    {
        let mut results = Vec::new();
        self.scan(
            range,
            true,
            in_transaction,
            |_, _, _| ScanEvaluation::ReadData,
            |_, _| ScanEvaluation::ReadData,
            |key, index, value| {
                results.push(Entry {
                    key,
                    value,
                    index: index.clone(),
                });
                Ok(())
            },
        )?;
        Ok(results)
    }

    /// Scans the tree across all nodes that might contain nodes within `range`.
    ///
    /// If `forwards` is true, the tree is scanned in ascending order.
    /// Otherwise, the tree is scanned in descending order.
    ///
    /// `node_evaluator` is invoked for each [`Interior`] node to determine if
    /// the node should be traversed. The parameters to the callback are:
    ///
    /// - `&ArcBytes<'static>`: The maximum key stored within the all children
    ///   nodes.
    /// - `&Root::ReducedIndex`: The reduced index value stored within the node.
    /// - `usize`: The depth of the node. The root nodes are depth 0.
    ///
    /// The result of the callback is a [`ScanEvaluation`]. To read children
    /// nodes, return [`ScanEvaluation::ReadData`].
    ///
    /// `key_evaluator` is invoked for each key encountered that is contained
    /// within `range`. For all [`ScanEvaluation::ReadData`] results returned,
    /// `callback` will be invoked with the key and values. `callback` may not
    /// be invoked in the same order as the keys are scanned.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(self, node_evaluator, key_evaluator, key_reader))
    )]
    pub fn scan<'keys, CallerError, KeyRangeBounds, NodeEvaluator, KeyEvaluator, DataCallback>(
        &mut self,
        range: &'keys KeyRangeBounds,
        forwards: bool,
        in_transaction: bool,
        node_evaluator: NodeEvaluator,
        key_evaluator: KeyEvaluator,
        key_reader: DataCallback,
    ) -> Result<(), AbortError<CallerError>>
    where
        KeyRangeBounds: RangeBounds<&'keys [u8]> + Debug + ?Sized,
        NodeEvaluator: FnMut(&ArcBytes<'static>, &Root::ReducedIndex, usize) -> ScanEvaluation,
        KeyEvaluator: FnMut(&ArcBytes<'static>, &Root::Index) -> ScanEvaluation,
        DataCallback: FnMut(
            ArcBytes<'static>,
            &Root::Index,
            Root::Value,
        ) -> Result<(), AbortError<CallerError>>,
        CallerError: Display + Debug,
    {
        TreeScanner {
            forwards,
            from_transaction: in_transaction,
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            range,
            node_evaluator,
            key_reader,
            key_evaluator,
            _phantom: PhantomData,
        }
        .scan(&mut self.file)?;
        Ok(())
    }

    /// Returns the reduced index over the provided range. This is an
    /// aggregation function that builds atop the `scan()` operation which calls
    /// [`Reducer::reduce()`] and [`Reducer::rereduce()`] on all matching
    /// indexes stored within the nodes of this tree, producing a single
    /// aggregated [`Root::ReducedIndex`] value.
    ///
    /// If no keys match, the returned result is what [`Reducer::rereduce()`]
    /// returns when an empty slice is provided.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn reduce<'keys, KeyRangeBounds>(
        &mut self,
        range: &'keys KeyRangeBounds,
        in_transaction: bool,
    ) -> Result<Option<Root::ReducedIndex>, Error>
    where
        KeyRangeBounds: RangeBounds<&'keys [u8]> + Debug + ?Sized,
        Root::Index: Clone,
    {
        let reducer = {
            let state = self.state.lock();
            state.root.reducer().clone()
        };
        let reduce_state = RefCell::new(ReduceState::new(reducer));
        TreeScanner {
            forwards: true,
            from_transaction: in_transaction,
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            range,
            node_evaluator: |max_key, index, depth| {
                let mut state = reduce_state.borrow_mut();
                state.reduce_to_depth(depth);
                let start_is_after_max = match range.start_bound() {
                    Bound::Unbounded => false,
                    Bound::Excluded(start) => start >= &max_key.as_slice(),
                    Bound::Included(start) => start > &max_key.as_slice(),
                };
                let start_is_lowest = match range.start_bound() {
                    Bound::Unbounded => true,
                    Bound::Excluded(start) => start < &state.lowest_key.as_slice(),
                    Bound::Included(start) => start <= &state.lowest_key.as_slice(),
                };
                let end_included = match range.end_bound() {
                    Bound::Included(end) => end <= &max_key.as_slice(),
                    Bound::Excluded(end) => end < &max_key.as_slice(),
                    Bound::Unbounded => true,
                };
                if start_is_after_max {
                    // We are beyond the end, we can stop scanning.
                    ScanEvaluation::Stop
                } else if end_included && start_is_lowest {
                    // The node is fully included. Copy the index to the
                    // stack and skip all the children.
                    state.push_reduced(depth, index.clone());
                    ScanEvaluation::Skip
                } else {
                    // This node is partially contained.
                    ScanEvaluation::ReadData
                }
            },
            key_evaluator: |key, index| {
                if range.contains(&key.as_slice()) {
                    let mut state = reduce_state.borrow_mut();
                    state.push_index(index.clone());
                }
                ScanEvaluation::Skip
            },
            key_reader: |_, _, _| unreachable!(),
            _phantom: PhantomData,
        }
        .scan(&mut self.file)?;
        let reduce_state = reduce_state.into_inner();
        Ok(reduce_state.finish())
    }

    /// Returns the first key of the tree.
    pub fn first_key(&mut self, in_transaction: bool) -> Result<Option<ArcBytes<'static>>, Error> {
        let mut result = None;
        self.scan(
            &(..),
            true,
            in_transaction,
            |_, _, _| ScanEvaluation::ReadData,
            |key, _index| {
                result = Some(key.clone());
                ScanEvaluation::Stop
            },
            |_key, _index, _value| Ok(()),
        )?;

        Ok(result)
    }

    /// Returns the first key and value of the tree.
    pub fn first(
        &mut self,
        in_transaction: bool,
    ) -> Result<Option<(ArcBytes<'static>, Root::Value)>, Error> {
        let mut result = None;
        let mut key_requested = false;
        self.scan(
            &(..),
            true,
            in_transaction,
            |_, _, _| ScanEvaluation::ReadData,
            |_, _| {
                if key_requested {
                    ScanEvaluation::Stop
                } else {
                    key_requested = true;
                    ScanEvaluation::ReadData
                }
            },
            |key, _index, value| {
                result = Some((key, value));
                Ok(())
            },
        )?;

        Ok(result)
    }

    /// Returns the last key of the tree.
    pub fn last_key(&mut self, in_transaction: bool) -> Result<Option<ArcBytes<'static>>, Error> {
        let mut result = None;
        self.scan(
            &(..),
            false,
            in_transaction,
            |_, _, _| ScanEvaluation::ReadData,
            |key, _index| {
                result = Some(key.clone());
                ScanEvaluation::Stop
            },
            |_key, _index, _value| Ok(()),
        )?;

        Ok(result)
    }

    /// Returns the last key and value of the tree.
    pub fn last(
        &mut self,
        in_transaction: bool,
    ) -> Result<Option<(ArcBytes<'static>, Root::Value)>, Error> {
        let mut result = None;
        let mut key_requested = false;
        self.scan(
            &(..),
            false,
            in_transaction,
            |_, _, _| ScanEvaluation::ReadData,
            |_, _| {
                if key_requested {
                    ScanEvaluation::Stop
                } else {
                    key_requested = true;
                    ScanEvaluation::ReadData
                }
            },
            |key, _index, value| {
                result = Some((key, value));
                Ok(())
            },
        )?;

        Ok(result)
    }

    /// Begins a transactional commit. This is only needed if writes were done
    /// with a transaction id.
    ///
    /// The returned [`CommittedTreeState`] must be provided to the
    /// [`TransactionManager`] to publish the newly written state. If None is
    /// returned, the tree had no changes.
    pub fn begin_commit(&mut self) -> Result<Option<CommittedTreeState<File>>, Error> {
        // We need to move the current session into the CommittedTreeState. To
        // do this, we're going to create a clone (which has no session), and
        // then swap it out so that `file` has the session.
        let mut file = self.file.clone();
        std::mem::swap(&mut file, &mut self.file);

        TreeWriter {
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            scratch: &mut self.scratch,
        }
        .commit(file)
    }

    // /// Rewrites the database, removing all unused data in the process. For a
    // /// `VersionedTreeRoot`, this will remove old version information.
    // ///
    // /// This process is done atomically by creating a new file containing the
    // /// active data. Once the new file has all the current file's data, the file
    // /// contents are swapped using atomic file operations.
    // pub fn compact(
    //     mut self,
    //     file_manager: &File::Manager,
    //     transactions: Option<TransactableCompaction<'_, File::Manager>>,
    // ) -> Result<Self, Error> {
    //     let (compacted_file, finisher) = self.file.execute(TreeCompactor {
    //         state: &self.state,
    //         manager: file_manager,
    //         vault: self.vault.as_deref(),
    //         transactions,
    //         scratch: &mut self.scratch,
    //     })?;
    //     self.file = self
    //         .file
    //         .replace_with(compacted_file, file_manager, |file_id| {
    //             finisher.finish(file_id.id().expect("id can't be none at this stage"));
    //         })?;
    //     Ok(self)
    // }
}

#[derive(Debug)]
struct ReduceState<R, I, RI> {
    depths: Vec<DepthState<I, RI>>,
    lowest_key: ArcBytes<'static>,
    reducer: R,
}

impl<R, I, RI> ReduceState<R, I, RI>
where
    I: Clone,
    RI: Clone,
    R: Reducer<I, RI>,
{
    fn new(reducer: R) -> Self {
        Self {
            depths: vec![DepthState::default()],
            lowest_key: ArcBytes::default(),
            reducer,
        }
    }

    fn reduce_to_depth(&mut self, depth: usize) {
        while self.depths.len() > depth + 1 {
            let state_to_reduce = self.depths.pop().unwrap();
            if let Some(reduced) = state_to_reduce.finish(&self.reducer) {
                self.depths.last_mut().unwrap().reduced.push(reduced);
            }
        }
    }

    fn push_reduced(&mut self, depth: usize, reduced: RI) {
        if self.depths.len() < depth + 1 {
            self.depths.resize(depth + 1, DepthState::default());
        }
        self.depths[depth].reduced.push(reduced);
    }

    fn push_index(&mut self, index: I) {
        self.depths.last_mut().unwrap().indexes.push(index);
    }

    fn finish(mut self) -> Option<RI> {
        self.reduce_to_depth(0);
        self.depths.pop().unwrap().finish(&self.reducer)
    }
}

#[derive(Clone, Debug)]
struct DepthState<I, RI> {
    reduced: Vec<RI>,
    indexes: Vec<I>,
}

impl<I, RI> Default for DepthState<I, RI> {
    fn default() -> Self {
        Self {
            reduced: Vec::default(),
            indexes: Vec::default(),
        }
    }
}

impl<I, RI> DepthState<I, RI>
where
    I: Clone,
    RI: Clone,
{
    fn finish<R>(mut self, reducer: &R) -> Option<RI>
    where
        R: Reducer<I, RI>,
    {
        if !self.indexes.is_empty() {
            self.reduced.push(reducer.reduce(self.indexes.iter()));
        }

        (!self.reduced.is_empty()).then(|| reducer.rereduce(self.reduced.iter()))
    }
}

impl<File: io::FileManager, Index> TreeFile<VersionedTreeRoot<Index>, File>
where
    Index: EmbeddedIndex<ArcBytes<'static>> + Clone + Debug + 'static,
{
    /// Scans the tree for keys that are contained within `range`. If `forwards`
    /// is true, scanning starts at the lowest sort-order key and scans forward.
    /// Otherwise, scanning starts at the highest sort-order key and scans
    /// backwards. `key_evaluator` is invoked for each key as it is encountered.
    /// For all [`ScanEvaluation::ReadData`] results returned, `callback` will be
    /// invoked with the key and values. The callback may not be invoked in the
    /// same order as the keys are scanned.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(self, key_evaluator, data_callback))
    )]
    pub fn scan_sequences<CallerError, Range, KeyEvaluator, DataCallback>(
        &mut self,
        range: Range,
        forwards: bool,
        in_transaction: bool,
        mut key_evaluator: KeyEvaluator,
        data_callback: DataCallback,
    ) -> Result<(), AbortError<CallerError>>
    where
        Range: RangeBounds<SequenceId> + Debug + 'static,
        KeyEvaluator: FnMut(KeySequence<Index>) -> ScanEvaluation,
        DataCallback:
            FnMut(KeySequence<Index>, ArcBytes<'static>) -> Result<(), AbortError<CallerError>>,
        CallerError: Display + Debug,
    {
        TreeSequenceScanner {
            forwards,
            from_transaction: in_transaction,
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            range: &U64Range::new(range).borrow_as_bytes(),
            key_evaluator: &mut move |key: &ArcBytes<'_>, index: &BySequenceIndex<Index>| {
                let id = SequenceId(BigEndian::read_u64(key));
                key_evaluator(KeySequence {
                    key: index.key.clone(),
                    sequence: id,
                    last_sequence: index.last_sequence,
                    embedded: index.embedded.clone(),
                })
            },
            data_callback,
        }
        .scan(&mut self.file)?;
        Ok(())
    }

    /// Retrieves the keys and values associated with one or more `sequences`.
    /// The value retrieved is the value of the key at the given [`SequenceId`].
    /// If a sequence is not found, it will not appear in the result map. If
    /// the value was removed, None is returned for the value.
    pub fn get_multiple_by_sequence<Sequences>(
        &mut self,
        sequences: Sequences,
        in_transaction: bool,
    ) -> Result<HashMap<SequenceId, (ArcBytes<'static>, Option<ArcBytes<'static>>)>, Error>
    where
        Sequences: Iterator<Item = SequenceId>,
    {
        let results = RefCell::new(HashMap::new());
        TreeSequenceGetter {
            keys: sequences,
            from_transaction: in_transaction,
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            key_evaluator: |sequence, index| {
                results
                    .borrow_mut()
                    .insert(sequence, (index.key.clone(), None));
                ScanEvaluation::ReadData
            },
            key_reader: |sequence, _index, value| {
                results
                    .borrow_mut()
                    .get_mut(&sequence)
                    .expect("reader can't be invoked without evaluator")
                    .1 = Some(value);
                Ok(())
            },
        }
        .get(&mut self.file)?;
        Ok(results.into_inner())
    }

    /// Retrieves the keys and indexes associated with one or more `sequences`.
    /// The value retrieved is the value of the key at the given [`SequenceId`].
    /// If a sequence is not found, it will not appear in the result list.
    pub fn get_multiple_indexes_by_sequence<Sequences>(
        &mut self,
        sequences: Sequences,
        in_transaction: bool,
    ) -> Result<Vec<SequenceIndex<Index>>, Error>
    where
        Sequences: Iterator<Item = SequenceId>,
    {
        let mut results = Vec::new();
        TreeSequenceGetter {
            keys: sequences,
            from_transaction: in_transaction,
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            key_evaluator: |sequence, index| {
                results.push(SequenceIndex {
                    sequence,
                    index: index.clone(),
                });
                ScanEvaluation::Skip
            },
            key_reader: |_, _, _| unreachable!(),
        }
        .get(&mut self.file)?;
        Ok(results)
    }

    /// Retrieves the keys, values, and indexes associated with one or more
    /// `sequences`. The value retrieved is the value of the key at the given
    /// [`SequenceId`]. If a sequence is not found, it will not appear in the
    /// result list.
    pub fn get_multiple_with_indexes_by_sequence<Sequences>(
        &mut self,
        sequences: Sequences,
        in_transaction: bool,
    ) -> Result<HashMap<SequenceId, SequenceEntry<Index>>, Error>
    where
        Sequences: Iterator<Item = SequenceId>,
    {
        let results = RefCell::new(HashMap::new());
        TreeSequenceGetter {
            keys: sequences,
            from_transaction: in_transaction,
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            key_evaluator: |sequence, index| {
                results.borrow_mut().insert(
                    sequence,
                    SequenceEntry {
                        index: index.clone(),
                        value: None,
                    },
                );
                ScanEvaluation::ReadData
            },
            key_reader: |sequence, _index, value| {
                results
                    .borrow_mut()
                    .get_mut(&sequence)
                    .expect("reader can't be invoked without evaluator")
                    .value = Some(value);
                Ok(())
            },
        }
        .get(&mut self.file)?;
        Ok(results.into_inner())
    }
}

// /// A compaction process that runs in concert with a transaction manager.
// pub struct TransactableCompaction<'a, Manager: FileManager> {
//     /// The name of the tree being compacted.
//     pub name: &'a str,
//     /// The transaction manager.
//     pub manager: &'a TransactionManager<Manager>,
// }

// struct TreeCompactor<'a, Root: root::Root, Manager: FileManager> {
//     manager: &'a Manager,
//     state: &'a State<Root>,
//     vault: Option<&'a dyn AnyVault>,
//     transactions: Option<TransactableCompaction<'a, Manager>>,
//     scratch: &'a mut Vec<u8>,
// }

// impl<'a, Root, Manager> TreeCompactor<'a, Root, Manager>
// where
//     Root: root::Root,
//     Manager: FileManager,
// {
//     fn compact(
//         self,
//         file: &mut dyn BlobStorage,
//     ) -> Result<(Manager::File, TreeCompactionFinisher<'a, Root, Manager>), Error> {
//         let current_path = file.id().path();
//         let file_name = current_path
//             .file_name()
//             .ok_or_else(|| ErrorKind::message("could not retrieve file name"))?;
//         let mut compacted_name = file_name.to_os_string();
//         compacted_name.push(".compacting");
//         let compacted_path = current_path
//             .parent()
//             .ok_or_else(|| ErrorKind::message("couldn't access parent of file"))?
//             .join(compacted_name);

//         if compacted_path.exists() {
//             std::fs::remove_file(&compacted_path)?;
//         }

//         let transaction = self.transactions.as_ref().map(|transactions| {
//             transactions
//                 .manager
//                 .new_transaction([transactions.name.as_bytes()])
//         });
//         let mut new_file = self.manager.open_for_append(&compacted_path)?;
//         let mut writer = PagedWriter::new(None, &mut new_file, self.vault, None, 0)?;

//         // Use the read state to list all the currently live chunks
//         let mut copied_chunks = HashMap::new();
//         let read_state = self.state.read();
//         let mut temporary_header = read_state.root.clone();
//         drop(read_state);
//         temporary_header.copy_data_to(false, file, &mut copied_chunks, &mut writer, self.vault)?;

//         // Now, do the same with the write state, which should be very fast,
//         // since only nodes that have changed will need to be visited.
//         let mut write_state = self.state.lock();
//         write_state
//             .root
//             .copy_data_to(true, file, &mut copied_chunks, &mut writer, self.vault)?;

//         let file = save_tree(&mut write_state, self.vault, None, writer, self.scratch)?;
//         file.synchronize()?;

//         // Because the file path now refers to a new file handle, some operating
//         // systems such as Linux and Mac OS require synchronizing the metadata
//         // of the containing directory.
//         if let Some(parent) = compacted_path.parent() {
//             self.manager.synchronize(parent)?;
//         }

//         let read_state = self.state.lock_read();

//         Ok((
//             new_file,
//             TreeCompactionFinisher {
//                 write_state,
//                 read_state,
//                 _transaction: transaction,
//             },
//         ))
//     }
// }

// struct TreeCompactionFinisher<'a, Root: root::Root, Manager: FileManager> {
//     write_state: MutexGuard<'a, ActiveState<Root>>,
//     read_state: RwLockWriteGuard<'a, Arc<ActiveState<Root>>>,
//     _transaction: Option<ManagedTransaction<Manager>>,
// }

// impl<'a, Root: root::Root, Manager: FileManager> TreeCompactionFinisher<'a, Root, Manager> {
//     fn finish(mut self, new_file_id: u64) {
//         self.write_state.file_id = Some(new_file_id);
//         *self.read_state = Arc::new(self.write_state.clone());
//         drop(self);
//     }
// }

struct TreeWriter<'a, Root: root::Root> {
    state: &'a State<Root>,
    vault: Option<&'a dyn AnyVault>,
    cache: Option<&'a ChunkCache>,
    scratch: &'a mut Vec<u8>,
}

impl<'a, Root> TreeWriter<'a, Root>
where
    Root: root::Root,
{
    fn commit<Manager: io::FileManager>(
        self,
        mut file: SedimentFile<Manager>,
    ) -> Result<Option<CommittedTreeState<Manager>>, Error> {
        let mut active_state = self.state.lock();
        let path_id = file.unique_id();
        if active_state.file_id != Some(path_id.id) {
            return Err(Error::from(ErrorKind::TreeCompacted));
        }
        if active_state.root.dirty() {
            let data_block = PagedWriter::new(&mut file, self.vault, self.cache);

            self.scratch.clear();
            save_tree(&mut *active_state, self.vault, data_block, self.scratch)?;
            Ok(Some(CommittedTreeState {
                path_id,
                state: Box::new(self.state.clone()),
                committed: CommitStateGuard(Box::new(active_state.clone())),
                session_to_commit: file.enqueue_commit()?,
            }))
        } else {
            Ok(None)
        }
    }
}

/// The state of an updated tree that has been written to disk but has not been
/// synchronized with the transaction log yet.
pub struct CommittedTreeState<Manager: io::FileManager> {
    pub(crate) path_id: PathId,
    pub(crate) state: Box<dyn AnyTreeState>,
    pub(crate) committed: CommitStateGuard,
    pub(crate) session_to_commit: Option<PendingCommit<Manager>>,
}

struct TreeModifier<'a, 'm, Root: root::Root> {
    state: &'a State<Root>,
    vault: Option<&'a dyn AnyVault>,
    cache: Option<&'a ChunkCache>,
    modification: Option<Modification<'m, Root::Value, Root::Index>>,
    scratch: &'a mut Vec<u8>,
}

impl<'a, 'm, Root> TreeModifier<'a, 'm, Root>
where
    Root: root::Root,
{
    fn modify(
        mut self,
        file: &mut dyn BlobStorage,
    ) -> Result<Vec<ModificationResult<Root::Index>>, Error> {
        let mut active_state = self.state.lock();
        if active_state.file_id != Some(file.unique_id().id) {
            return Err(Error::from(ErrorKind::TreeCompacted));
        }

        let mut data_block = PagedWriter::new(file, self.vault, self.cache);

        let modification = self.modification.take().unwrap();
        let persistence_mode = modification.persistence_mode;
        let is_transactional = persistence_mode.transaction_id().is_some();
        let max_order = active_state.max_order;

        // Execute the modification
        let results = active_state
            .root
            .modify(modification, &mut data_block, max_order)?;

        if is_transactional {
            // Transactions will written to disk later.
            data_block.finish();
        } else {
            // Save the tree to disk immediately.
            self.scratch.clear();
            let file = save_tree(&mut *active_state, self.vault, data_block, self.scratch)?;
            let guard = file.sync()?;
            active_state.publish(guard, self.state);
        }

        Ok(results)
    }
}

#[allow(clippy::shadow_unrelated)] // It is related, but clippy can't tell.
fn save_tree<'writer, Root: root::Root>(
    active_state: &mut ActiveState<Root>,
    vault: Option<&dyn AnyVault>,
    mut data_block: PagedWriter<'writer, '_>,
    scratch: &mut Vec<u8>,
) -> Result<&'writer mut dyn BlobStorage, Error> {
    scratch.clear();
    active_state.root.serialize(&mut data_block, scratch)?;
    let file = data_block.finish();

    if let Some(vault) = vault {
        *scratch = vault.encrypt(scratch)?;
    }
    scratch.splice(
        ..0,
        b"Nbr".iter().copied().chain(iter::once(Root::HEADER as u8)),
    );

    file.write_header(scratch)?;

    Ok(file)
}

/// One or more keys.
#[derive(Debug)]
pub struct KeyRange<I: Iterator<Item = Bytes>, Bytes: AsRef<[u8]>> {
    remaining_keys: I,
    current_key: Option<Bytes>,
    _bytes: PhantomData<Bytes>,
}

impl<I: Iterator<Item = Bytes>, Bytes: AsRef<[u8]>> KeyRange<I, Bytes> {
    /// Returns a new instance from the keys provided.
    pub fn new(mut keys: I) -> Self {
        Self {
            current_key: keys.next(),
            remaining_keys: keys,
            _bytes: PhantomData,
        }
    }

    fn current_key(&self) -> Option<&[u8]> {
        self.current_key.as_ref().map(Bytes::as_ref)
    }
}

impl<I: Iterator<Item = Bytes>, Bytes: AsRef<[u8]>> Iterator for KeyRange<I, Bytes> {
    type Item = Bytes;
    fn next(&mut self) -> Option<Bytes> {
        let mut key = self.remaining_keys.next();
        std::mem::swap(&mut key, &mut self.current_key);
        key
    }
}

#[derive(Clone, Copy)]
/// The result of evaluating a key or node that was scanned.
pub enum ScanEvaluation {
    /// Read the data for this entry.
    ReadData,
    /// Skip this entry's contained data.
    Skip,
    /// Stop scanning.
    Stop,
}

struct TreeGetter<
    'a,
    'keys,
    Root: root::Root,
    KeyEvaluator: FnMut(&ArcBytes<'static>, &Root::Index) -> ScanEvaluation,
    KeyReader: FnMut(ArcBytes<'static>, Root::Value, Root::Index) -> Result<(), Error>,
    Keys: Iterator<Item = &'keys [u8]>,
> {
    from_transaction: bool,
    state: &'a State<Root>,
    vault: Option<&'a dyn AnyVault>,
    cache: Option<&'a ChunkCache>,
    keys: Keys,
    key_evaluator: KeyEvaluator,
    key_reader: KeyReader,
}

impl<'a, 'keys, KeyEvaluator, KeyReader, Root, Keys>
    TreeGetter<'a, 'keys, Root, KeyEvaluator, KeyReader, Keys>
where
    KeyEvaluator: FnMut(&ArcBytes<'static>, &Root::Index) -> ScanEvaluation,
    KeyReader: FnMut(ArcBytes<'static>, Root::Value, Root::Index) -> Result<(), Error>,
    Keys: Iterator<Item = &'keys [u8]>,
    Root: root::Root,
{
    fn get(mut self, file: &mut dyn BlobStorage) -> Result<(), Error> {
        if self.from_transaction {
            let state = self.state.lock();
            if state.file_id != Some(file.unique_id().id) {
                return Err(Error::from(ErrorKind::TreeCompacted));
            }

            state.root.get_multiple(
                &mut self.keys,
                &mut self.key_evaluator,
                &mut self.key_reader,
                file,
                self.vault,
                self.cache,
            )
        } else {
            let state = self.state.read();
            if state.file_id != Some(file.unique_id().id) {
                return Err(Error::from(ErrorKind::TreeCompacted));
            }

            state.root.get_multiple(
                &mut self.keys,
                &mut self.key_evaluator,
                &mut self.key_reader,
                file,
                self.vault,
                self.cache,
            )
        }
    }
}

struct TreeScanner<
    'a,
    'keys,
    CallerError,
    Root: root::Root,
    NodeEvaluator,
    KeyEvaluator,
    KeyReader,
    KeyRangeBounds,
> where
    NodeEvaluator: FnMut(&ArcBytes<'static>, &Root::ReducedIndex, usize) -> ScanEvaluation,
    KeyEvaluator: FnMut(&ArcBytes<'static>, &Root::Index) -> ScanEvaluation,
    KeyReader:
        FnMut(ArcBytes<'static>, &Root::Index, Root::Value) -> Result<(), AbortError<CallerError>>,
    KeyRangeBounds: RangeBounds<&'keys [u8]> + Debug + ?Sized,
    CallerError: Display + Debug,
{
    forwards: bool,
    from_transaction: bool,
    state: &'a State<Root>,
    vault: Option<&'a dyn AnyVault>,
    cache: Option<&'a ChunkCache>,
    range: &'keys KeyRangeBounds,
    node_evaluator: NodeEvaluator,
    key_evaluator: KeyEvaluator,
    key_reader: KeyReader,
    _phantom: PhantomData<&'keys [u8]>,
}

impl<
        'a,
        'keys,
        CallerError,
        Root: root::Root,
        NodeEvaluator,
        KeyEvaluator,
        KeyReader,
        KeyRangeBounds,
    >
    TreeScanner<
        'a,
        'keys,
        CallerError,
        Root,
        NodeEvaluator,
        KeyEvaluator,
        KeyReader,
        KeyRangeBounds,
    >
where
    NodeEvaluator: FnMut(&ArcBytes<'static>, &Root::ReducedIndex, usize) -> ScanEvaluation,
    KeyEvaluator: FnMut(&ArcBytes<'static>, &Root::Index) -> ScanEvaluation,
    KeyReader:
        FnMut(ArcBytes<'static>, &Root::Index, Root::Value) -> Result<(), AbortError<CallerError>>,
    KeyRangeBounds: RangeBounds<&'keys [u8]> + Debug + ?Sized,
    CallerError: Display + Debug,
{
    fn scan(mut self, file: &mut dyn BlobStorage) -> Result<bool, AbortError<CallerError>> {
        if self.from_transaction {
            let state = self.state.lock();
            if state.file_id != Some(file.unique_id().id) {
                return Err(AbortError::Nebari(Error::from(ErrorKind::TreeCompacted)));
            }

            state.root.scan(
                self.range,
                ScanArgs::new(
                    self.forwards,
                    &mut self.node_evaluator,
                    &mut self.key_evaluator,
                    &mut self.key_reader,
                ),
                file,
                self.vault,
                self.cache,
            )
        } else {
            let state = self.state.read();
            if state.file_id != Some(file.unique_id().id) {
                return Err(AbortError::Nebari(Error::from(ErrorKind::TreeCompacted)));
            }

            state.root.scan(
                self.range,
                ScanArgs::new(
                    self.forwards,
                    &mut self.node_evaluator,
                    &mut self.key_evaluator,
                    &mut self.key_reader,
                ),
                file,
                self.vault,
                self.cache,
            )
        }
    }
}

struct TreeSequenceGetter<
    'a,
    Index: EmbeddedIndex<ArcBytes<'static>> + Clone + Debug + 'static,
    KeyEvaluator: for<'k> FnMut(SequenceId, &'k BySequenceIndex<Index>) -> ScanEvaluation,
    KeyReader: FnMut(SequenceId, BySequenceIndex<Index>, ArcBytes<'static>) -> Result<(), Error>,
    Keys: Iterator<Item = SequenceId>,
> {
    from_transaction: bool,
    state: &'a State<VersionedTreeRoot<Index>>,
    vault: Option<&'a dyn AnyVault>,
    cache: Option<&'a ChunkCache>,
    keys: Keys,
    key_evaluator: KeyEvaluator,
    key_reader: KeyReader,
}

impl<'a, KeyEvaluator, KeyReader, Index, Keys>
    TreeSequenceGetter<'a, Index, KeyEvaluator, KeyReader, Keys>
where
    KeyEvaluator: for<'k> FnMut(SequenceId, &'k BySequenceIndex<Index>) -> ScanEvaluation,
    KeyReader: FnMut(SequenceId, BySequenceIndex<Index>, ArcBytes<'static>) -> Result<(), Error>,
    Keys: Iterator<Item = SequenceId>,
    Index: EmbeddedIndex<ArcBytes<'static>> + Clone + Debug + 'static,
{
    fn get(mut self, file: &mut dyn BlobStorage) -> Result<(), Error> {
        if self.from_transaction {
            let state = self.state.lock();
            if state.file_id != Some(file.unique_id().id) {
                return Err(Error::from(ErrorKind::TreeCompacted));
            }

            state.root.by_sequence_root.get_multiple(
                &mut self
                    .keys
                    .into_iter()
                    .map(|sequence| sequence.0.to_be_bytes()),
                |key, index| {
                    (self.key_evaluator)(SequenceId::try_from(key.as_slice()).unwrap(), index)
                },
                |key, value, index| {
                    (self.key_reader)(SequenceId::try_from(key.as_slice()).unwrap(), index, value)
                },
                file,
                self.vault,
                self.cache,
            )
        } else {
            let state = self.state.read();
            if state.file_id != Some(file.unique_id().id) {
                return Err(Error::from(ErrorKind::TreeCompacted));
            }

            state.root.by_sequence_root.get_multiple(
                &mut self
                    .keys
                    .into_iter()
                    .map(|sequence| sequence.0.to_be_bytes()),
                |key, index| {
                    (self.key_evaluator)(SequenceId::try_from(key.as_slice()).unwrap(), index)
                },
                |key, value, index| {
                    (self.key_reader)(SequenceId::try_from(key.as_slice()).unwrap(), index, value)
                },
                file,
                self.vault,
                self.cache,
            )
        }
    }
}

struct TreeSequenceScanner<
    'a,
    'keys,
    KeyEvaluator,
    KeyRangeBounds,
    DataCallback,
    CallerError,
    Index,
> where
    KeyEvaluator: FnMut(&ArcBytes<'static>, &BySequenceIndex<Index>) -> ScanEvaluation,
    KeyRangeBounds: RangeBounds<&'keys [u8]> + ?Sized,
    DataCallback:
        FnMut(KeySequence<Index>, ArcBytes<'static>) -> Result<(), AbortError<CallerError>>,
    CallerError: Display + Debug,
    Index: EmbeddedIndex<ArcBytes<'static>> + Clone + Debug + 'static,
{
    forwards: bool,
    from_transaction: bool,
    state: &'a State<VersionedTreeRoot<Index>>,
    vault: Option<&'a dyn AnyVault>,
    cache: Option<&'a ChunkCache>,
    range: &'keys KeyRangeBounds,
    key_evaluator: KeyEvaluator,
    data_callback: DataCallback,
}

impl<'a, 'keys, KeyEvaluator, KeyRangeBounds, DataCallback, CallerError, Index>
    TreeSequenceScanner<'a, 'keys, KeyEvaluator, KeyRangeBounds, DataCallback, CallerError, Index>
where
    KeyEvaluator: FnMut(&ArcBytes<'static>, &BySequenceIndex<Index>) -> ScanEvaluation,
    KeyRangeBounds: RangeBounds<&'keys [u8]> + Debug + ?Sized,
    DataCallback:
        FnMut(KeySequence<Index>, ArcBytes<'static>) -> Result<(), AbortError<CallerError>>,
    CallerError: Display + Debug,
    Index: EmbeddedIndex<ArcBytes<'static>> + Clone + Debug + 'static,
{
    fn scan(self, file: &mut dyn BlobStorage) -> Result<(), AbortError<CallerError>> {
        let Self {
            forwards,
            from_transaction,
            state,
            vault,
            cache,
            range,
            mut key_evaluator,
            mut data_callback,
            ..
        } = self;
        let mapped_data_callback =
            |key: ArcBytes<'static>, index: &BySequenceIndex<Index>, data: ArcBytes<'static>| {
                let sequence = SequenceId(BigEndian::read_u64(&key));
                (data_callback)(
                    KeySequence {
                        key: index.key.clone(),
                        sequence,
                        last_sequence: index.last_sequence,
                        embedded: index.embedded.clone(),
                    },
                    data,
                )
            };
        if from_transaction {
            let state = state.lock();
            if state.file_id != Some(file.unique_id().id) {
                return Err(AbortError::Nebari(Error::from(ErrorKind::TreeCompacted)));
            }

            state
                .root
                .by_sequence_root
                .scan(
                    range,
                    &mut ScanArgs::new(
                        forwards,
                        |_, _, _| ScanEvaluation::ReadData,
                        &mut key_evaluator,
                        mapped_data_callback,
                    ),
                    file,
                    vault,
                    cache,
                    0,
                )
                .map(|_| {})
        } else {
            let state = state.read();
            if state.file_id != Some(file.unique_id().id) {
                return Err(AbortError::Nebari(Error::from(ErrorKind::TreeCompacted)));
            }

            state
                .root
                .by_sequence_root
                .scan(
                    range,
                    &mut ScanArgs::new(
                        forwards,
                        |_, _, _| ScanEvaluation::ReadData,
                        &mut key_evaluator,
                        mapped_data_callback,
                    ),
                    file,
                    vault,
                    cache,
                    0,
                )
                .map(|_| {})
        }
    }
}

/// Writes data in pages, allowing for quick scanning through the file.
pub struct PagedWriter<'file, 'a> {
    file: &'file mut dyn BlobStorage,
    vault: Option<&'a dyn AnyVault>,
    cache: Option<&'a ChunkCache>,
}

impl<'file, 'a> Deref for PagedWriter<'file, 'a> {
    type Target = &'file mut dyn BlobStorage;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

impl<'file, 'a> DerefMut for PagedWriter<'file, 'a> {
    #[allow(clippy::mut_mut)] // Forced because of deref_mut
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.file
    }
}

impl<'file, 'a> PagedWriter<'file, 'a> {
    fn new(
        file: &'file mut dyn BlobStorage,
        vault: Option<&'a dyn AnyVault>,
        cache: Option<&'a ChunkCache>,
    ) -> Self {
        Self { file, vault, cache }
    }

    // fn commit_if_needed(&mut self) -> Result<(), Error> {
    //     if self.offset > 0 {
    //         self.commit()?;
    //     }
    //     Ok(())
    // }

    // fn commit(&mut self) -> Result<(), Error> {
    //     self.file.write_all(&self.buffered_write[0..self.offset])?;
    //     self.position += self.offset as u64;
    //     self.offset = 0;
    //     Ok(())
    // }

    /// Writes a chunk of data to the file, after possibly encrypting it.
    /// Returns the position that this chunk can be read from in the file.
    #[allow(clippy::cast_possible_truncation)]
    pub fn write_chunk(&mut self, contents: &[u8]) -> Result<GrainId, Error> {
        let possibly_encrypted = self
            .vault
            .as_ref()
            .map_or_else(|| Ok(contents.to_vec()), |vault| vault.encrypt(contents))?;
        // let length =
        //     u32::try_from(possibly_encrypted.len()).map_err(|_| ErrorKind::ValueTooLarge)?;
        // let crc = CRC32.checksum(&possibly_encrypted);

        let position = self.file.write_async(possibly_encrypted)?;
        // self.write_u32::<BigEndian>(length)?;
        // self.write_u32::<BigEndian>(crc)?;
        // self.write(&possibly_encrypted)?;

        Ok(position)
    }

    /// Writes a chunk of data to the file, after possibly encrypting it.
    /// Returns the position that this chunk can be read from in the file.
    #[allow(clippy::cast_possible_truncation)]
    pub fn write_chunk_cached(&mut self, contents: ArcBytes<'static>) -> Result<GrainId, Error> {
        let position = self.write_chunk(&contents)?;

        if let Some(cache) = self.cache {
            cache.insert(self.file.unique_id().id, position, contents);
        }

        Ok(position)
    }

    /// Reads a "chunk" of data located at `position`. `position` should be a
    /// location previously returned by [`Self::write_chunk()`].
    pub fn read_chunk(&mut self, position: GrainId) -> Result<CacheEntry, Error> {
        read_chunk(position, false, self.file, self.vault, self.cache)
    }

    /// Copies a chunk from `original_position` in file `from_file` to this
    /// file. This function will update `copied_chunks` with the newly written
    /// location and return the new position. If `copied_chunks` already
    /// contains `original_position`, the already copied position is returned.
    pub fn copy_chunk_from<Hasher: BuildHasher>(
        &mut self,
        original_position: GrainId,
        from_file: &mut dyn BlobStorage,
        copied_chunks: &mut std::collections::HashMap<GrainId, GrainId, Hasher>,
        vault: Option<&dyn AnyVault>,
    ) -> Result<GrainId, Error> {
        if original_position.as_u64() == 0 {
            Ok(GrainId::from(0))
        } else if let Some(new_position) = copied_chunks.get(&original_position) {
            Ok(*new_position)
        } else {
            // Since these are one-time copies, and receiving a Decoded entry
            // makes things tricky, we're going to not use caching for reads
            // here. This gives the added benefit for a long-running server to
            // ensure it's doing CRC checks occasionally as it copies itself.
            let chunk = match read_chunk(original_position, true, from_file, vault, None)? {
                CacheEntry::ArcBytes(buffer) => buffer,
                CacheEntry::Decoded(_) => unreachable!(),
            };
            let new_location = self.write_chunk(&chunk)?;
            copied_chunks.insert(original_position, new_location);
            Ok(new_location)
        }
    }

    fn finish(mut self) -> &'file mut dyn BlobStorage {
        self.file
    }
}

#[allow(clippy::cast_possible_truncation)]
#[cfg_attr(feature = "tracing", tracing::instrument(skip(file, vault, cache)))]
fn read_chunk(
    position: GrainId,
    validate_crc: bool,
    file: &mut dyn BlobStorage,
    vault: Option<&dyn AnyVault>,
    cache: Option<&ChunkCache>,
) -> Result<CacheEntry, Error> {
    if let Some(cache) = cache {
        if let Some(entry) = cache.get(file.unique_id().id, position) {
            return Ok(entry);
        }
    }

    let data = file.read(position)?;

    let decrypted = match vault {
        Some(vault) => ArcBytes::from(vault.decrypt(&data)?),
        None => data,
    };

    if let Some(cache) = cache {
        cache.insert(file.unique_id().id, position, decrypted.clone());
    }

    Ok(CacheEntry::ArcBytes(decrypted))
}

/// Returns a value for the "order" (maximum children per node) value for the
/// database. This function is meant to keep the tree shallow while still
/// keeping the nodes smaller along the way. This is an approximation that
/// always returns an order larger than what is needed, but will never return a
/// value larger than `MAX_ORDER`.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
#[must_use]
pub fn dynamic_order(number_of_records: u64, max_order: Option<usize>) -> usize {
    // Current approximation is the 3rd root.
    let max_order = max_order.unwrap_or(DEFAULT_MAX_ORDER);
    if number_of_records > max_order.pow(3) as u64 {
        max_order
    } else {
        let estimated_order = 4.max((number_of_records as f64).cbrt() as usize);
        max_order.min(estimated_order)
    }
}

/// A range of u64 values that is able to be used as keys in a tree scan, once
/// [borrowed](BorrowByteRange::borrow_as_bytes()).
#[derive(Debug)]
pub struct U64Range {
    start_bound: Bound<u64>,
    start_bound_bytes: Bound<[u8; 8]>,
    end_bound: Bound<u64>,
    end_bound_bytes: Bound<[u8; 8]>,
}

impl RangeBounds<u64> for U64Range {
    fn start_bound(&self) -> Bound<&u64> {
        match &self.start_bound {
            Bound::Included(value) => Bound::Included(value),
            Bound::Excluded(value) => Bound::Excluded(value),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    fn end_bound(&self) -> Bound<&u64> {
        match &self.end_bound {
            Bound::Included(value) => Bound::Included(value),
            Bound::Excluded(value) => Bound::Excluded(value),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

/// A borrowed range in byte form.
#[derive(Debug, Clone)]
pub struct BorrowedRange<'a> {
    /// The start bound for this range.
    pub start: Bound<&'a [u8]>,
    /// The end bound for this range.
    pub end: Bound<&'a [u8]>,
}

/// Borrows a range.
pub trait BorrowByteRange<'a> {
    /// Returns a borrowed version of byte representation the original range.
    fn borrow_as_bytes(&'a self) -> BorrowedRange<'a>;
}

impl<'a> BorrowByteRange<'a> for Range<Vec<u8>> {
    fn borrow_as_bytes(&'a self) -> BorrowedRange<'a> {
        BorrowedRange {
            start: Bound::Included(&self.start[..]),
            end: Bound::Excluded(&self.end[..]),
        }
    }
}

impl<'a> BorrowByteRange<'a> for U64Range {
    fn borrow_as_bytes(&'a self) -> BorrowedRange<'a> {
        BorrowedRange {
            start: match &self.start_bound_bytes {
                Bound::Included(bytes) => Bound::Included(&bytes[..]),
                Bound::Excluded(bytes) => Bound::Excluded(&bytes[..]),
                Bound::Unbounded => Bound::Unbounded,
            },
            end: match &self.end_bound_bytes {
                Bound::Included(bytes) => Bound::Included(&bytes[..]),
                Bound::Excluded(bytes) => Bound::Excluded(&bytes[..]),
                Bound::Unbounded => Bound::Unbounded,
            },
        }
    }
}

impl<'a, 'b: 'a> RangeBounds<&'a [u8]> for BorrowedRange<'b> {
    fn start_bound(&self) -> Bound<&&'a [u8]> {
        match &self.start {
            Bound::Included(value) => Bound::Included(value),
            Bound::Excluded(value) => Bound::Excluded(value),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    fn end_bound(&self) -> Bound<&&'a [u8]> {
        match &self.end {
            Bound::Included(value) => Bound::Included(value),
            Bound::Excluded(value) => Bound::Excluded(value),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

impl U64Range {
    /// Creates a new instance from the range passed in.
    pub fn new<B: RangeBounds<T>, T: Clone + Into<u64>>(bounds: B) -> Self {
        Self {
            start_bound: match bounds.start_bound() {
                Bound::Included(id) => Bound::Included(id.clone().into()),
                Bound::Excluded(id) => Bound::Excluded(id.clone().into()),
                Bound::Unbounded => Bound::Unbounded,
            },
            start_bound_bytes: match bounds.start_bound() {
                Bound::Included(id) => Bound::Included(id.clone().into().to_be_bytes()),
                Bound::Excluded(id) => Bound::Excluded(id.clone().into().to_be_bytes()),
                Bound::Unbounded => Bound::Unbounded,
            },
            end_bound: match bounds.end_bound() {
                Bound::Included(id) => Bound::Included(id.clone().into()),
                Bound::Excluded(id) => Bound::Excluded(id.clone().into()),
                Bound::Unbounded => Bound::Unbounded,
            },
            end_bound_bytes: match bounds.end_bound() {
                Bound::Included(id) => Bound::Included(id.clone().into().to_be_bytes()),
                Bound::Excluded(id) => Bound::Excluded(id.clone().into().to_be_bytes()),
                Bound::Unbounded => Bound::Unbounded,
            },
        }
    }
}

/// The key and value of an entry..
#[derive(Eq, PartialEq, Clone, Debug, Default)]
pub struct KeyValue<Key, Value> {
    /// The key of this entry.
    pub key: Key,
    /// The value of this entry.
    pub value: Value,
}

/// The key and index of an entry.
#[derive(Eq, PartialEq, Clone, Debug, Default)]
pub struct KeyIndex<Index> {
    /// The key of this entry.
    pub key: ArcBytes<'static>,
    /// The index of this entry.
    pub index: Index,
}

/// A key and index of an entry from a tree with [`Root`] `R`.
pub type TreeKeyIndex<R> = KeyIndex<<R as Root>::Index>;

/// The value and index of an entry.
#[derive(Eq, PartialEq, Clone, Debug, Default)]
pub struct ValueIndex<Value, Index> {
    /// The value of this entry.
    pub value: Value,
    /// The index of this entry.
    pub index: Index,
}

/// A value and index of an entry from a tree with [`Root`] `R`.
pub type TreeValueIndex<R> = ValueIndex<<R as Root>::Value, <R as Root>::Index>;

/// A complete entry in a tree.
#[derive(Eq, PartialEq, Clone, Debug, Default)]
pub struct Entry<Value, Index> {
    /// The key of this entry.
    pub key: ArcBytes<'static>,
    /// The value of this entry.
    pub value: Value,
    /// The index of this entry.
    pub index: Index,
}

/// An entry from a tree with [`Root`] `R`.
pub type TreeEntry<R> = Entry<<R as Root>::Value, <R as Root>::Index>;

/// An index that is embeddable within a tree.
///
/// An index is a computed value that is stored directly within the B-Tree
/// structure. Because these are encoded directly onto the nodes, they should be
/// kept shorter for better performance.
pub trait EmbeddedIndex<Value>: Serializable + Clone + Debug + Send + Sync + 'static {
    /// The reduced representation of this index.
    type Reduced: Serializable + Clone + Debug + Send + Sync + 'static;
    /// The reducer that reduces arrays of `Self` or `Self::Reduced` into `Self::Reduced`.
    type Indexer: Indexer<Value, Self> + Reducer<Self, Self::Reduced>;
}

/// A type that can be serialized and deserialized.
pub trait Serializable: Send + Sync + Sized + 'static {
    /// Serializes into `writer` and returns the number of bytes written.
    fn serialize_to<W: WriteBytesExt>(&self, writer: &mut W) -> Result<usize, Error>;
    /// Deserializes from `reader`, and returns the deserialized instance.
    /// Implementors should not expect for the reader to be fully consumed at
    /// the end of this call.
    fn deserialize_from<R: ReadBytesExt>(reader: &mut R) -> Result<Self, Error>;
}

impl<Value> EmbeddedIndex<Value> for () {
    type Reduced = Self;
    type Indexer = Self;
}

impl<Value> Indexer<Value, ()> for () {
    fn index(&self, _key: &ArcBytes<'_>, _value: Option<&Value>) -> Self {}
}

impl Serializable for () {
    fn serialize_to<W: WriteBytesExt>(&self, _writer: &mut W) -> Result<usize, Error> {
        Ok(0)
    }

    fn deserialize_from<R: ReadBytesExt>(_reader: &mut R) -> Result<Self, Error> {
        Ok(())
    }
}

/// A single key's modification result.
pub struct ModificationResult<Index> {
    /// The key that was changed.
    pub key: ArcBytes<'static>,
    /// The updated index, if the key is still present.
    pub index: Option<Index>,
}

/// The result of a change to a [`BTreeNode`].
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum ChangeResult {
    /// No changes were made.
    Unchanged,
    /// The node modified is now empty and should be removed.
    Remove,
    /// The node modified is now has fewer entries than the tree should have,
    /// and its children should be absorbed into neighbors.
    Absorb,
    /// The node was changed.
    Changed,
    /// The node modified is now has more entries than the tree should have, and
    /// it should be split.
    Split,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashSet},
        path::Path,
    };

    use nanorand::{Pcg64, Rng};
    use sediment::io::{
        any::AnyFileManager, fs::StdFileManager, memory::MemoryFileManager, FileManager,
    };

    use super::*;
    use crate::storage::sediment::SedimentFile;

    fn insert_one_record<R: Root<Value = ArcBytes<'static>> + Default, M: io::FileManager>(
        context: &Context<M>,
        file_path: &Path,
        ids: &mut HashSet<u64>,
        rng: &mut Pcg64,
        max_order: Option<usize>,
    ) {
        let id = loop {
            let id = rng.generate::<u64>();
            if ids.insert(id) {
                break id;
            }
        };
        let id_buffer = ArcBytes::from(id.to_be_bytes().to_vec());
        {
            let mut tree = TreeFile::<R, M>::open(
                file_path,
                State::new(None, max_order, R::default()),
                context,
                None,
            )
            .unwrap();
            tree.set(None, id_buffer.clone(), ArcBytes::from(b"hello world"))
                .unwrap();

            // This shouldn't have to scan the file, as the data fits in memory.
            let value = tree.get(&id_buffer, false).unwrap();
            assert_eq!(&*value.unwrap(), b"hello world");
        }

        // Try loading the file up and retrieving the data.
        {
            let mut tree = TreeFile::<R, M>::open(
                file_path,
                State::new(None, max_order, R::default()),
                context,
                None,
            )
            .unwrap();
            let value = tree.get(&id_buffer, false).unwrap();
            assert_eq!(&*value.unwrap(), b"hello world");
        }
    }

    fn remove_one_record<R: Root<Value = ArcBytes<'static>> + Default, F: io::FileManager>(
        context: &Context<F>,
        file_path: &Path,
        id: u64,
        max_order: Option<usize>,
    ) {
        let id_buffer = ArcBytes::from(id.to_be_bytes().to_vec());
        {
            let mut file =
                SedimentFile::open(file_path, true, context.file_manager.clone()).unwrap();
            let state = State::new(None, max_order, R::default());
            TreeFile::<R, F>::initialize_state(&mut file, &state, context.vault(), None).unwrap();
            let mut tree =
                TreeFile::<R, F>::new(file, state, context.vault.clone(), context.cache.clone())
                    .unwrap();
            tree.modify(Modification {
                persistence_mode: PersistenceMode::Sync,
                keys: vec![id_buffer.clone()],
                operation: Operation::Remove,
            })
            .unwrap();

            // The row should no longer exist in memory.
            let value = tree.get(&id_buffer, false).unwrap();
            assert_eq!(value, None);
        }

        // Try loading the file up and retrieving the data.
        {
            let mut file =
                SedimentFile::open(file_path, true, context.file_manager.clone()).unwrap();
            let state = State::default();
            TreeFile::<R, F>::initialize_state(&mut file, &state, context.vault(), None).unwrap();

            let mut tree =
                TreeFile::<R, F>::new(file, state, context.vault.clone(), context.cache.clone())
                    .unwrap();
            let value = tree.get(&id_buffer, false).unwrap();
            assert_eq!(value, None);
        }
    }

    #[test]
    fn simple_inserts() {
        const ORDER: usize = 4;

        let mut rng = Pcg64::new_seed(1);
        let temp_dir = crate::test_util::TestDirectory::new("btree-tests");
        std::fs::create_dir(&temp_dir).unwrap();
        let file_path = temp_dir.join("tree");
        let mut ids = HashSet::new();
        let context = Context {
            file_manager: sediment::io::fs::StdFileManager::default(),
            vault: None,
            cache: None,
        };
        // Insert up to the limit of a LEAF, which is ORDER - 1.
        for _ in 0..ORDER - 1 {
            insert_one_record::<Versioned, sediment::io::fs::StdFileManager>(
                &context,
                &file_path,
                &mut ids,
                &mut rng,
                Some(ORDER),
            );
        }
        println!("Successfully inserted up to ORDER - 1 nodes.");

        // The next record will split the node
        insert_one_record::<Versioned, sediment::io::fs::StdFileManager>(
            &context,
            &file_path,
            &mut ids,
            &mut rng,
            Some(ORDER),
        );
        println!("Successfully introduced one layer of depth.");

        // Insert a lot more.
        for i in 0..1_000 {
            println!("{i}");
            insert_one_record::<Versioned, sediment::io::fs::StdFileManager>(
                &context,
                &file_path,
                &mut ids,
                &mut rng,
                Some(ORDER),
            );
        }
    }

    fn remove<R: Root<Value = ArcBytes<'static>> + Default>(label: &str) {
        const ORDER: usize = 4;

        // We've seen a couple of failures in CI, but have never been able to
        // reproduce locally. There used to be a small bit of randomness that
        // wasn't deterministic in the conversion between a HashSet and a Vec
        // for the IDs. This randomness has been removed, and instead we're now
        // embracing running a randomly seeded test -- and logging the seed that
        // fails so that we can attempt to reproduce it outside of CI.

        let mut seed_rng = Pcg64::new();
        let seed = seed_rng.generate();
        println!("Seeding removal {} with {}", label, seed);
        let mut rng = Pcg64::new_seed(seed);
        let context = Context {
            file_manager: StdFileManager::default(),
            vault: None,
            cache: None,
        };
        let temp_dir = crate::test_util::TestDirectory::new(format!("btree-removals-{}", label));
        std::fs::create_dir(&temp_dir).unwrap();
        let file_path = temp_dir.join("tree");
        let mut ids = HashSet::new();
        for _ in 0..250 {
            insert_one_record::<R, StdFileManager>(
                &context,
                &file_path,
                &mut ids,
                &mut rng,
                Some(ORDER),
            );
        }

        let mut ids = ids.into_iter().collect::<Vec<_>>();
        ids.sort_unstable();
        rng.shuffle(&mut ids);

        // Remove each of the records
        for id in ids {
            remove_one_record::<R, StdFileManager>(&context, &file_path, id, Some(ORDER));
        }

        // Test being able to add a record again
        insert_one_record::<R, StdFileManager>(
            &context,
            &file_path,
            &mut HashSet::default(),
            &mut rng,
            Some(ORDER),
        );
    }

    #[test]
    fn remove_versioned() {
        remove::<Versioned>("versioned");
    }

    #[test]
    fn remove_unversioned() {
        remove::<Unversioned>("unversioned");
    }

    #[test]
    fn spam_insert_std_versioned() {
        spam_insert::<Versioned, StdFileManager>("std-versioned");
    }

    #[test]
    fn spam_insert_std_unversioned() {
        spam_insert::<Unversioned, StdFileManager>("std-unversioned");
    }

    fn spam_insert<R: Root<Value = ArcBytes<'static>> + Default, F: FileManager>(name: &str) {
        const RECORDS: usize = 1_000;
        let mut rng = Pcg64::new_seed(1);
        let ids = (0..RECORDS).map(|_| rng.generate::<u64>());
        let context = Context {
            file_manager: F::default(),
            vault: None,
            cache: Some(ChunkCache::new(100, 160_384)),
        };
        let temp_dir = crate::test_util::TestDirectory::new(format!("spam-inserts-{}", name));
        std::fs::create_dir(&temp_dir).unwrap();
        let file_path = temp_dir.join("tree");
        let state = State::default();
        let mut tree = TreeFile::<R, F>::open(&file_path, state, &context, None).unwrap();
        for (_index, id) in ids.enumerate() {
            let id_buffer = ArcBytes::from(id.to_be_bytes().to_vec());
            tree.set(None, id_buffer.clone(), ArcBytes::from(b"hello world"))
                .unwrap();
        }
    }

    #[test]
    fn std_bulk_insert_versioned() {
        bulk_insert::<Versioned, _>("std-versioned", StdFileManager::default());
    }

    #[test]
    fn memory_bulk_insert_versioned() {
        bulk_insert::<Versioned, _>("memory-versioned", MemoryFileManager::default());
    }

    #[test]
    fn any_bulk_insert_versioned() {
        bulk_insert::<Versioned, _>("any-versioned", AnyFileManager::new_memory());
        bulk_insert::<Versioned, _>("any-versioned", AnyFileManager::new_file());
    }

    #[test]
    fn std_bulk_insert_unversioned() {
        bulk_insert::<Unversioned, _>("std-unversioned", StdFileManager::default());
    }

    #[test]
    fn memory_bulk_insert_unversioned() {
        bulk_insert::<Unversioned, _>("memory-unversioned", MemoryFileManager::default());
    }

    #[test]
    fn any_bulk_insert_unversioned() {
        bulk_insert::<Unversioned, _>("any-unversioned", AnyFileManager::new_memory());
        bulk_insert::<Unversioned, _>("any-unversioned", AnyFileManager::new_file());
    }

    fn bulk_insert<R: Root<Value = ArcBytes<'static>> + Default, M: FileManager>(
        name: &str,
        file_manager: M,
    ) {
        const RECORDS_PER_BATCH: usize = 10;
        const BATCHES: usize = 1000;
        let mut rng = Pcg64::new_seed(1);
        let context = Context {
            file_manager,
            vault: None,
            cache: Some(ChunkCache::new(100, 160_384)),
        };
        let temp_dir = crate::test_util::TestDirectory::new(format!("bulk-inserts-{}", name));
        std::fs::create_dir(&temp_dir).unwrap();
        let file_path = temp_dir.join("tree");
        let state = State::default();
        let mut tree = TreeFile::<R, M>::open(&file_path, state, &context, None).unwrap();
        for _ in 0..BATCHES {
            let mut ids = (0..RECORDS_PER_BATCH)
                .map(|_| rng.generate::<u64>())
                .collect::<Vec<_>>();
            ids.sort_unstable();
            let modification = Modification {
                persistence_mode: PersistenceMode::Sync,
                keys: ids
                    .iter()
                    .map(|id| ArcBytes::from(id.to_be_bytes().to_vec()))
                    .collect(),
                operation: Operation::Set(ArcBytes::from(b"hello world")),
            };
            tree.modify(modification).unwrap();

            // Try five random gets
            for _ in 0..5 {
                let index = rng.generate_range(0..ids.len());
                let id = ArcBytes::from(ids[index].to_be_bytes().to_vec());
                let value = tree.get(&id, false).unwrap();
                assert_eq!(&*value.unwrap(), b"hello world");
            }
        }
    }

    #[test]
    fn batch_get() {
        let context = Context {
            file_manager: MemoryFileManager::default(),
            vault: None,
            cache: None,
        };
        let state = State::default();
        // let file = context.file_manager.append("test").unwrap();
        let mut tree =
            TreeFile::<Versioned, MemoryFileManager>::open("test", state, &context, None).unwrap();
        // Create enough records to go 4 levels deep.
        let mut ids = Vec::new();
        for id in 0..3_u32.pow(4) {
            let id_buffer = ArcBytes::from(id.to_be_bytes().to_vec());
            tree.set(None, id_buffer.clone(), id_buffer.clone())
                .unwrap();
            ids.push(id_buffer);
        }

        // Get them all
        let mut all_records = tree
            .get_multiple(ids.iter().map(ArcBytes::as_slice), false)
            .unwrap();
        // Order isn't guaranteeed.
        all_records.sort();
        assert_eq!(
            all_records
                .iter()
                .map(|kv| kv.1.clone())
                .collect::<Vec<_>>(),
            ids
        );

        // Try some ranges
        let mut unbounded_to_five = tree.get_range(&(..ids[5].as_slice()), false).unwrap();
        unbounded_to_five.sort();
        assert_eq!(&all_records[..5], &unbounded_to_five);
        let mut one_to_ten_unbounded = tree
            .get_range(&(ids[1].as_slice()..ids[10].as_slice()), false)
            .unwrap();
        one_to_ten_unbounded.sort();
        assert_eq!(&all_records[1..10], &one_to_ten_unbounded);
        let mut bounded_upper = tree
            .get_range(&(ids[3].as_slice()..=ids[50].as_slice()), false)
            .unwrap();
        bounded_upper.sort();
        assert_eq!(&all_records[3..=50], &bounded_upper);
        let mut unbounded_upper = tree.get_range(&(ids[60].as_slice()..), false).unwrap();
        unbounded_upper.sort();
        assert_eq!(&all_records[60..], &unbounded_upper);
        let mut all_through_scan = tree.get_range(&(..), false).unwrap();
        all_through_scan.sort();
        assert_eq!(&all_records, &all_through_scan);
    }

    // fn compact<R: Root<Value = ArcBytes<'static>> + Default, M: FileManager>(
    //     label: &str,
    //     file_manager: M,
    // ) {
    //     const ORDER: usize = 4;
    //     let mut rng = Pcg64::new_seed(1);
    //     let context = Context {
    //         file_manager,
    //         vault: None,
    //         cache: None,
    //     };
    //     let temp_dir = crate::test_util::TestDirectory::new(format!("btree-compact-{}", label));
    //     std::fs::create_dir(&temp_dir).unwrap();
    //     let file_path = temp_dir.join("tree");
    //     let mut ids = HashSet::new();
    //     for _ in 0..5 {
    //         insert_one_record::<R, M>(&context, &file_path, &mut ids, &mut rng, Some(ORDER));
    //     }

    //     let mut tree =
    //         TreeFile::<R, M>::open(&file_path, State::default(), &context, None).unwrap();
    //     let pre_compact_size = context.file_manager.file_length(&file_path).unwrap();
    //     tree = tree.compact(&context.file_manager, None).unwrap();
    //     let after_compact_size = context.file_manager.file_length(&file_path).unwrap();
    //     assert!(
    //         after_compact_size < pre_compact_size,
    //         "compact didn't remove any data"
    //     );

    //     // Try fetching all the records to ensure they're still present.
    //     for id in ids {
    //         let id_buffer = ArcBytes::from(id.to_be_bytes().to_vec());
    //         tree.get(&id_buffer, false)
    //             .unwrap()
    //             .expect("no value found");
    //     }
    // }

    // #[test]
    // fn std_compact_versioned() {
    //     compact::<Versioned, _>("std-versioned", StdFileManager::default());
    // }

    // #[test]
    // fn std_compact_unversioned() {
    //     compact::<Unversioned, _>("std-unversioned", StdFileManager::default());
    // }

    // #[test]
    // fn memory_compact_versioned() {
    //     compact::<Versioned, _>("memory-versioned", MemoryFileManager::default());
    // }

    // #[test]
    // fn memory_compact_unversioned() {
    //     compact::<Unversioned, _>("memory-unversioned", MemoryFileManager::default());
    // }

    // #[test]
    // fn any_compact_versioned() {
    //     compact::<Versioned, _>("any-versioned", AnyFileManager::new_file());
    //     compact::<Versioned, _>("any-versioned", AnyFileManager::new_memory());
    // }

    // #[test]
    // fn any_compact_unversioned() {
    //     compact::<Unversioned, _>("any-unversioned", AnyFileManager::new_file());
    //     compact::<Unversioned, _>("any-unversioned", AnyFileManager::new_memory());
    // }

    // #[test]
    // fn revision_history() {
    //     let context = Context {
    //         file_manager: StdFileManager::default(),
    //         vault: None,
    //         cache: None,
    //     };
    //     let state = State::default();
    //     let tempfile = NamedTempFile::new().unwrap();
    //     let mut tree =
    //         TreeFile::<Versioned, StdFile>::write(tempfile.path(), state, &context, None).unwrap();

    //     // Store three versions of the same key.
    //     tree.set(None, ArcBytes::from(b"a"), ArcBytes::from(b"0"))
    //         .unwrap();
    //     tree.set(None, ArcBytes::from(b"a"), ArcBytes::from(b"1"))
    //         .unwrap();
    //     tree.set(None, ArcBytes::from(b"a"), ArcBytes::from(b"2"))
    //         .unwrap();

    //     // Retrieve the sequences
    //     let mut sequences = Vec::new();
    //     tree.scan_sequences::<Infallible, _, _, _>(
    //         ..,
    //         true,
    //         false,
    //         |_| ScanEvaluation::ReadData,
    //         |sequence, value| {
    //             sequences.push((sequence, value));
    //             Ok(())
    //         },
    //     )
    //     .unwrap();
    //     assert_eq!(sequences.len(), 3);
    //     sequences.sort_by(|a, b| a.0.sequence.cmp(&b.0.sequence));
    //     assert!(sequences.iter().all(|s| s.0.key.as_slice() == b"a"));
    //     assert_eq!(sequences[0].0.last_sequence, None);
    //     assert_eq!(sequences[1].0.last_sequence, Some(sequences[0].0.sequence));
    //     assert_eq!(sequences[2].0.last_sequence, Some(sequences[1].0.sequence));

    //     assert_eq!(sequences[0].1, b"0");
    //     assert_eq!(sequences[1].1, b"1");
    //     assert_eq!(sequences[2].1, b"2");
    // }

    // // struct ExtendToPageBoundaryPlus(u64);

    // // impl FileOp<()> for ExtendToPageBoundaryPlus {
    // //     #[allow(clippy::cast_possible_truncation)]
    // //     fn execute(self, file: &mut dyn BlobStorage) {
    // //         let length = file.length().unwrap();
    // //         let bytes = vec![42_u8; (length - (length % PAGE_SIZE as u64) + self.0) as usize];
    // //         file.write_all(&bytes).unwrap();
    // //     }
    // // }

    // // #[test]
    // // fn header_incompatible() {
    // //     let context = Context {
    // //         file_manager: MemoryFileManager::default(),
    // //         vault: None,
    // //         cache: None,
    // //     };
    // //     let temp_dir = crate::test_util::TestDirectory::new("header_incompatible");
    // //     std::fs::create_dir(&temp_dir).unwrap();
    // //     let file_path = temp_dir.join("tree");

    // //     // Write some data using unversioned
    // //     {
    // //         let state = State::default();
    // //         let mut tree =
    // //             TreeFile::<Unversioned, MemoryFile>::write(&file_path, state, &context, None)
    // //                 .unwrap();
    // //         tree.set(
    // //             None,
    // //             ArcBytes::from(b"test"),
    // //             ArcBytes::from(b"hello world"),
    // //         )
    // //         .unwrap();
    // //     }
    // //     // Try reading it as versioned.
    // //     let mut file = context.file_manager.append(&file_path).unwrap();
    // //     file.execute(ExtendToPageBoundaryPlus(3));
    // //     let state = State::default();
    // //     assert!(matches!(
    // //         TreeFile::<Versioned, MemoryFile>::write(&file_path, state, &context, None)
    // //             .unwrap_err()
    // //             .kind,
    // //         ErrorKind::DataIntegrity(_)
    // //     ));
    // // }

    // // #[test]
    // // fn file_length_page_offset_plus_a_little() {
    // //     let context = Context {
    // //         file_manager: MemoryFileManager::default(),
    // //         vault: None,
    // //         cache: None,
    // //     };
    // //     let temp_dir = crate::test_util::TestDirectory::new("page-header-edge-cases");
    // //     std::fs::create_dir(&temp_dir).unwrap();
    // //     let file_path = temp_dir.join("tree");

    // //     // Write some data.
    // //     {
    // //         let state = State::default();
    // //         let mut tree =
    // //             TreeFile::<Unversioned, MemoryFile>::write(&file_path, state, &context, None)
    // //                 .unwrap();
    // //         tree.set(
    // //             None,
    // //             ArcBytes::from(b"test"),
    // //             ArcBytes::from(b"hello world"),
    // //         )
    // //         .unwrap();
    // //     }
    // //     // Test when the file is of a length that is less than 4 bytes longer than a multiple of a PAGE_SIZE.
    // //     let mut file = context.file_manager.append(&file_path).unwrap();
    // //     file.execute(ExtendToPageBoundaryPlus(3));
    // //     let state = State::default();
    // //     let mut tree =
    // //         TreeFile::<Unversioned, MemoryFile>::write(&file_path, state, &context, None).unwrap();

    // //     assert_eq!(tree.get(b"test", false).unwrap().unwrap(), b"hello world");
    // // }

    fn edit_keys<R: Root<Value = ArcBytes<'static>> + Default, M: FileManager>(
        label: &str,
        file_manager: M,
    ) {
        let context = Context {
            file_manager,
            vault: None,
            cache: None,
        };
        let temp_dir = crate::test_util::TestDirectory::new(format!("edit-keys-{}", label));
        std::fs::create_dir(&temp_dir).unwrap();
        let file_path = temp_dir.join("tree");

        let mut tree =
            TreeFile::<R, M>::open(&file_path, State::default(), &context, None).unwrap();
        assert!(matches!(
            tree.compare_and_swap(b"test", Some(&b"won't match"[..]), None, None)
                .unwrap_err(),
            CompareAndSwapError::Conflict(_)
        ));
        tree.compare_and_swap(b"test", None, Some(ArcBytes::from(b"first")), None)
            .unwrap();
        assert!(matches!(
            tree.compare_and_swap(b"test", Some(&b"won't match"[..]), None, None)
                .unwrap_err(),
            CompareAndSwapError::Conflict(_)
        ));
        tree.compare_and_swap(
            b"test",
            Some(&b"first"[..]),
            Some(ArcBytes::from(b"second")),
            None,
        )
        .unwrap();

        let stored = tree.replace(b"test", b"third", None).unwrap().0.unwrap();
        assert_eq!(stored, b"second");

        tree.compare_and_swap(b"test", Some(b"third"), None, None)
            .unwrap();
        assert!(tree.get(b"test", false).unwrap().is_none());
    }

    #[test]
    fn std_edit_keys_versioned() {
        edit_keys::<Versioned, _>("versioned", StdFileManager::default());
    }

    #[test]
    fn std_edit_keys_unversioned() {
        edit_keys::<Unversioned, _>("unversioned", StdFileManager::default());
    }

    #[test]
    fn memory_edit_keys_versioned() {
        edit_keys::<Versioned, _>("versioned", MemoryFileManager::default());
    }

    #[test]
    fn memory_edit_keys_unversioned() {
        edit_keys::<Unversioned, _>("unversioned", MemoryFileManager::default());
    }

    #[test]
    fn any_edit_keys_versioned() {
        edit_keys::<Versioned, _>("any-versioned", AnyFileManager::new_file());
        edit_keys::<Versioned, _>("any-versioned", AnyFileManager::new_memory());
    }

    #[test]
    fn any_edit_keys_unversioned() {
        edit_keys::<Unversioned, _>("any-unversioned", AnyFileManager::new_file());
        edit_keys::<Unversioned, _>("any-unversioned", AnyFileManager::new_memory());
    }

    #[test]
    fn reduce() {
        #[derive(Debug)]
        struct ExcludedStart<'a>(&'a [u8]);

        impl<'a> RangeBounds<&'a [u8]> for ExcludedStart<'a> {
            fn start_bound(&self) -> Bound<&&'a [u8]> {
                Bound::Excluded(&self.0)
            }

            fn end_bound(&self) -> Bound<&&'a [u8]> {
                Bound::Unbounded
            }
        }

        let context = Context {
            file_manager: StdFileManager::default(),
            vault: None,
            cache: None,
        };
        let temp_dir = crate::test_util::TestDirectory::new("reduce");
        std::fs::create_dir(&temp_dir).unwrap();
        let file_path = temp_dir.join("tree");

        let mut tree = TreeFile::<Unversioned, StdFileManager>::open(
            &file_path,
            State::new(None, Some(4), Unversioned::default()),
            &context,
            None,
        )
        .unwrap();
        for i in 0..=u8::MAX {
            let bytes = ArcBytes::from([i]);
            tree.set(None, bytes.clone(), bytes.clone()).unwrap();
        }

        assert_eq!(tree.reduce(&(..), false).unwrap().unwrap().alive_keys, 256);
        assert_eq!(
            tree.reduce(&ExcludedStart(&[0]), false)
                .unwrap()
                .unwrap()
                .alive_keys,
            255
        );
        assert_eq!(
            tree.reduce(&(&[0][..]..&[u8::MAX][..]), false)
                .unwrap()
                .unwrap()
                .alive_keys,
            255
        );
        assert_eq!(
            tree.reduce(&(&[1][..]..=&[100][..]), false)
                .unwrap()
                .unwrap()
                .alive_keys,
            100
        );

        for start in 0..u8::MAX {
            for end in start + 1..=u8::MAX {
                assert_eq!(
                    tree.reduce(&(&[start][..]..&[end][..]), false)
                        .unwrap()
                        .unwrap()
                        .alive_keys,
                    u64::from(end - start)
                );
            }
        }
    }

    fn first_last<R: Root<Value = ArcBytes<'static>> + Default, M: FileManager>(
        label: &str,
        file_manager: M,
    ) {
        let context = Context {
            file_manager,
            vault: None,
            cache: None,
        };
        let temp_dir = crate::test_util::TestDirectory::new(format!("first-last-{}", label));
        std::fs::create_dir(&temp_dir).unwrap();
        let file_path = temp_dir.join("tree");

        let mut tree =
            TreeFile::<R, M>::open(&file_path, State::default(), &context, None).unwrap();
        tree.set(None, ArcBytes::from(b"a"), ArcBytes::from(b"first"))
            .unwrap();
        tree.set(None, ArcBytes::from(b"z"), ArcBytes::from(b"last"))
            .unwrap();

        assert_eq!(tree.first_key(false).unwrap().unwrap(), b"a");
        let (key, value) = tree.first(false).unwrap().unwrap();
        assert_eq!(key, b"a");
        assert_eq!(value, b"first");

        assert_eq!(tree.last_key(false).unwrap().unwrap(), b"z");
        let (key, value) = tree.last(false).unwrap().unwrap();
        assert_eq!(key, b"z");
        assert_eq!(value, b"last");
    }

    #[test]
    fn std_first_last_versioned() {
        first_last::<Versioned, _>("versioned", StdFileManager::default());
    }

    #[test]
    fn std_first_last_unversioned() {
        first_last::<Unversioned, _>("unversioned", StdFileManager::default());
    }

    #[test]
    fn memory_first_last_versioned() {
        first_last::<Versioned, _>("versioned", MemoryFileManager::default());
    }

    #[test]
    fn memory_first_last_unversioned() {
        first_last::<Unversioned, _>("unversioned", MemoryFileManager::default());
    }

    #[test]
    fn any_first_last_versioned() {
        first_last::<Versioned, _>("any-versioned", AnyFileManager::new_file());
        first_last::<Versioned, _>("any-versioned", AnyFileManager::new_memory());
    }

    #[test]
    fn any_first_last_unversioned() {
        first_last::<Unversioned, _>("any-unversioned", AnyFileManager::new_file());
        first_last::<Unversioned, _>("any-unversioned", AnyFileManager::new_memory());
    }

    fn bulk_compare_swaps<R: Root<Value = ArcBytes<'static>> + Default, M: FileManager>(
        label: &str,
        file_manager: M,
    ) {
        const BATCH: usize = 10_000;
        let context = Context {
            file_manager,
            vault: None,
            cache: None,
        };
        let temp_dir = crate::test_util::TestDirectory::new(format!("bulk-swap-{}", label));
        std::fs::create_dir(&temp_dir).unwrap();
        let file_path = temp_dir.join("tree");

        let mut tree =
            TreeFile::<R, M>::open(&file_path, State::default(), &context, None).unwrap();
        let mut rng = Pcg64::new_seed(1);

        let mut database_state = HashMap::new();
        for index in 1..=10 {
            println!("Batch {index}");
            // Generate a series of operations by randomly inserting or deleting
            // keys. Because the keyspace is u32, this first loop will mostly
            // append records.
            let mut batch = Vec::new();
            let mut operated_keys = HashSet::new();
            while batch.len() < BATCH {
                let key = ArcBytes::from(rng.generate::<u32>().to_be_bytes());
                let key_state = database_state.entry(key.clone()).or_insert(false);
                if operated_keys.insert(key.clone()) {
                    batch.push((key, *key_state));
                    *key_state = !*key_state;
                }
            }

            // Half of the time, expire a significant number of keys, allowing
            // for our absorbtion rules to apply. We make sure not to consider
            // any keys that are already in the list above, as `modify()` can't
            // modify the same key twice.
            if rng.generate::<f32>() < 0.5 {
                for (key, key_state) in &mut database_state {
                    if *key_state
                        && operated_keys.insert(key.clone())
                        && rng.generate::<f32>() < 0.75
                    {
                        batch.push((key.clone(), true));
                        *key_state = !*key_state;
                    }
                }
            }

            let key_operations = batch
                .iter()
                .cloned()
                .collect::<BTreeMap<ArcBytes<'static>, bool>>();
            tree.modify(Modification {
                persistence_mode: PersistenceMode::Sync,
                keys: key_operations.keys().cloned().collect(),
                operation: Operation::CompareSwap(CompareSwap::new(
                    &mut |key, _index, existing_value| {
                        let should_remove = *key_operations.get(key).unwrap();
                        if should_remove {
                            assert!(
                                existing_value.is_some(),
                                "key {key:?} had no existing value"
                            );
                            KeyOperation::Remove
                        } else {
                            assert!(existing_value.is_none(), "key {key:?} already had a value");
                            KeyOperation::Set(key.to_owned())
                        }
                    },
                )),
            })
            .unwrap();
        }
    }

    #[test]
    fn std_bulk_compare_swaps_unversioned() {
        bulk_compare_swaps::<Unversioned, _>("unversioned", StdFileManager::default());
    }

    #[test]
    fn std_bulk_compare_swaps_versioned() {
        bulk_compare_swaps::<Versioned, _>("versioned", StdFileManager::default());
    }
}
