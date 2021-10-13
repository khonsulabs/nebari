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
//! ## File pages
//!
//! The file is written in pages that are 4,096 bytes long. Each page has single
//! `u8` representing the `PageHeader`. If data needs to span more than one
//! page, every [`PAGE_SIZE`] byte boundary must contain a `PageHeader::Continuation`.
//!
//! ### File Headers
//!
//! If the header is a `PageHeader::Header`, the contents of the block will be a
//! single chunk that contains a serialized `BTreeRoot`.
//!
//! ## Chunks
//!
//! Each time a value, B-Tree node, or header is written, it is written as a
//! chunk. If a [`Vault`] is in-use, each chunk will be pre-processed by the
//! vault before a `CRC-32-BZIP2` checksum is calculated. A chunk is limited to
//! 4 gigabytes of data (2^32).
//!
//! The chunk is written as:
//!
//! - `u32` - Data length, excluding the header.
//! - `u32` - CRC
//! - `[u8]` - Contents
//!
//! A data block may contain more than one chunk.

use std::{
    borrow::Cow,
    collections::HashMap,
    convert::TryFrom,
    fmt::{Debug, Display},
    hash::BuildHasher,
    io::SeekFrom,
    marker::PhantomData,
    ops::{Bound, Deref, DerefMut, RangeBounds},
    path::{Path, PathBuf},
    sync::Arc,
};

use byteorder::{BigEndian, ByteOrder};
use crc::{Crc, CRC_32_BZIP2};
use parking_lot::MutexGuard;

use crate::{
    chunk_cache::CacheEntry,
    error::Error,
    io::{FileManager, FileOp, ManagedFile, OpenableFile},
    roots::AbortError,
    transaction::{TransactionHandle, TransactionManager},
    tree::btree_entry::ScanArgs,
    Buffer, ChunkCache, CompareAndSwapError, Context, ErrorKind, Vault,
};

mod btree_entry;
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

use self::serialization::BinarySerialization;
pub use self::{
    btree_entry::{BTreeEntry, BTreeNode, KeyOperation, Reducer},
    by_id::{ByIdStats, UnversionedByIdIndex, VersionedByIdIndex},
    by_sequence::{BySequenceIndex, BySequenceStats},
    interior::{Interior, Pointer},
    key_entry::{KeyEntry, ValueIndex},
    modify::{CompareSwap, CompareSwapFn, Modification, Operation},
    root::{AnyTreeRoot, Root, TreeRoot},
    state::{ActiveState, State},
    unversioned::UnversionedTreeRoot,
    versioned::{KeySequence, VersionedTreeRoot},
};

/// The number of bytes in each page on-disk.
// The memory used by PagedWriter is PAGE_SIZE * PAGED_WRITER_BATCH_COUNT. E.g,
// 4096 * 4 = 16kb
pub const PAGE_SIZE: usize = 256;

const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_BZIP2);

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
pub struct TreeFile<Root: root::Root, File: ManagedFile> {
    pub(crate) file: <File::Manager as FileManager>::FileHandle,
    /// The state of the file.
    pub state: State<Root>,
    vault: Option<Arc<dyn Vault>>,
    cache: Option<ChunkCache>,
    scratch: Vec<u8>,
}

impl<Root: root::Root, File: ManagedFile> Deref for TreeFile<Root, File> {
    type Target = <File::Manager as FileManager>::FileHandle;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

impl<Root: root::Root, File: ManagedFile> DerefMut for TreeFile<Root, File> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.file
    }
}

impl<Root: root::Root, File: ManagedFile> TreeFile<Root, File> {
    /// Returns a tree as contained in `file`.
    ///
    /// `state` should already be initialized using [`Self::initialize_state`] if the file exists.
    pub fn new(
        file: <File::Manager as FileManager>::FileHandle,
        state: State<Root>,
        vault: Option<Arc<dyn Vault>>,
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

    /// Opens a tree file with read-only permissions.
    pub fn read(
        path: impl AsRef<Path>,
        state: State<Root>,
        context: &Context<File::Manager>,
        transactions: Option<&TransactionManager<File::Manager>>,
    ) -> Result<Self, Error> {
        let file = context.file_manager.read(path.as_ref())?;
        Self::initialize_state(&state, path.as_ref(), file.id(), context, transactions)?;
        Self::new(file, state, context.vault.clone(), context.cache.clone())
    }

    /// Opens a tree file with the ability to read and write.
    pub fn write(
        path: impl AsRef<Path>,
        state: State<Root>,
        context: &Context<File::Manager>,
        transactions: Option<&TransactionManager<File::Manager>>,
    ) -> Result<Self, Error> {
        let file = context.file_manager.append(path.as_ref())?;
        Self::initialize_state(&state, path.as_ref(), file.id(), context, transactions)?;
        Self::new(file, state, context.vault.clone(), context.cache.clone())
    }

    /// Attempts to load the last saved state of this tree into `state`.
    pub fn initialize_state(
        state: &State<Root>,
        file_path: &Path,
        file_id: Option<u64>,
        context: &Context<File::Manager>,
        transaction_manager: Option<&TransactionManager<File::Manager>>,
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

        active_state.file_id = file_id;
        let file_length = context.file_manager.file_length(file_path)?;
        if file_length == 0 {
            active_state.root.initialize_default();
            active_state.publish(state);
            return Ok(());
        }

        let mut tree = File::open_for_read(file_path, None)?;

        // Scan back block by block until we find a header page.
        let mut block_start = file_length - (file_length % PAGE_SIZE as u64);
        if file_length - block_start < 4 {
            // We need room for at least the 4-byte page header
            block_start -= PAGE_SIZE as u64;
        }
        let mut scratch_buffer = vec![0_u8; 4];
        active_state.root = loop {
            // Read the page header
            tree.seek(SeekFrom::Start(block_start))?;
            tree.read_exact(&mut scratch_buffer)?;

            #[allow(clippy::match_on_vec_items)]
            match (
                &scratch_buffer[0..3],
                PageHeader::try_from(scratch_buffer[3]),
            ) {
                (b"Nbr", Ok(header)) => {
                    if header != Root::HEADER {
                        return Err(Error::data_integrity(format!(
                            "Tree {:?} contained another header type",
                            file_path
                        )));
                    }
                    let contents = match read_chunk(
                        block_start + 4,
                        true,
                        &mut tree,
                        context.vault(),
                        context.cache(),
                    )? {
                        CacheEntry::Buffer(buffer) => buffer,
                        CacheEntry::Decoded(_) => unreachable!(),
                    };
                    let root = Root::deserialize(contents)
                        .map_err(|err| ErrorKind::DataIntegrity(Box::new(err)))?;
                    if let Some(transaction_manager) = transaction_manager {
                        if !transaction_manager.transaction_was_successful(root.transaction_id())? {
                            // The transaction wasn't written successfully, so
                            // we cannot trust the data present.
                            if block_start == 0 {
                                // No data was ever fully written.
                                active_state.root.initialize_default();
                                return Ok(());
                            }
                            block_start -= PAGE_SIZE as u64;
                            continue;
                        }
                    }
                    break root;
                }
                (_, Ok(_) | Err(_)) => {
                    if block_start == 0 {
                        return Err(Error::data_integrity(format!(
                            "Tree {:?} contained data, but no valid pages were found",
                            file_path
                        )));
                    }
                    block_start -= PAGE_SIZE as u64;
                    continue;
                }
            }
        };

        active_state.current_position = file_length;
        active_state.publish(state);
        Ok(())
    }

    /// Pushes a key/value pair. Replaces any previous value if set.
    pub fn push(
        &mut self,
        transaction_id: Option<u64>,
        key: Buffer<'static>,
        value: Buffer<'static>,
    ) -> Result<(), Error> {
        self.file.execute(TreeModifier {
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            modification: Some(Modification {
                transaction_id,
                keys: vec![key],
                operation: Operation::Set(value),
            }),
            scratch: &mut self.scratch,
        })
    }

    /// Executes a modification.
    pub fn modify(&mut self, modification: Modification<'_, Buffer<'static>>) -> Result<(), Error> {
        self.file.execute(TreeModifier {
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            modification: Some(modification),
            scratch: &mut self.scratch,
        })
    }

    /// Compares the value of `key` against `old`. If the values match, key will
    /// be set to the new value if `new` is `Some` or removed if `new` is
    /// `None`.
    pub fn compare_and_swap(
        &mut self,
        key: &[u8],
        old: Option<&Buffer<'_>>,
        mut new: Option<Buffer<'_>>,
        transaction_id: Option<u64>,
    ) -> Result<(), CompareAndSwapError> {
        let mut result = Ok(());
        self.modify(Modification {
            transaction_id,
            keys: vec![Buffer::from(key)],
            operation: Operation::CompareSwap(CompareSwap::new(&mut |_key, value| {
                if value.as_ref() == old {
                    match new.take() {
                        Some(new) => KeyOperation::Set(new.to_owned()),
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

    /// Removes `key` and returns the existing value, if present.
    pub fn remove(
        &mut self,
        key: &[u8],
        transaction_id: Option<u64>,
    ) -> Result<Option<Buffer<'static>>, Error> {
        let mut existing_value = None;
        self.modify(Modification {
            transaction_id,
            keys: vec![Buffer::from(key)],
            operation: Operation::CompareSwap(CompareSwap::new(&mut |_key, value| {
                existing_value = value;
                KeyOperation::Remove
            })),
        })?;
        Ok(existing_value)
    }

    /// Sets `key` to `value`. If a value already exists, it will be returned.
    #[allow(clippy::missing_panics_doc)]
    pub fn replace(
        &mut self,
        key: impl Into<Buffer<'static>>,
        value: impl Into<Buffer<'static>>,
        transaction_id: Option<u64>,
    ) -> Result<Option<Buffer<'static>>, Error> {
        let mut existing_value = None;
        let mut value = Some(value.into());
        self.modify(Modification {
            transaction_id,
            keys: vec![key.into()],
            operation: Operation::CompareSwap(CompareSwap::new(&mut |_, stored_value| {
                existing_value = stored_value;
                KeyOperation::Set(value.take().unwrap())
            })),
        })?;

        Ok(existing_value)
    }

    /// Gets the value stored for `key`.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn get<'k>(
        &mut self,
        key: &'k [u8],
        in_transaction: bool,
    ) -> Result<Option<Buffer<'static>>, Error> {
        let mut buffer = None;
        self.file.execute(TreeGetter {
            from_transaction: in_transaction,
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            keys: KeyRange::new(std::iter::once(key)),
            key_reader: |_key, value| {
                buffer = Some(value);
                Ok(())
            },
            key_evaluator: |_| KeyEvaluation::ReadData,
        })?;
        Ok(buffer)
    }

    /// Gets the values stored in `keys`. Does not error if a key is missing.
    /// Returns key/value pairs in an unspecified order.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn get_multiple(
        &mut self,
        keys: &[&[u8]],
        in_transaction: bool,
    ) -> Result<Vec<(Buffer<'static>, Buffer<'static>)>, Error> {
        let mut buffers = Vec::with_capacity(keys.len());
        self.file.execute(TreeGetter {
            from_transaction: in_transaction,
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            keys: KeyRange::new(keys.iter().copied()),
            key_reader: |key, value| {
                buffers.push((key, value));
                Ok(())
            },
            key_evaluator: |_| KeyEvaluation::ReadData,
        })?;
        Ok(buffers)
    }

    /// Retrieves all keys and values with keys that are contained by `range`.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn get_range<'b, B: RangeBounds<Buffer<'b>> + Debug + 'static>(
        &mut self,
        range: B,
        in_transaction: bool,
    ) -> Result<Vec<(Buffer<'static>, Buffer<'static>)>, Error> {
        let mut results = Vec::new();
        self.scan(
            range,
            true,
            in_transaction,
            &mut |_| KeyEvaluation::ReadData,
            &mut |key, value| {
                results.push((key, value));
                Ok(())
            },
        )?;
        Ok(results)
    }

    /// Scans the tree for keys that are contained within `range`. If `forwards`
    /// is true, scanning starts at the lowest sort-order key and scans forward.
    /// Otherwise, scanning starts at the highest sort-order key and scans
    /// backwards. `key_evaluator` is invoked for each key as it is encountered.
    /// For all [`KeyEvaluation::ReadData`] results returned, `callback` will be
    /// invoked with the key and values. The callback may not be invoked in the
    /// same order as the keys are scanned.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(self, key_evaluator, callback))
    )]
    pub fn scan<'range, CallerError, Range, KeyEvaluator, DataCallback>(
        &mut self,
        range: Range,
        forwards: bool,
        in_transaction: bool,
        key_evaluator: &mut KeyEvaluator,
        callback: &mut DataCallback,
    ) -> Result<(), AbortError<CallerError>>
    where
        Range: RangeBounds<Buffer<'range>> + Debug + 'static,
        KeyEvaluator: FnMut(&Buffer<'static>) -> KeyEvaluation,
        DataCallback:
            FnMut(Buffer<'static>, Buffer<'static>) -> Result<(), AbortError<CallerError>>,
        CallerError: Display + Debug,
    {
        self.file.execute(TreeScanner {
            forwards,
            from_transaction: in_transaction,
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            range,
            key_reader: |key: Buffer<'static>, _: &Root::Index, value: Buffer<'static>| {
                callback(key, value)
            },
            key_evaluator: |key: &Buffer<'static>, _: &Root::Index| key_evaluator(key),
            _phantom: PhantomData,
        })?;
        Ok(())
    }

    /// Returns the last key of the tree.
    pub fn last_key(&mut self, in_transaction: bool) -> Result<Option<Buffer<'static>>, Error> {
        let mut result = None;
        self.scan(
            ..,
            false,
            in_transaction,
            &mut |key| {
                result = Some(key.clone());
                KeyEvaluation::Stop
            },
            &mut |_key, _value| Ok(()),
        )?;

        Ok(result)
    }

    /// Returns the last key and value of the tree.
    pub fn last(
        &mut self,
        in_transaction: bool,
    ) -> Result<Option<(Buffer<'static>, Buffer<'static>)>, Error> {
        let mut result = None;
        let mut key_requested = false;
        self.scan(
            ..,
            false,
            in_transaction,
            &mut |_| {
                if key_requested {
                    KeyEvaluation::Stop
                } else {
                    key_requested = true;
                    KeyEvaluation::ReadData
                }
            },
            &mut |key, value| {
                result = Some((key, value));
                Ok(())
            },
        )?;

        Ok(result)
    }

    /// Commits the tree. This is only needed if writes were done with a
    /// transaction id. This will fully flush the tree and publish the
    /// transactional state to be available to readers.
    pub fn commit(&mut self) -> Result<(), Error> {
        self.file.execute(TreeWriter {
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            scratch: &mut self.scratch,
        })
    }

    /// Rewrites the database, removing all unused data in the process. For a
    /// `VersionedTreeRoot`, this will remove old version information.
    ///
    /// This process is done atomically by creating a new file containing the
    /// active data. Once the new file has all the current file's data, the file
    /// contents are swapped using atomic file operations.
    pub fn compact(
        mut self,
        file_manager: &File::Manager,
        transactions: Option<TransactableCompaction<'_, File::Manager>>,
    ) -> Result<Self, Error> {
        let (compacted_path, finisher) = self.file.execute(TreeCompactor {
            state: &self.state,
            vault: self.vault.as_deref(),
            transactions,
            scratch: &mut self.scratch,
        })?;
        self.file = self
            .file
            .replace_with(&compacted_path, file_manager, |file_id| {
                finisher.finish(file_id);
            })?;
        Ok(self)
    }
}

impl<File: ManagedFile> TreeFile<VersionedTreeRoot, File> {
    /// Scans the tree for keys that are contained within `range`. If `forwards`
    /// is true, scanning starts at the lowest sort-order key and scans forward.
    /// Otherwise, scanning starts at the highest sort-order key and scans
    /// backwards. `key_evaluator` is invoked for each key as it is encountered.
    /// For all [`KeyEvaluation::ReadData`] results returned, `callback` will be
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
        key_evaluator: &mut KeyEvaluator,
        data_callback: &mut DataCallback,
    ) -> Result<(), AbortError<CallerError>>
    where
        Range: RangeBounds<u64> + Debug + 'static,
        KeyEvaluator: FnMut(KeySequence) -> KeyEvaluation,
        DataCallback: FnMut(KeySequence, Buffer<'static>) -> Result<(), AbortError<CallerError>>,
        CallerError: Display + Debug,
    {
        self.file.execute(TreeSequenceScanner {
            forwards,
            from_transaction: in_transaction,
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            range: U64Bounds::new(range),
            key_evaluator: &mut move |key: &Buffer<'static>, index: &BySequenceIndex| {
                let id = BigEndian::read_u64(key);
                key_evaluator(KeySequence {
                    key: index.key.clone(),
                    sequence: id,
                    last_sequence: index.last_sequence,
                })
            },
            data_callback,
            _phantom: PhantomData,
        })?;
        Ok(())
    }
}

/// A compaction process that runs in concert with a transaction manager.
pub struct TransactableCompaction<'a, Manager: FileManager> {
    /// The name of the tree being compacted.
    pub name: &'a str,
    /// The transaction manager.
    pub manager: &'a TransactionManager<Manager>,
}

struct TreeCompactor<'a, Root: root::Root, Manager: FileManager> {
    state: &'a State<Root>,
    vault: Option<&'a dyn Vault>,
    transactions: Option<TransactableCompaction<'a, Manager>>,
    scratch: &'a mut Vec<u8>,
}
impl<'a, Root: root::Root, File: ManagedFile> FileOp<File>
    for TreeCompactor<'a, Root, File::Manager>
{
    type Output = Result<(PathBuf, TreeCompactionFinisher<'a, Root>), Error>;

    fn execute(self, file: &mut File) -> Self::Output {
        let current_path = file.path().to_path_buf();
        let file_name = current_path
            .file_name()
            .ok_or_else(|| ErrorKind::message("could not retrieve file name"))?;
        let mut compacted_name = file_name.to_os_string();
        compacted_name.push(".compacting");
        let compacted_path = current_path
            .parent()
            .ok_or_else(|| ErrorKind::message("couldn't access parent of file"))?
            .join(compacted_name);

        if compacted_path.exists() {
            std::fs::remove_file(&compacted_path)?;
        }

        let transaction = self.transactions.as_ref().map(|transactions| {
            transactions
                .manager
                .new_transaction([transactions.name.as_bytes()])
        });
        let mut new_file = File::open_for_append(&compacted_path, None)?;
        let mut writer = PagedWriter::new(None, &mut new_file, self.vault, None, 0)?;

        // Use the read state to list all the currently live chunks
        let mut copied_chunks = HashMap::new();
        let read_state = self.state.read();
        let mut temporary_header = read_state.root.clone();
        drop(read_state);
        temporary_header.copy_data_to(false, file, &mut copied_chunks, &mut writer, self.vault)?;

        // Now, do the same with the write state, which should be very fast,
        // since only nodes that have changed will need to be visited.
        let mut write_state = self.state.lock();
        write_state
            .root
            .copy_data_to(true, file, &mut copied_chunks, &mut writer, self.vault)?;

        save_tree(&mut write_state, self.vault, None, writer, self.scratch)?;

        // Close any existing handles to the file. This ensures that once we
        // save the tree, new requests to the file manager will point to the new
        // file.

        Ok((
            compacted_path,
            TreeCompactionFinisher {
                write_state,
                state: self.state,
                _transaction: transaction,
            },
        ))
    }
}

struct TreeCompactionFinisher<'a, Root: root::Root> {
    state: &'a State<Root>,
    write_state: MutexGuard<'a, ActiveState<Root>>,
    _transaction: Option<TransactionHandle>,
}

impl<'a, Root: root::Root> TreeCompactionFinisher<'a, Root> {
    fn finish(mut self, new_file_id: u64) {
        self.write_state.file_id = Some(new_file_id);
        self.write_state.publish(self.state);
        drop(self);
    }
}

struct TreeWriter<'a, Root: root::Root> {
    state: &'a State<Root>,
    vault: Option<&'a dyn Vault>,
    cache: Option<&'a ChunkCache>,
    scratch: &'a mut Vec<u8>,
}

impl<'a, Root: root::Root, File: ManagedFile> FileOp<File> for TreeWriter<'a, Root> {
    type Output = Result<(), Error>;
    fn execute(self, file: &mut File) -> Self::Output {
        let mut active_state = self.state.lock();
        if active_state.file_id != file.id() {
            return Err(Error::from(ErrorKind::TreeCompacted));
        }
        if active_state.root.dirty() {
            let data_block = PagedWriter::new(
                None,
                file,
                self.vault.as_deref(),
                self.cache,
                active_state.current_position,
            )?;

            self.scratch.clear();
            save_tree(
                &mut *active_state,
                self.vault,
                self.cache,
                data_block,
                self.scratch,
            )
        } else {
            Ok(())
        }
    }
}

struct TreeModifier<'a, 'm, Root: root::Root> {
    state: &'a State<Root>,
    vault: Option<&'a dyn Vault>,
    cache: Option<&'a ChunkCache>,
    modification: Option<Modification<'m, Buffer<'static>>>,
    scratch: &'a mut Vec<u8>,
}

impl<'a, 'm, Root: root::Root, File: ManagedFile> FileOp<File> for TreeModifier<'a, 'm, Root> {
    type Output = Result<(), Error>;

    fn execute(mut self, file: &mut File) -> Self::Output {
        let mut active_state = self.state.lock();
        if active_state.file_id != file.id() {
            return Err(Error::from(ErrorKind::TreeCompacted));
        }

        let mut data_block = PagedWriter::new(
            None,
            file,
            self.vault.as_deref(),
            self.cache,
            active_state.current_position,
        )?;

        let modification = self.modification.take().unwrap();
        let is_transactional = modification.transaction_id.is_some();
        let max_order = active_state.max_order;

        // Execute the modification
        active_state
            .root
            .modify(modification, &mut data_block, max_order)?;

        if is_transactional {
            // Transactions will written to disk later.
            let (_, new_position) = data_block.finish()?;
            active_state.current_position = new_position;
        } else {
            // Save the tree to disk immediately.
            self.scratch.clear();
            save_tree(
                &mut *active_state,
                self.vault,
                self.cache,
                data_block,
                self.scratch,
            )?;
            active_state.publish(self.state);
        }

        Ok(())
    }
}

#[allow(clippy::shadow_unrelated)] // It is related, but clippy can't tell.
fn save_tree<Root: root::Root, File: ManagedFile>(
    active_state: &mut ActiveState<Root>,
    vault: Option<&dyn Vault>,
    cache: Option<&ChunkCache>,
    mut data_block: PagedWriter<'_, File>,
    scratch: &mut Vec<u8>,
) -> Result<(), Error> {
    scratch.clear();
    active_state.root.serialize(&mut data_block, scratch)?;
    let (file, after_data) = data_block.finish()?;
    active_state.current_position = after_data;

    // Write a new header.
    let mut header_block = PagedWriter::new(
        Some(Root::HEADER),
        file,
        vault,
        cache,
        active_state.current_position,
    )?;
    header_block.write_chunk(scratch, false)?;

    let (file, after_header) = header_block.finish()?;
    active_state.current_position = after_header;

    file.flush()?;
    Ok(())
}

/// One or more keys.
#[derive(Debug)]
pub struct KeyRange<'a, I: Iterator<Item = &'a [u8]>> {
    remaining_keys: I,
    current_key: Option<&'a [u8]>,
}

impl<'a, I: Iterator<Item = &'a [u8]>> KeyRange<'a, I> {
    fn new(mut keys: I) -> Self {
        Self {
            current_key: keys.next(),
            remaining_keys: keys,
        }
    }

    fn current_key(&self) -> Option<&'a [u8]> {
        self.current_key
    }
}

impl<'a, I: Iterator<Item = &'a [u8]>> Iterator for KeyRange<'a, I> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<&'a [u8]> {
        let key_to_return = self.current_key;
        self.current_key = self.remaining_keys.next();
        key_to_return
    }
}

#[derive(Clone, Copy)]
/// The result of evaluating a key that was scanned.
pub enum KeyEvaluation {
    /// Read the data for this key.
    ReadData,
    /// Skip this key.
    Skip,
    /// Stop scanning keys.
    Stop,
}

struct TreeGetter<
    'a,
    'keys,
    Root: root::Root,
    KeyEvaluator: FnMut(&Buffer<'static>) -> KeyEvaluation,
    KeyReader: FnMut(Buffer<'static>, Buffer<'static>) -> Result<(), Error>,
    Keys: Iterator<Item = &'keys [u8]>,
> {
    from_transaction: bool,
    state: &'a State<Root>,
    vault: Option<&'a dyn Vault>,
    cache: Option<&'a ChunkCache>,
    keys: Keys,
    key_evaluator: KeyEvaluator,
    key_reader: KeyReader,
}

impl<'a, 'keys, KeyEvaluator, KeyReader, Root, File, Keys> FileOp<File>
    for TreeGetter<'a, 'keys, Root, KeyEvaluator, KeyReader, Keys>
where
    KeyEvaluator: FnMut(&Buffer<'static>) -> KeyEvaluation,
    KeyReader: FnMut(Buffer<'static>, Buffer<'static>) -> Result<(), Error>,
    Keys: Iterator<Item = &'keys [u8]>,
    Root: root::Root,
    File: ManagedFile,
{
    type Output = Result<(), Error>;
    fn execute(mut self, file: &mut File) -> Self::Output {
        if self.from_transaction {
            let state = self.state.lock();
            if state.file_id != file.id() {
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
            if state.file_id != file.id() {
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
    KeyEvaluator,
    KeyReader,
    KeyRangeBounds,
> where
    KeyEvaluator: FnMut(&Buffer<'static>, &Root::Index) -> KeyEvaluation,
    KeyReader: FnMut(
        Buffer<'static>,
        &Root::Index,
        Buffer<'static>,
    ) -> Result<(), AbortError<CallerError>>,
    KeyRangeBounds: RangeBounds<Buffer<'keys>>,
    CallerError: Display + Debug,
{
    forwards: bool,
    from_transaction: bool,
    state: &'a State<Root>,
    vault: Option<&'a dyn Vault>,
    cache: Option<&'a ChunkCache>,
    range: KeyRangeBounds,
    key_evaluator: KeyEvaluator,
    key_reader: KeyReader,
    _phantom: PhantomData<&'keys ()>,
}

impl<'a, 'keys, CallerError, Root: root::Root, KeyEvaluator, KeyReader, KeyRangeBounds, File>
    FileOp<File>
    for TreeScanner<'a, 'keys, CallerError, Root, KeyEvaluator, KeyReader, KeyRangeBounds>
where
    File: ManagedFile,
    KeyEvaluator: FnMut(&Buffer<'static>, &Root::Index) -> KeyEvaluation,
    KeyReader: FnMut(
        Buffer<'static>,
        &Root::Index,
        Buffer<'static>,
    ) -> Result<(), AbortError<CallerError>>,
    KeyRangeBounds: RangeBounds<Buffer<'keys>> + Debug,
    CallerError: Display + Debug,
{
    type Output = Result<bool, AbortError<CallerError>>;
    fn execute(mut self, file: &mut File) -> Self::Output {
        if self.from_transaction {
            let state = self.state.lock();
            if state.file_id != file.id() {
                return Err(AbortError::Nebari(Error::from(ErrorKind::TreeCompacted)));
            }

            state.root.scan(
                &self.range,
                &mut ScanArgs::new(self.forwards, &mut self.key_evaluator, &mut self.key_reader),
                file,
                self.vault,
                self.cache,
            )
        } else {
            let state = self.state.read();
            if state.file_id != file.id() {
                return Err(AbortError::Nebari(Error::from(ErrorKind::TreeCompacted)));
            }

            state.root.scan(
                &self.range,
                &mut ScanArgs::new(self.forwards, &mut self.key_evaluator, &mut self.key_reader),
                file,
                self.vault,
                self.cache,
            )
        }
    }
}
struct TreeSequenceScanner<'a, 'keys, KeyEvaluator, KeyRangeBounds, DataCallback, CallerError>
where
    KeyEvaluator: FnMut(&Buffer<'static>, &BySequenceIndex) -> KeyEvaluation,
    KeyRangeBounds: RangeBounds<Buffer<'keys>>,
    DataCallback: FnMut(KeySequence, Buffer<'static>) -> Result<(), AbortError<CallerError>>,
    CallerError: Display + Debug,
{
    forwards: bool,
    from_transaction: bool,
    state: &'a State<VersionedTreeRoot>,
    vault: Option<&'a dyn Vault>,
    cache: Option<&'a ChunkCache>,
    range: KeyRangeBounds,
    key_evaluator: KeyEvaluator,
    data_callback: DataCallback,
    _phantom: PhantomData<&'keys ()>,
}

impl<'a, 'keys, KeyEvaluator, KeyRangeBounds, File, DataCallback, CallerError> FileOp<File>
    for TreeSequenceScanner<'a, 'keys, KeyEvaluator, KeyRangeBounds, DataCallback, CallerError>
where
    File: ManagedFile,
    KeyEvaluator: FnMut(&Buffer<'static>, &BySequenceIndex) -> KeyEvaluation,
    KeyRangeBounds: RangeBounds<Buffer<'keys>> + Debug,
    DataCallback: FnMut(KeySequence, Buffer<'static>) -> Result<(), AbortError<CallerError>>,
    CallerError: Display + Debug,
{
    type Output = Result<(), AbortError<CallerError>>;
    fn execute(self, file: &mut File) -> Self::Output {
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
            |key: Buffer<'static>, index: &BySequenceIndex, data: Buffer<'static>| {
                let sequence = BigEndian::read_u64(&key);
                (data_callback)(
                    KeySequence {
                        key: index.key.clone(),
                        sequence,
                        last_sequence: index.last_sequence,
                    },
                    data,
                )
            };
        if from_transaction {
            let state = state.lock();
            if state.file_id != file.id() {
                return Err(AbortError::Nebari(Error::from(ErrorKind::TreeCompacted)));
            }

            state
                .root
                .by_sequence_root
                .scan(
                    &range,
                    &mut ScanArgs::new(forwards, &mut key_evaluator, mapped_data_callback),
                    file,
                    vault,
                    cache,
                )
                .map(|_| {})
        } else {
            let state = state.read();
            if state.file_id != file.id() {
                return Err(AbortError::Nebari(Error::from(ErrorKind::TreeCompacted)));
            }

            state
                .root
                .by_sequence_root
                .scan(
                    &range,
                    &mut ScanArgs::new(forwards, &mut key_evaluator, mapped_data_callback),
                    file,
                    vault,
                    cache,
                )
                .map(|_| {})
        }
    }
}

/// Writes data in pages, allowing for quick scanning through the file.
pub struct PagedWriter<'a, File: ManagedFile> {
    file: &'a mut File,
    vault: Option<&'a dyn Vault>,
    cache: Option<&'a ChunkCache>,
    position: u64,
    offset: usize,
    buffered_write: [u8; WRITE_BUFFER_SIZE],
}

impl<'a, File: ManagedFile> Deref for PagedWriter<'a, File> {
    type Target = File;

    fn deref(&self) -> &Self::Target {
        self.file
    }
}

impl<'a, File: ManagedFile> DerefMut for PagedWriter<'a, File> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.file
    }
}

const WRITE_BUFFER_SIZE: usize = 8 * 1024;

impl<'a, File: ManagedFile> PagedWriter<'a, File> {
    fn new(
        header: Option<PageHeader>,
        file: &'a mut File,
        vault: Option<&'a dyn Vault>,
        cache: Option<&'a ChunkCache>,
        position: u64,
    ) -> Result<PagedWriter<'a, File>, Error> {
        let mut writer = Self {
            file,
            vault,
            cache,
            position,
            offset: 0,
            buffered_write: [0; WRITE_BUFFER_SIZE],
        };
        if let Some(header) = header {
            // Ensure alignment if we have a header
            #[allow(clippy::cast_possible_truncation)]
            let padding_needed = PAGE_SIZE - (writer.position % PAGE_SIZE as u64) as usize;
            let mut padding_and_header = Vec::new();
            padding_and_header.resize(padding_needed + 4, header as u8);
            padding_and_header.splice(
                padding_and_header.len() - 4..padding_and_header.len() - 1,
                b"Nbr".iter().copied(),
            );
            writer.write(&padding_and_header)?;
        }
        if writer.current_position() == 0 {
            // Write a magic code
            writer.write(b"Nbri")?;
        }
        Ok(writer)
    }

    fn current_position(&self) -> u64 {
        self.position + self.offset as u64
    }

    fn write(&mut self, mut data: &[u8]) -> Result<usize, Error> {
        let bytes_written = data.len();
        if data.len() > self.buffered_write.len() {
            self.commit_if_needed()?;
            self.file.write_all(data)?;
            self.position += data.len() as u64;
        } else {
            while !data.is_empty() {
                let bytes_available = self.buffered_write.len() - self.offset;
                let bytes_to_copy = bytes_available.min(data.len());
                let new_offset = self.offset + bytes_to_copy;
                let (data_to_copy, remaining) = data.split_at(bytes_to_copy);
                self.buffered_write[self.offset..new_offset].copy_from_slice(data_to_copy);
                self.offset = new_offset;
                if self.offset == self.buffered_write.len() {
                    self.commit()?;
                }

                data = remaining;
            }
        }
        Ok(bytes_written)
    }

    fn commit_if_needed(&mut self) -> Result<(), Error> {
        if self.offset > 0 {
            self.commit()?;
        }
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        self.file.write_all(&self.buffered_write[0..self.offset])?;
        self.position += self.offset as u64;
        self.offset = 0;
        Ok(())
    }

    /// Writes a chunk of data to the file, after possibly encrypting it.
    /// Returns the position that this chunk can be read from in the file.
    #[allow(clippy::cast_possible_truncation)]
    fn write_chunk(&mut self, contents: &[u8], cache: bool) -> Result<u64, Error> {
        let possibly_encrypted = self.vault.as_ref().map_or_else(
            || Cow::Borrowed(contents),
            |vault| Cow::Owned(vault.encrypt(contents)),
        );
        let length =
            u32::try_from(possibly_encrypted.len()).map_err(|_| ErrorKind::ValueTooLarge)?;
        let crc = CRC32.checksum(&possibly_encrypted);
        let position = self.current_position();

        self.write_u32::<BigEndian>(length)?;
        self.write_u32::<BigEndian>(crc)?;
        self.write(&possibly_encrypted)?;

        if cache {
            if let (Some(cache), Cow::Owned(vec), Some(file_id)) =
                (self.cache, possibly_encrypted, self.file.id())
            {
                cache.insert(file_id, position, Buffer::from(vec));
            }
        }

        Ok(position)
    }

    fn read_chunk(&mut self, position: u64) -> Result<CacheEntry, Error> {
        read_chunk(position, false, self.file, self.vault, self.cache)
    }

    fn write_u32<B: ByteOrder>(&mut self, value: u32) -> Result<usize, Error> {
        let mut buffer = [0_u8; 4];
        B::write_u32(&mut buffer, value);
        self.write(&buffer)
    }

    fn finish(mut self) -> Result<(&'a mut File, u64), Error> {
        self.commit_if_needed()?;
        Ok((self.file, self.position))
    }
}

#[allow(clippy::cast_possible_truncation)]
#[cfg_attr(feature = "tracing", tracing::instrument(skip(file, vault, cache)))]
fn read_chunk<File: ManagedFile>(
    position: u64,
    validate_crc: bool,
    file: &mut File,
    vault: Option<&dyn Vault>,
    cache: Option<&ChunkCache>,
) -> Result<CacheEntry, Error> {
    if let (Some(cache), Some(file_id)) = (cache, file.id()) {
        if let Some(entry) = cache.get(file_id, position) {
            return Ok(entry);
        }
    }

    // Read the chunk header
    let mut header = [0_u8; 8];
    file.seek(SeekFrom::Start(position))?;
    file.read_exact(&mut header)?;
    let length = BigEndian::read_u32(&header[0..4]) as usize;

    let mut scratch = Vec::new();
    scratch.resize(length, 0);
    file.read_exact(&mut scratch)?;

    if validate_crc {
        let crc = BigEndian::read_u32(&header[4..8]);
        let computed_crc = CRC32.checksum(&scratch);
        if crc != computed_crc {
            return Err(Error::data_integrity(format!(
                "crc32 failure on chunk at position {}",
                position
            )));
        }
    }

    let decrypted = Buffer::from(match vault {
        Some(vault) => vault.decrypt(&scratch),
        None => scratch,
    });

    if let (Some(cache), Some(file_id)) = (cache, file.id()) {
        cache.insert(file_id, position, decrypted.clone());
    }

    Ok(CacheEntry::Buffer(decrypted))
}

pub(crate) fn copy_chunk<File: ManagedFile, Hasher: BuildHasher>(
    original_position: u64,
    from_file: &mut File,
    copied_chunks: &mut std::collections::HashMap<u64, u64, Hasher>,
    to_file: &mut PagedWriter<'_, File>,
    vault: Option<&dyn crate::Vault>,
) -> Result<u64, Error> {
    if original_position == 0 {
        Ok(0)
    } else if let Some(new_position) = copied_chunks.get(&original_position) {
        Ok(*new_position)
    } else {
        // Since these are one-time copies, and receiving a Decoded entry
        // makes things tricky, we're going to not use caching for reads
        // here. This gives the added benefit for a long-running server to
        // ensure it's doing CRC checks occasionally as it copies itself.
        let chunk = match read_chunk(original_position, true, from_file, vault, None)? {
            CacheEntry::Buffer(buffer) => buffer,
            CacheEntry::Decoded(_) => unreachable!(),
        };
        let new_location = to_file.write_chunk(&chunk, false)?;
        copied_chunks.insert(original_position, new_location);
        Ok(new_location)
    }
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
fn dynamic_order(number_of_records: u64, max_order: Option<usize>) -> usize {
    // Current approximation is the 3rd root.
    let max_order = max_order.unwrap_or(DEFAULT_MAX_ORDER);
    if number_of_records > max_order.pow(3) as u64 {
        max_order
    } else {
        let estimated_order = 4.max((number_of_records as f64).cbrt() as usize);
        max_order.min(estimated_order)
    }
}

#[derive(Debug)]
struct U64Bounds {
    start_bound: Bound<u64>,
    start_bound_bytes: Bound<Buffer<'static>>,
    end_bound: Bound<u64>,
    end_bound_bytes: Bound<Buffer<'static>>,
}

impl RangeBounds<u64> for U64Bounds {
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

impl RangeBounds<Buffer<'static>> for U64Bounds {
    fn start_bound(&self) -> Bound<&Buffer<'static>> {
        match &self.start_bound_bytes {
            Bound::Included(value) => Bound::Included(value),
            Bound::Excluded(value) => Bound::Excluded(value),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    fn end_bound(&self) -> Bound<&Buffer<'static>> {
        match &self.end_bound_bytes {
            Bound::Included(value) => Bound::Included(value),
            Bound::Excluded(value) => Bound::Excluded(value),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

impl U64Bounds {
    pub fn new<B: RangeBounds<u64>>(bounds: B) -> Self {
        Self {
            start_bound: bounds.start_bound().cloned(),
            start_bound_bytes: match bounds.start_bound() {
                Bound::Included(id) => Bound::Included(Buffer::from(id.to_be_bytes())),
                Bound::Excluded(id) => Bound::Excluded(Buffer::from(id.to_be_bytes())),
                Bound::Unbounded => Bound::Unbounded,
            },
            end_bound: bounds.end_bound().cloned(),
            end_bound_bytes: match bounds.end_bound() {
                Bound::Included(id) => Bound::Included(Buffer::from(id.to_be_bytes())),
                Bound::Excluded(id) => Bound::Excluded(Buffer::from(id.to_be_bytes())),
                Bound::Unbounded => Bound::Unbounded,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, convert::Infallible};

    use nanorand::{Pcg64, Rng};
    use tempfile::NamedTempFile;

    use super::*;
    use crate::{
        io::{
            fs::{StdFile, StdFileManager},
            memory::{MemoryFile, MemoryFileManager},
        },
        tree::unversioned::UnversionedTreeRoot,
    };

    fn test_paged_write(offset: usize, length: usize) -> Result<(), Error> {
        let mut file = MemoryFile::open_for_append(format!("test-{}-{}", offset, length), None)?;
        let mut paged_writer =
            PagedWriter::new(Some(PageHeader::VersionedHeader), &mut file, None, None, 0)?;

        let mut scratch = Vec::new();
        scratch.resize(offset.max(length), 0);
        if offset > 0 {
            paged_writer.write(&scratch[..offset])?;
        }
        scratch.fill(1);
        let written_position = paged_writer.write_chunk(&scratch[..length], false)?;
        drop(paged_writer.finish());

        match read_chunk(written_position, true, &mut file, None, None)? {
            CacheEntry::Buffer(data) => {
                assert_eq!(data.len(), length);
                assert!(data.iter().all(|i| i == &1));
            }
            CacheEntry::Decoded(_) => unreachable!(),
        }

        drop(file);

        Ok(())
    }

    /// Tests the writing of pages at various boundaries. This should cover
    /// every edge case: offset is on page 1/2/3, write stays on first page or
    /// lands on page 1/2/3 end or extends onto page 4.
    #[test]
    fn paged_writer() {
        for offset in 0..=PAGE_SIZE * 2 {
            for length in [
                1,
                PAGE_SIZE / 2,
                PAGE_SIZE,
                PAGE_SIZE * 3 / 2,
                PAGE_SIZE * 2,
            ] {
                if let Err(err) = test_paged_write(offset, length) {
                    unreachable!(
                        "paged writer failure at offset {} length {}: {:?}",
                        offset, length, err
                    );
                }
            }
        }
    }

    fn insert_one_record<R: Root, F: ManagedFile>(
        context: &Context<F::Manager>,
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
        let id_buffer = Buffer::from(id.to_be_bytes().to_vec());
        {
            let file = context.file_manager.append(file_path).unwrap();
            let state = if ids.len() > 1 {
                let state = State::default();
                TreeFile::<R, F>::initialize_state(&state, file_path, file.id(), context, None)
                    .unwrap();
                state
            } else {
                State::initialized(file.id(), max_order)
            };
            let mut tree =
                TreeFile::<R, F>::new(file, state, context.vault.clone(), context.cache.clone())
                    .unwrap();
            tree.push(None, id_buffer.clone(), Buffer::from(b"hello world"))
                .unwrap();

            // This shouldn't have to scan the file, as the data fits in memory.
            let value = tree.get(&id_buffer, false).unwrap();
            assert_eq!(&*value.unwrap(), b"hello world");
        }

        // Try loading the file up and retrieving the data.
        {
            let state = State::default();
            let file = context.file_manager.append(file_path).unwrap();
            TreeFile::<R, F>::initialize_state(&state, file_path, file.id(), context, None)
                .unwrap();

            let mut tree =
                TreeFile::<R, F>::new(file, state, context.vault.clone(), context.cache.clone())
                    .unwrap();
            let value = tree.get(&id_buffer, false).unwrap();
            assert_eq!(&*value.unwrap(), b"hello world");
        }
    }

    fn remove_one_record<R: Root, F: ManagedFile>(
        context: &Context<F::Manager>,
        file_path: &Path,
        id: u64,
        max_order: Option<usize>,
    ) {
        let id_buffer = Buffer::from(id.to_be_bytes().to_vec());
        {
            let file = context.file_manager.append(file_path).unwrap();
            let state = State::new(None, max_order);
            TreeFile::<R, F>::initialize_state(&state, file_path, file.id(), context, None)
                .unwrap();
            let mut tree =
                TreeFile::<R, F>::new(file, state, context.vault.clone(), context.cache.clone())
                    .unwrap();
            tree.modify(Modification {
                transaction_id: None,
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
            let file = context.file_manager.append(file_path).unwrap();
            let state = State::default();
            TreeFile::<R, F>::initialize_state(&state, file_path, file.id(), context, None)
                .unwrap();

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
        let context = Context {
            file_manager: StdFileManager::default(),
            vault: None,
            cache: None,
        };
        let temp_dir = crate::test_util::TestDirectory::new("btree-tests");
        std::fs::create_dir(&temp_dir).unwrap();
        let file_path = temp_dir.join("tree");
        let mut ids = HashSet::new();
        // Insert up to the limit of a LEAF, which is ORDER - 1.
        for _ in 0..ORDER - 1 {
            insert_one_record::<VersionedTreeRoot, StdFile>(
                &context,
                &file_path,
                &mut ids,
                &mut rng,
                Some(ORDER),
            );
        }
        println!("Successfully inserted up to ORDER - 1 nodes.");

        // The next record will split the node
        insert_one_record::<VersionedTreeRoot, StdFile>(
            &context,
            &file_path,
            &mut ids,
            &mut rng,
            Some(ORDER),
        );
        println!("Successfully introduced one layer of depth.");

        // Insert a lot more.
        for _ in 0..1_000 {
            insert_one_record::<VersionedTreeRoot, StdFile>(
                &context,
                &file_path,
                &mut ids,
                &mut rng,
                Some(ORDER),
            );
        }
    }

    fn remove<R: Root>(label: &str) {
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
        for _ in 0..1000 {
            insert_one_record::<R, StdFile>(&context, &file_path, &mut ids, &mut rng, Some(ORDER));
        }

        let mut ids = ids.into_iter().collect::<Vec<_>>();
        ids.sort_unstable();
        rng.shuffle(&mut ids);

        // Remove each of the records
        for id in ids {
            remove_one_record::<R, StdFile>(&context, &file_path, id, Some(ORDER));
        }
    }

    #[test]
    fn remove_versioned() {
        remove::<VersionedTreeRoot>("versioned");
    }

    #[test]
    fn remove_unversioned() {
        remove::<UnversionedTreeRoot>("unversioned");
    }

    #[test]
    fn spam_insert_std_versioned() {
        spam_insert::<VersionedTreeRoot, StdFile>("std");
    }

    #[test]
    fn spam_insert_std_unversioned() {
        spam_insert::<UnversionedTreeRoot, StdFile>("std");
    }

    #[test]
    #[cfg(feature = "uring")]
    fn spam_insert_uring() {
        spam_insert::<crate::UringFile>("uring");
    }

    fn spam_insert<R: Root, F: ManagedFile>(name: &str) {
        const RECORDS: usize = 1_000;
        let mut rng = Pcg64::new_seed(1);
        let ids = (0..RECORDS).map(|_| rng.generate::<u64>());
        let context = Context {
            file_manager: F::Manager::default(),
            vault: None,
            cache: Some(ChunkCache::new(100, 160_384)),
        };
        let temp_dir = crate::test_util::TestDirectory::new(format!("spam-inserts-{}", name));
        std::fs::create_dir(&temp_dir).unwrap();
        let file_path = temp_dir.join("tree");
        let state = State::default();
        let mut tree = TreeFile::<R, F>::write(file_path, state, &context, None).unwrap();
        for (_index, id) in ids.enumerate() {
            let id_buffer = Buffer::from(id.to_be_bytes().to_vec());
            tree.push(None, id_buffer.clone(), Buffer::from(b"hello world"))
                .unwrap();
        }
    }

    #[test]
    fn bulk_insert_versioned() {
        bulk_insert::<VersionedTreeRoot, StdFile>("std-versioned");
    }

    #[test]
    fn bulk_insert_unversioned() {
        bulk_insert::<UnversionedTreeRoot, StdFile>("std-unversioned");
    }

    fn bulk_insert<R: Root, F: ManagedFile>(name: &str) {
        const RECORDS_PER_BATCH: usize = 10;
        const BATCHES: usize = 1000;
        let mut rng = Pcg64::new_seed(1);
        let context = Context {
            file_manager: F::Manager::default(),
            vault: None,
            cache: Some(ChunkCache::new(100, 160_384)),
        };
        let temp_dir = crate::test_util::TestDirectory::new(format!("bulk-inserts-{}", name));
        std::fs::create_dir(&temp_dir).unwrap();
        let file_path = temp_dir.join("tree");
        let state = State::default();
        let mut tree = TreeFile::<R, F>::write(file_path, state, &context, None).unwrap();
        for _ in 0..BATCHES {
            let mut ids = (0..RECORDS_PER_BATCH)
                .map(|_| rng.generate::<u64>())
                .collect::<Vec<_>>();
            ids.sort_unstable();
            let modification = Modification {
                transaction_id: None,
                keys: ids
                    .iter()
                    .map(|id| Buffer::from(id.to_be_bytes().to_vec()))
                    .collect(),
                operation: Operation::Set(Buffer::from(b"hello world")),
            };
            tree.modify(modification).unwrap();

            // Try five random gets
            for _ in 0..5 {
                let index = rng.generate_range(0..ids.len());
                let id = Buffer::from(ids[index].to_be_bytes().to_vec());
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
            TreeFile::<VersionedTreeRoot, MemoryFile>::write("test", state, &context, None)
                .unwrap();
        // Create enough records to go 4 levels deep.
        let mut ids = Vec::new();
        for id in 0..3_u32.pow(4) {
            let id_buffer = Buffer::from(id.to_be_bytes().to_vec());
            tree.push(None, id_buffer.clone(), id_buffer.clone())
                .unwrap();
            ids.push(id_buffer);
        }

        // Get them all
        let mut all_records = tree
            .get_multiple(&ids.iter().map(Buffer::as_slice).collect::<Vec<_>>(), false)
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
        let mut unbounded_to_five = tree.get_range(..ids[5].clone(), false).unwrap();
        unbounded_to_five.sort();
        assert_eq!(&all_records[..5], &unbounded_to_five);
        let mut one_to_ten_unbounded = tree
            .get_range(ids[1].clone()..ids[10].clone(), false)
            .unwrap();
        one_to_ten_unbounded.sort();
        assert_eq!(&all_records[1..10], &one_to_ten_unbounded);
        let mut bounded_upper = tree
            .get_range(ids[3].clone()..=ids[50].clone(), false)
            .unwrap();
        bounded_upper.sort();
        assert_eq!(&all_records[3..=50], &bounded_upper);
        let mut unbounded_upper = tree.get_range(ids[60].clone().., false).unwrap();
        unbounded_upper.sort();
        assert_eq!(&all_records[60..], &unbounded_upper);
        let mut all_through_scan = tree.get_range(.., false).unwrap();
        all_through_scan.sort();
        assert_eq!(&all_records, &all_through_scan);
    }

    fn compact<R: Root>(label: &str) {
        const ORDER: usize = 4;
        let mut rng = Pcg64::new_seed(1);
        let context = Context {
            file_manager: StdFileManager::default(),
            vault: None,
            cache: None,
        };
        let temp_dir = crate::test_util::TestDirectory::new(format!("btree-compact-{}", label));
        std::fs::create_dir(&temp_dir).unwrap();
        let file_path = temp_dir.join("tree");
        let mut ids = HashSet::new();
        for _ in 0..5 {
            insert_one_record::<R, StdFile>(&context, &file_path, &mut ids, &mut rng, Some(ORDER));
        }

        let mut tree =
            TreeFile::<R, StdFile>::write(&file_path, State::default(), &context, None).unwrap();
        let pre_compact_size = context.file_manager.file_length(&file_path).unwrap();
        tree = tree.compact(&context.file_manager, None).unwrap();
        let after_compact_size = context.file_manager.file_length(&file_path).unwrap();
        assert!(
            after_compact_size < pre_compact_size,
            "compact didn't remove any data"
        );

        // Try fetching all the records to ensure they're still present.
        for id in ids {
            let id_buffer = Buffer::from(id.to_be_bytes().to_vec());
            tree.get(&id_buffer, false)
                .unwrap()
                .expect("no value found");
        }
    }

    #[test]
    fn compact_versioned() {
        compact::<VersionedTreeRoot>("versioned");
    }

    #[test]
    fn compact_unversioned() {
        compact::<UnversionedTreeRoot>("unversioned");
    }

    #[test]
    fn revision_history() {
        let context = Context {
            file_manager: StdFileManager::default(),
            vault: None,
            cache: None,
        };
        let state = State::default();
        let tempfile = NamedTempFile::new().unwrap();
        let mut tree =
            TreeFile::<VersionedTreeRoot, StdFile>::write(tempfile.path(), state, &context, None)
                .unwrap();

        // Store three versions of the same key.
        tree.push(None, Buffer::from(b"a"), Buffer::from(b"0"))
            .unwrap();
        tree.push(None, Buffer::from(b"a"), Buffer::from(b"1"))
            .unwrap();
        tree.push(None, Buffer::from(b"a"), Buffer::from(b"2"))
            .unwrap();

        // Retrieve the sequences
        let mut sequences = Vec::new();
        tree.scan_sequences::<Infallible, _, _, _>(
            ..,
            true,
            false,
            &mut |_| KeyEvaluation::ReadData,
            &mut |sequence, value| {
                sequences.push((sequence, value));
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(sequences.len(), 3);
        sequences.sort_by(|a, b| a.0.sequence.cmp(&b.0.sequence));
        assert!(sequences.iter().all(|s| s.0.key.as_slice() == b"a"));
        assert_eq!(sequences[0].0.last_sequence, None);
        assert_eq!(sequences[1].0.last_sequence, Some(sequences[0].0.sequence));
        assert_eq!(sequences[2].0.last_sequence, Some(sequences[1].0.sequence));

        assert_eq!(sequences[0].1, b"0");
        assert_eq!(sequences[1].1, b"1");
        assert_eq!(sequences[2].1, b"2");
    }
}
