//! Append-only B-Tree implementation
//!
//! The file format is inspired by
//! [Couchstore](https://github.com/couchbase/couchstore). The main difference
//! is that the file header has extra information to allow for cross-tree
//! transactions.
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
//! page, every 4,096 byte boundary must contain a `PageHeader::Continuation`.
//!
//! ### File Headers
//!
//! If the header is a `PageHeader::Header`, the contents of the block will be
//! a single chunk that contains a serialized `BTreeRoot`.
//!
//! ## Chunks
//!
//! Each time a document, B-Tree node, or header is written, it is written as a
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
    convert::{Infallible, TryFrom},
    fmt::{Debug, Display},
    fs::OpenOptions,
    hash::BuildHasher,
    io::SeekFrom,
    marker::PhantomData,
    ops::{Deref, DerefMut, RangeBounds},
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
    tree::{btree_entry::ScanArgs, state::ActiveState},
    Buffer, ChunkCache, CompareAndSwapError, Context, ErrorKind, Vault,
};

mod btree_entry;
mod by_id;
mod by_sequence;
mod interior;
mod key_entry;
mod modify;
mod root;
mod serialization;
pub(crate) mod state;
mod unversioned;
mod versioned;

use self::serialization::BinarySerialization;
pub use self::{
    btree_entry::KeyOperation,
    modify::{CompareSwap, CompareSwapFn, Modification, Operation},
    root::{Root, TreeRoot},
    state::State,
    unversioned::UnversionedTreeRoot,
    versioned::VersionedTreeRoot,
};

// The memory used by PagedWriter is PAGE_SIZE * PAGED_WRITER_BATCH_COUNT. E.g,
// 4096 * 4 = 16kb
const PAGE_SIZE: usize = 4096;
const PAGED_WRITER_BATCH_COUNT: usize = 4;

const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_BZIP2);

/// The header byte for a tree file's page.
#[derive(Eq, PartialEq)]
pub enum PageHeader {
    // Using a strange value so that errors in the page writing/reading
    // algorithm are easier to detect.
    /// A page that continues data from a previous page.
    Continuation = 0xF1,
    /// A page that contains only chunks, no headers.
    Data = 1,
    /// A [`VersionedTreeRoot`] header.
    VersionedHeader = 2,
    /// An [`UnversionedTreeRoot`] header.
    UnversionedHeader = 3,
}

impl TryFrom<u8> for PageHeader {
    type Error = ErrorKind;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0xF1 => Ok(Self::Continuation),
            1 => Ok(Self::Data),
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
/// - `F`: An [`ManagedFile`] implementor.
/// - `MAX_ORDER`: The maximum number of children a node in the tree can
///   contain. This implementation attempts to grow naturally towards this upper
///   limit. Changing this parameter does not automatically rebalance the tree,
///   but over time the tree will be updated.
pub struct TreeFile<Root: root::Root, F: ManagedFile> {
    pub(crate) file: <F::Manager as FileManager>::FileHandle,
    /// The state of the file.
    pub state: State<Root>,
    vault: Option<Arc<dyn Vault>>,
    cache: Option<ChunkCache>,
    scratch: Vec<u8>,
}

impl<Root: root::Root, F: ManagedFile> TreeFile<Root, F> {
    /// Returns a tree as contained in `file`.
    ///
    /// `state` should already be initialized using [`Self::initialize_state`] if the file exists.
    pub fn new(
        file: <F::Manager as FileManager>::FileHandle,
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
        context: &Context<F::Manager>,
        transactions: Option<&TransactionManager<F::Manager>>,
    ) -> Result<Self, Error> {
        let file = context.file_manager.read(path.as_ref())?;
        Self::initialize_state(&state, path.as_ref(), file.id(), context, transactions)?;
        Self::new(file, state, context.vault.clone(), context.cache.clone())
    }

    /// Opens a tree file with the ability to read and write.
    pub fn write(
        path: impl AsRef<Path>,
        state: State<Root>,
        context: &Context<F::Manager>,
        transactions: Option<&TransactionManager<F::Manager>>,
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
        context: &Context<F::Manager>,
        transaction_manager: Option<&TransactionManager<F::Manager>>,
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
        let mut file_length = context.file_manager.file_length(file_path)?;
        if file_length == 0 {
            active_state.header.initialize_default();
            return Ok(());
        }

        let excess_length = file_length % PAGE_SIZE as u64;
        if excess_length > 0 {
            // Truncate the file to the proper page size. This should only happen in a recovery situation.
            eprintln!(
                "Tree {:?} has {} extra bytes. Truncating.",
                file_path, excess_length
            );
            let file = OpenOptions::new()
                .append(true)
                .write(true)
                .open(&file_path)?;
            file_length -= excess_length;
            file.set_len(file_length)?;
            file.sync_all()?;
        }

        let mut tree = F::open_for_read(file_path, None)?;

        // Scan back block by block until we find a header page.
        let mut block_start = file_length - PAGE_SIZE as u64;
        let mut scratch_buffer = vec![0_u8];
        active_state.header = loop {
            // Read the page header
            tree.seek(SeekFrom::Start(block_start))?;
            tree.read_exact(&mut scratch_buffer[0..1])?;

            #[allow(clippy::match_on_vec_items)]
            match PageHeader::try_from(scratch_buffer[0])? {
                PageHeader::Continuation | PageHeader::Data => {
                    if block_start == 0 {
                        return Err(Error::data_integrity(format!(
                            "Tree {:?} contained data, but no valid pages were found",
                            file_path
                        )));
                    }
                    block_start -= PAGE_SIZE as u64;
                    continue;
                }
                header => {
                    if header != Root::HEADER {
                        return Err(Error::data_integrity(format!(
                            "Tree {:?} contained another header type",
                            file_path
                        )));
                    }
                    let contents = match read_chunk(
                        block_start + 1,
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
                                active_state.header.initialize_default();
                                return Ok(());
                            }
                            block_start -= PAGE_SIZE as u64;
                            continue;
                        }
                    }
                    break root;
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
        transaction_id: u64,
        key: Buffer<'static>,
        document: Buffer<'static>,
    ) -> Result<(), Error> {
        self.file.execute(DocumentWriter {
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            modification: Some(Modification {
                transaction_id,
                keys: vec![key],
                operation: Operation::Set(document),
            }),
            scratch: &mut self.scratch,
        })
    }

    /// Executes a modification.
    pub fn modify(&mut self, modification: Modification<'_, Buffer<'static>>) -> Result<(), Error> {
        self.file.execute(DocumentWriter {
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
        transaction_id: u64,
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
        transaction_id: u64,
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
        transaction_id: u64,
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
        self.file.execute(DocumentGetter {
            from_transaction: in_transaction,
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            keys: KeyRange::Single(key),
            key_reader: |_key, value| {
                buffer = Some(value);
                Ok(())
            },
            key_evaluator: |_| KeyEvaluation::ReadData,
        })?;
        Ok(buffer)
    }

    /// Gets the value stored for `key`.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn get_multiple(
        &mut self,
        keys: &[&[u8]],
        in_transaction: bool,
    ) -> Result<Vec<(Buffer<'static>, Buffer<'static>)>, Error> {
        let mut buffers = Vec::with_capacity(keys.len());
        self.file.execute(DocumentGetter {
            from_transaction: in_transaction,
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            keys: KeyRange::Multiple(keys),
            key_reader: |key, value| {
                buffers.push((key, value));
                Ok(())
            },
            key_evaluator: |_| KeyEvaluation::ReadData,
        })?;
        Ok(buffers)
    }

    /// Gets the value stored for `key`.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn get_range<'b, B: RangeBounds<Buffer<'b>> + Debug + 'static>(
        &mut self,
        range: B,
        in_transaction: bool,
    ) -> Result<Vec<(Buffer<'static>, Buffer<'static>)>, Error> {
        let mut results = Vec::new();
        match self.scan::<Infallible, _, _, _>(
            range,
            true,
            in_transaction,
            |_| KeyEvaluation::ReadData,
            |key, value| {
                results.push((key, value));
                Ok(())
            },
        ) {
            Ok(_) => Ok(results),
            Err(AbortError::Other(_)) => unreachable!(),
            Err(AbortError::Nebari(error)) => Err(error),
        }
    }

    /// Gets the value stored for `key`.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(self, key_evaluator, callback))
    )]
    pub fn scan<'b, E, B, KeyEvaluator, DataCallback>(
        &mut self,
        range: B,
        forwards: bool,
        in_transaction: bool,
        key_evaluator: KeyEvaluator,
        callback: DataCallback,
    ) -> Result<(), AbortError<E>>
    where
        B: RangeBounds<Buffer<'b>> + Debug + 'static,
        KeyEvaluator: FnMut(&Buffer<'static>) -> KeyEvaluation,
        DataCallback: FnMut(Buffer<'static>, Buffer<'static>) -> Result<(), AbortError<E>>,
        E: Display + Debug,
    {
        self.file.execute(DocumentScanner {
            forwards,
            from_transaction: in_transaction,
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            range,
            key_reader: callback,
            key_evaluator,
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
            |key| {
                result = Some(key.clone());
                KeyEvaluation::Stop
            },
            |_key, _value| Ok(()),
        )
        .map_err(AbortError::infallible)?;

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
            |_| {
                if key_requested {
                    KeyEvaluation::Stop
                } else {
                    key_requested = true;
                    KeyEvaluation::ReadData
                }
            },
            |key, value| {
                result = Some((key, value));
                Ok(())
            },
        )
        .map_err(AbortError::infallible)?;

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
        file_manager: &F::Manager,
        transactions: Option<TransactableCompaction<'_, F::Manager>>,
    ) -> Result<Self, Error> {
        let (compacted_path, finisher) = self.file.execute(TreeCompactor {
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            file_manager,
            transactions,
            scratch: &mut self.scratch,
        })?;
        self.file = self
            .file
            .replace_with(&compacted_path, file_manager, |file_id| {
                finisher.finish(file_id);
                println!("Published compaction.");
            })?;
        Ok(self)
    }
}

pub struct TransactableCompaction<'a, Manager: FileManager> {
    pub name: &'a str,
    pub manager: &'a TransactionManager<Manager>,
}

struct TreeCompactor<'a, Root: root::Root, M: FileManager> {
    state: &'a State<Root>,
    vault: Option<&'a dyn Vault>,
    cache: Option<&'a ChunkCache>,
    file_manager: &'a M,
    transactions: Option<TransactableCompaction<'a, M>>,
    scratch: &'a mut Vec<u8>,
}
impl<'a, Root: root::Root, F: ManagedFile> FileOp<F> for TreeCompactor<'a, Root, F::Manager> {
    type Output = Result<(PathBuf, TreeCompactionFinisher<'a, Root>), Error>;

    fn execute(&mut self, file: &mut F) -> Self::Output {
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
                .new_transaction(&[transactions.name.as_bytes()])
        });
        let mut new_file = F::open_for_append(&compacted_path, None)?;
        let mut writer = PagedWriter::new(PageHeader::Data, &mut new_file, self.vault, None, 0);

        // Use the read state to list all the currently live chunks
        let mut copied_chunks = HashMap::new();
        let read_state = self.state.read();
        let mut temporary_header = read_state.header.clone();
        drop(read_state);
        temporary_header.copy_data_to(false, file, &mut copied_chunks, &mut writer, self.vault)?;

        // Now, do the same with the write state, which should be very fast,
        // since only nodes that have changed will need to be visited.
        let mut write_state = self.state.lock();
        write_state
            .header
            .copy_data_to(true, file, &mut copied_chunks, &mut writer, self.vault)?;

        save_tree(&mut write_state, self.vault, None, writer, self.scratch)?;

        // Close any existing handles to the file. This ensures that once we
        // save the tree, new requests to the file manager will point to the new
        // file.

        Ok((
            compacted_path,
            TreeCompactionFinisher {
                transaction,
                write_state,
                state: self.state,
            },
        ))
    }
}

struct TreeCompactionFinisher<'a, Root: root::Root> {
    transaction: Option<TransactionHandle>,
    state: &'a State<Root>,
    write_state: MutexGuard<'a, ActiveState<Root>>,
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

impl<'a, Root: root::Root, F: ManagedFile> FileOp<F> for TreeWriter<'a, Root> {
    type Output = Result<(), Error>;
    fn execute(&mut self, file: &mut F) -> Self::Output {
        let mut active_state = self.state.lock();
        if active_state.file_id != file.id() {
            return Err(Error::from(ErrorKind::DatabaseCompacted));
        }
        if active_state.header.dirty() {
            let data_block = PagedWriter::new(
                PageHeader::Data,
                file,
                self.vault.as_deref(),
                self.cache,
                active_state.current_position,
            );

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

struct DocumentWriter<'a, 'm, Root: root::Root> {
    state: &'a State<Root>,
    vault: Option<&'a dyn Vault>,
    cache: Option<&'a ChunkCache>,
    modification: Option<Modification<'m, Buffer<'static>>>,
    scratch: &'a mut Vec<u8>,
}

impl<'a, 'm, Root: root::Root, F: ManagedFile> FileOp<F> for DocumentWriter<'a, 'm, Root> {
    type Output = Result<(), Error>;

    fn execute(&mut self, file: &mut F) -> Self::Output {
        let mut active_state = self.state.lock();
        if active_state.file_id != file.id() {
            return Err(Error::from(ErrorKind::DatabaseCompacted));
        }

        let mut data_block = PagedWriter::new(
            PageHeader::Data,
            file,
            self.vault.as_deref(),
            self.cache,
            active_state.current_position,
        );

        // Now that we have the document data's position, we can update the by_sequence and by_id indexes.
        let modification = self.modification.take().unwrap();
        let is_transactional = modification.transaction_id != 0;
        active_state.header.modify(modification, &mut data_block)?;

        if is_transactional {
            // Transactions will saved later.
            let (_, new_position) = data_block.finish()?;
            active_state.current_position = new_position;
        } else {
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
fn save_tree<Root: root::Root, F: ManagedFile>(
    active_state: &mut ActiveState<Root>,
    vault: Option<&dyn Vault>,
    cache: Option<&ChunkCache>,
    mut data_block: PagedWriter<'_, F>,
    scratch: &mut Vec<u8>,
) -> Result<(), Error> {
    scratch.clear();
    active_state.header.serialize(&mut data_block, scratch)?;
    let (file, after_data) = data_block.finish()?;
    active_state.current_position = after_data;

    // Write a new header.
    let mut header_block = PagedWriter::new(
        Root::HEADER,
        file,
        vault,
        cache,
        active_state.current_position,
    );
    header_block.write_chunk(scratch, false)?;

    let (file, after_header) = header_block.finish()?;
    active_state.current_position = after_header;

    file.flush()?;
    Ok(())
}

/// One or more keys.
#[derive(Debug)]
pub enum KeyRange<'a> {
    /// No keys.
    Empty,
    /// A single key.
    Single(&'a [u8]),
    /// A list of keys.
    Multiple(&'a [&'a [u8]]),
}

impl<'a> KeyRange<'a> {
    fn current_key(&self) -> Option<&'a [u8]> {
        match self {
            KeyRange::Single(key) => Some(*key),
            KeyRange::Multiple(keys) => keys.get(0).copied(),
            KeyRange::Empty => None,
        }
    }
}

impl<'a> Iterator for KeyRange<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<&'a [u8]> {
        match self {
            KeyRange::Empty => None,
            KeyRange::Single(key) => {
                let key = *key;
                *self = Self::Empty;
                Some(key)
            }
            KeyRange::Multiple(keys) => {
                if keys.is_empty() {
                    None
                } else {
                    let (one_key, remaining) = keys.split_at(1);
                    *keys = remaining;

                    Some(one_key[0])
                }
            }
        }
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

struct DocumentGetter<
    'a,
    'k,
    Root: root::Root,
    E: FnMut(&Buffer<'static>) -> KeyEvaluation,
    R: FnMut(Buffer<'static>, Buffer<'static>) -> Result<(), Error>,
> {
    from_transaction: bool,
    state: &'a State<Root>,
    vault: Option<&'a dyn Vault>,
    cache: Option<&'a ChunkCache>,
    keys: KeyRange<'k>,
    key_evaluator: E,
    key_reader: R,
}

impl<'a, 'k, E, R, Root: root::Root, F: ManagedFile> FileOp<F>
    for DocumentGetter<'a, 'k, Root, E, R>
where
    E: FnMut(&Buffer<'static>) -> KeyEvaluation,
    R: FnMut(Buffer<'static>, Buffer<'static>) -> Result<(), Error>,
{
    type Output = Result<(), Error>;
    fn execute(&mut self, file: &mut F) -> Self::Output {
        if self.from_transaction {
            let state = self.state.lock();
            if state.file_id != file.id() {
                return Err(Error::from(ErrorKind::DatabaseCompacted));
            }

            state.header.get_multiple(
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
                return Err(Error::from(ErrorKind::DatabaseCompacted));
            }

            state.header.get_multiple(
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

struct DocumentScanner<'a, 'k, E, Root: root::Root, KeyEvaluator, KeyReader, KeyRangeBounds>
where
    KeyEvaluator: FnMut(&Buffer<'static>) -> KeyEvaluation,
    KeyReader: FnMut(Buffer<'static>, Buffer<'static>) -> Result<(), AbortError<E>>,
    KeyRangeBounds: RangeBounds<Buffer<'k>>,
    E: Display + Debug,
{
    forwards: bool,
    from_transaction: bool,
    state: &'a State<Root>,
    vault: Option<&'a dyn Vault>,
    cache: Option<&'a ChunkCache>,
    range: KeyRangeBounds,
    key_evaluator: KeyEvaluator,
    key_reader: KeyReader,
    _phantom: PhantomData<&'k ()>,
}

impl<'a, 'k, E, Root: root::Root, KeyEvaluator, KeyReader, KeyRangeBounds, F> FileOp<F>
    for DocumentScanner<'a, 'k, E, Root, KeyEvaluator, KeyReader, KeyRangeBounds>
where
    F: ManagedFile,
    KeyEvaluator: FnMut(&Buffer<'static>) -> KeyEvaluation,
    KeyReader: FnMut(Buffer<'static>, Buffer<'static>) -> Result<(), AbortError<E>>,
    KeyRangeBounds: RangeBounds<Buffer<'k>> + Debug,
    E: Display + Debug,
{
    type Output = Result<(), AbortError<E>>;
    fn execute(&mut self, file: &mut F) -> Self::Output {
        if self.from_transaction {
            let state = self.state.lock();
            if state.file_id != file.id() {
                return Err(AbortError::Nebari(Error::from(
                    ErrorKind::DatabaseCompacted,
                )));
            }

            state.header.scan(
                &self.range,
                &mut ScanArgs::new(self.forwards, &mut self.key_evaluator, &mut self.key_reader),
                file,
                self.vault,
                self.cache,
            )
        } else {
            let state = self.state.read();
            if state.file_id != file.id() {
                return Err(AbortError::Nebari(Error::from(
                    ErrorKind::DatabaseCompacted,
                )));
            }

            state.header.scan(
                &self.range,
                &mut ScanArgs::new(self.forwards, &mut self.key_evaluator, &mut self.key_reader),
                file,
                self.vault,
                self.cache,
            )
        }
    }
}

/// Writes data in pages, allowing for quick scanning through the file.
pub struct PagedWriter<'a, F: ManagedFile> {
    file: &'a mut F,
    vault: Option<&'a dyn Vault>,
    cache: Option<&'a ChunkCache>,
    position: u64,
    scratch: [u8; PAGE_SIZE * PAGED_WRITER_BATCH_COUNT],
    offset: usize,
}

impl<'a, F: ManagedFile> Deref for PagedWriter<'a, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        self.file
    }
}

impl<'a, F: ManagedFile> DerefMut for PagedWriter<'a, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.file
    }
}

impl<'a, F: ManagedFile> PagedWriter<'a, F> {
    fn new(
        header: PageHeader,
        file: &'a mut F,
        vault: Option<&'a dyn Vault>,
        cache: Option<&'a ChunkCache>,
        position: u64,
    ) -> PagedWriter<'a, F> {
        let mut writer = Self {
            file,
            vault,
            cache,
            position,
            scratch: [0; PAGE_SIZE * PAGED_WRITER_BATCH_COUNT],
            offset: 0,
        };
        writer.scratch[0] = header as u8;
        for page_num in 1..PAGED_WRITER_BATCH_COUNT {
            writer.scratch[page_num * PAGE_SIZE] = PageHeader::Continuation as u8;
        }
        writer.offset = 1;
        writer
    }

    fn current_position(&self) -> u64 {
        self.position + self.offset as u64
    }

    fn write(&mut self, data: &[u8]) -> Result<usize, Error> {
        let bytes_written = data.len();
        self.commit_if_needed(true)?;

        let new_offset = self.offset + data.len();
        let start_page = self.offset / PAGE_SIZE;
        let end_page = (new_offset - 1) / PAGE_SIZE;
        if start_page == end_page && new_offset <= self.scratch.len() {
            // Everything fits within the current page
            self.scratch[self.offset..new_offset].copy_from_slice(data);
            self.offset = new_offset;
        } else {
            // This won't fully fit within the page remainder. First, fill the remainder of the page
            let page_remaining = PAGE_SIZE - self.offset % PAGE_SIZE;
            let (fill_amount, mut remaining) = data.split_at(page_remaining);
            if !fill_amount.is_empty() {
                self.scratch[self.offset..self.offset + fill_amount.len()]
                    .copy_from_slice(fill_amount);
                self.offset += fill_amount.len();
            }

            // If the data is large enough to span multiple pages, continue to do so.
            while remaining.len() >= PAGE_SIZE {
                self.commit_if_needed(true)?;

                let (one_page, after) = remaining.split_at(PAGE_SIZE - 1);
                remaining = after;

                self.scratch[self.offset..self.offset + PAGE_SIZE - 1].copy_from_slice(one_page);
                self.offset += PAGE_SIZE - 1;
            }

            self.commit_if_needed(!remaining.is_empty())?;

            // If there's any data left, add it to the scratch
            if !remaining.is_empty() {
                let final_offset = self.offset + remaining.len();
                self.scratch[self.offset..final_offset].copy_from_slice(remaining);
                self.offset = final_offset;
            }

            self.commit_if_needed(!remaining.is_empty())?;
        }
        Ok(bytes_written)
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
        let mut position = self.current_position();
        // Ensure that the chunk header can be read contiguously
        let position_relative_to_page = position % PAGE_SIZE as u64;
        if position_relative_to_page + 8 > PAGE_SIZE as u64 {
            // Write the number of zeroes required to pad the current position
            // such that our header's 8 bytes will not be interrupted by a page
            // header.
            let bytes_needed = if position_relative_to_page == 0 {
                1
            } else {
                PAGE_SIZE - position_relative_to_page as usize
            };
            let zeroes = [0; 8];
            self.write(&zeroes[0..bytes_needed])?;
            position = self.current_position();
        }

        if position % PAGE_SIZE as u64 == 0 {
            // A page header will be written before our first byte.
            position += 1;
        }

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

    /// Writes the scratch buffer and resets `offset`.
    #[cfg_attr(not(debug_assertions), allow(unused_mut))]
    fn commit(&mut self) -> Result<(), Error> {
        let length = (self.offset + PAGE_SIZE - 1) / PAGE_SIZE * PAGE_SIZE;

        // In debug builds, fill the padding with a recognizable number: the
        // answer to the ultimate question of life, the universe and everything.
        // For refence, 42 in hex is 0x2A.
        #[cfg(debug_assertions)]
        if self.offset < length {
            self.scratch[self.offset..length].fill(42);
        }

        self.file.seek(SeekFrom::Start(self.position))?;
        self.file.write_all(&self.scratch[..length])?;
        self.position += length as u64;

        // Set the header to be a continuation block
        self.scratch[0] = PageHeader::Continuation as u8;
        self.offset = 1;
        Ok(())
    }

    fn commit_if_needed(&mut self, about_to_write: bool) -> Result<(), Error> {
        if self.offset == self.scratch.len() {
            self.commit()?;
        } else if about_to_write && self.offset % PAGE_SIZE == 0 {
            self.offset += 1;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<(&'a mut F, u64), Error> {
        if self.offset > 1 {
            self.commit()?;
        }
        Ok((self.file, self.position))
    }
}

#[allow(clippy::cast_possible_truncation)]
#[cfg_attr(feature = "tracing", tracing::instrument(skip(file, vault, cache)))]
fn read_chunk<F: ManagedFile>(
    position: u64,
    validate_crc: bool,
    file: &mut F,
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

    let mut data_start = position + 8;
    // If the data starts on a page boundary, there will have been a page
    // boundary inserted. Note: We don't read this byte, so technically it's
    // unchecked as part of this process.
    if data_start % PAGE_SIZE as u64 == 0 {
        data_start += 1;
        file.seek(SeekFrom::Current(1))?;
    }

    let data_start_relative_to_page = data_start % PAGE_SIZE as u64;
    let data_end_relative_to_page = data_start_relative_to_page + length as u64;
    // Minus 2 may look like code cruft, but it's due to the correct value being
    // `data_end_relative_to_page as usize - 1`, except that if a write occurs
    // that ends exactly on a page boundary, we omit the boundary.
    let number_of_page_boundaries = (data_end_relative_to_page as usize - 2) / (PAGE_SIZE - 1);

    let data_page_start = data_start - data_start % PAGE_SIZE as u64;
    let total_bytes_to_read = length + number_of_page_boundaries;
    let mut scratch = Vec::new();
    scratch.resize(total_bytes_to_read, 0);
    file.read_exact(&mut scratch)?;

    // We need to remove the `PageHeader::Continuation` bytes before continuing.
    let first_page_relative_end = data_page_start + PAGE_SIZE as u64 - data_start;
    for page in 0..number_of_page_boundaries {
        let write_start = first_page_relative_end as usize + page * (PAGE_SIZE - 1);
        let read_start = write_start + page + 1;
        let length = (length - write_start).min(PAGE_SIZE - 1);
        scratch.copy_within(read_start..read_start + length, write_start);
    }
    scratch.truncate(length);

    // This is an extra sanity check on the above algorithm, but given the
    // thoroughness of the unit test around this functionality, it's only
    // checked in debug mode. The CRC will still fail on bad reads, but noticing
    // the length is incorrect is a sign that the byte removal loop is bad.
    debug_assert_eq!(scratch.len(), length);

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

pub(crate) fn copy_chunk<F: ManagedFile, S: BuildHasher>(
    original_position: u64,
    from_file: &mut F,
    copied_chunks: &mut std::collections::HashMap<u64, u64, S>,
    to_file: &mut PagedWriter<'_, F>,
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use nanorand::{Pcg64, Rng};

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
            PagedWriter::new(PageHeader::VersionedHeader, &mut file, None, None, 0);

        let mut scratch = Vec::new();
        scratch.resize(offset.max(length), 0);
        if offset > 0 {
            paged_writer.write(&scratch[..offset])?;
        }
        scratch.fill(1);
        let written_position = paged_writer.write_chunk(&scratch[..length], false)?;
        drop(paged_writer.finish()?);

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
    ) {
        let id = loop {
            let id = rng.generate::<u64>();
            if ids.insert(id) {
                break id;
            }
        };
        let id_buffer = Buffer::from(id.to_be_bytes().to_vec());
        {
            let state = if ids.len() > 1 {
                let state = State::default();
                TreeFile::<R, F>::initialize_state(&state, file_path, None, context, None).unwrap();
                state
            } else {
                State::initialized()
            };
            let file = context.file_manager.append(file_path).unwrap();
            let mut tree =
                TreeFile::<R, F>::new(file, state, context.vault.clone(), context.cache.clone())
                    .unwrap();
            tree.push(0, id_buffer.clone(), Buffer::from(b"hello world"))
                .unwrap();

            // This shouldn't have to scan the file, as the data fits in memory.
            let value = tree.get(&id_buffer, false).unwrap();
            assert_eq!(&*value.unwrap(), b"hello world");
        }

        // Try loading the file up and retrieving the data.
        {
            let state = State::default();
            TreeFile::<R, F>::initialize_state(&state, file_path, None, context, None).unwrap();

            let file = context.file_manager.append(file_path).unwrap();
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
    ) {
        let id_buffer = Buffer::from(id.to_be_bytes().to_vec());
        {
            let file = context.file_manager.append(file_path).unwrap();
            let state = State::default();
            TreeFile::<R, F>::initialize_state(&state, file_path, file.id(), context, None)
                .unwrap();
            let mut tree =
                TreeFile::<R, F>::new(file, state, context.vault.clone(), context.cache.clone())
                    .unwrap();
            tree.modify(Modification {
                transaction_id: 0,
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
                &context, &file_path, &mut ids, &mut rng,
            );
        }
        println!("Successfully inserted up to ORDER - 1 nodes.");

        // The next record will split the node
        insert_one_record::<VersionedTreeRoot, StdFile>(&context, &file_path, &mut ids, &mut rng);
        println!("Successfully introduced one layer of depth.");

        // Insert a lot more.
        for _ in 0..1_000 {
            insert_one_record::<VersionedTreeRoot, StdFile>(
                &context, &file_path, &mut ids, &mut rng,
            );
        }
    }

    fn remove<R: Root>(label: &str) {
        let mut rng = Pcg64::new_seed(1);
        let context = Context {
            file_manager: StdFileManager::default(),
            vault: None,
            cache: None,
        };
        let temp_dir = crate::test_util::TestDirectory::new(format!("btree-removals-{}", label));
        std::fs::create_dir(&temp_dir).unwrap();
        let file_path = temp_dir.join("tree");
        let mut ids = HashSet::new();
        // Insert up to the limit of a LEAF, which is ORDER - 1.
        for _ in 0..1000 {
            insert_one_record::<R, StdFile>(&context, &file_path, &mut ids, &mut rng);
        }

        let mut ids = ids.into_iter().collect::<Vec<_>>();

        for i in 0..ids.len() {
            let random = rng.generate_range(i..ids.len());
            if random != i {
                ids.swap(i, random);
            }
        }

        // Remove each of the records
        for id in ids {
            remove_one_record::<R, StdFile>(&context, &file_path, id);
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
        let file = context.file_manager.append(file_path).unwrap();
        let mut tree = TreeFile::<VersionedTreeRoot, F>::new(
            file,
            state,
            context.vault.clone(),
            context.cache.clone(),
        )
        .unwrap();
        for (_index, id) in ids.enumerate() {
            let id_buffer = Buffer::from(id.to_be_bytes().to_vec());
            tree.push(0, id_buffer.clone(), Buffer::from(b"hello world"))
                .unwrap();
        }
    }

    #[test]
    fn bulk_insert_tokio() {
        bulk_insert::<StdFile>("std");
    }

    fn bulk_insert<F: ManagedFile>(name: &str) {
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
        let file = context.file_manager.append(file_path).unwrap();
        let mut tree = TreeFile::<VersionedTreeRoot, F>::new(
            file,
            state,
            context.vault.clone(),
            context.cache.clone(),
        )
        .unwrap();
        for _ in 0..BATCHES {
            let mut ids = (0..RECORDS_PER_BATCH)
                .map(|_| rng.generate::<u64>())
                .collect::<Vec<_>>();
            ids.sort_unstable();
            let modification = Modification {
                transaction_id: 0,
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
        let file = context.file_manager.append("test").unwrap();
        let mut tree = TreeFile::<VersionedTreeRoot, MemoryFile>::new(
            file,
            state,
            context.vault.clone(),
            context.cache.clone(),
        )
        .unwrap();
        // Create enough records to go 4 levels deep.
        let mut ids = Vec::new();
        for id in 0..3_u32.pow(4) {
            let id_buffer = Buffer::from(id.to_be_bytes().to_vec());
            tree.push(0, id_buffer.clone(), id_buffer.clone()).unwrap();
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
            insert_one_record::<R, StdFile>(&context, &file_path, &mut ids, &mut rng);
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
}
