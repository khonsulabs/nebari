use std::{
    borrow::Cow,
    cmp::Ordering,
    convert::TryFrom,
    fs::OpenOptions,
    io::{SeekFrom, Write},
    marker::PhantomData,
    ops::{Bound, RangeBounds},
    path::Path,
    sync::Arc,
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use super::{State, TransactionHandle};
use crate::{
    error::Error,
    io::{FileManager, FileOp, ManagedFile, OpenableFile},
    vault::AnyVault,
    Buffer, Context, ErrorKind,
};

const PAGE_SIZE: usize = 1024;

/// A transaction log that records changes for one or more trees.
pub struct TransactionLog<File: ManagedFile> {
    vault: Option<Arc<dyn AnyVault>>,
    state: State,
    log: <File::Manager as FileManager>::FileHandle,
}

impl<File: ManagedFile> TransactionLog<File> {
    /// Opens a transaction log for reading.
    pub fn read(
        log_path: &Path,
        state: State,
        context: Context<File::Manager>,
    ) -> Result<Self, Error> {
        let log = context.file_manager.read(log_path)?;
        Ok(Self {
            vault: context.vault,
            state,
            log,
        })
    }

    /// Opens a transaction log for writing.
    pub fn open(
        log_path: &Path,
        state: State,
        context: Context<File::Manager>,
    ) -> Result<Self, Error> {
        let log = context.file_manager.append(log_path)?;
        Ok(Self {
            vault: context.vault,
            state,
            log,
        })
    }

    /// Returns the total size of the transaction log file.
    pub fn total_size(&self) -> u64 {
        let state = self.state.lock_for_write();
        *state
    }

    /// Initializes `state` to contain the information about the transaction log
    /// located at `log_path`.
    pub fn initialize_state(state: &State, context: &Context<File::Manager>) -> Result<(), Error> {
        let mut log_length = if context.file_manager.exists(state.path())? {
            context.file_manager.file_length(state.path())?
        } else {
            0
        };
        if log_length == 0 {
            state.initialize(1, 0);
            return Ok(());
        }

        let excess_length = log_length % PAGE_SIZE as u64;
        if excess_length > 0 {
            // Truncate the file to the proper page size. This should only happen in a recovery situation.
            eprintln!(
                "Transaction log has {} extra bytes. Truncating.",
                excess_length
            );
            let file = OpenOptions::new()
                .append(true)
                .write(true)
                .open(state.path())?;
            log_length -= excess_length;
            file.set_len(log_length)?;
            file.sync_all()?;
        }

        let mut file = context.file_manager.read(state.path())?;
        file.execute(StateInitializer {
            state,
            log_length,
            vault: context.vault(),
            _file: PhantomData,
        })
    }

    /// Logs one or more transactions. After this call returns, the transaction
    /// log is guaranteed to be fully written to disk.
    pub fn push(&mut self, handles: Vec<LogEntry<'static>>) -> Result<(), Error> {
        self.log.execute(LogWriter {
            state: self.state.clone(),
            vault: self.vault.clone(),
            transactions: handles,
            _file: PhantomData,
        })
    }

    /// Returns the executed transaction with the id provided. Returns None if not found.
    pub fn get(&mut self, id: u64) -> Result<Option<LogEntry<'static>>, Error> {
        match self.log.execute(EntryFetcher {
            id,
            state: &self.state,
            vault: self.vault.as_deref(),
        })? {
            ScanResult::Found { entry, .. } => Ok(Some(entry)),
            ScanResult::NotFound { .. } => Ok(None),
        }
    }

    /// Logs one or more transactions. After this call returns, the transaction
    /// log is guaranteed to be fully written to disk.
    pub fn scan<Callback: FnMut(LogEntry<'static>) -> bool>(
        &mut self,
        ids: impl RangeBounds<u64>,
        callback: Callback,
    ) -> Result<(), Error> {
        self.log.execute(EntryScanner {
            ids,
            callback,
            state: &self.state,
            vault: self.vault.as_deref(),
        })
    }

    /// Closes the transaction log.
    pub fn close(self) -> Result<(), Error> {
        self.log.close()
    }

    /// Returns the current transaction id.
    pub fn current_transaction_id(&self) -> u64 {
        self.state.next_transaction_id()
    }

    /// Begins a new transaction, exclusively locking `trees`.
    pub fn new_transaction<
        'a,
        I: IntoIterator<Item = &'a [u8], IntoIter = II>,
        II: ExactSizeIterator<Item = &'a [u8]>,
    >(
        &self,
        trees: I,
    ) -> TransactionHandle {
        self.state.new_transaction(trees)
    }

    /// Returns the current state of the log.
    pub fn state(&self) -> State {
        self.state.clone()
    }
}

struct StateInitializer<'a, F> {
    state: &'a State,
    log_length: u64,
    vault: Option<&'a dyn AnyVault>,
    _file: PhantomData<F>,
}

impl<'a, F: ManagedFile> FileOp<F> for StateInitializer<'a, F> {
    type Output = Result<(), Error>;
    fn execute(self, log: &mut F) -> Result<(), Error> {
        // Scan back block by block until we find a page header with a value of 1.
        let block_start = self.log_length - PAGE_SIZE as u64;
        let mut scratch_buffer = Vec::new();
        scratch_buffer.reserve(PAGE_SIZE);
        scratch_buffer.resize(4, 0);

        let last_transaction =
            match scan_for_transaction(log, &mut scratch_buffer, block_start, false, self.vault)? {
                ScanResult::Found { entry, .. } => entry,
                ScanResult::NotFound { .. } => {
                    return Err(Error::data_integrity(
                        "No entries found in an existing transaction log",
                    ))
                }
            };

        self.state
            .initialize(last_transaction.id + 1, self.log_length);
        Ok(())
    }
}

pub enum ScanResult {
    Found {
        entry: LogEntry<'static>,
        position: u64,
        length: u64,
    },
    NotFound {
        nearest_position: u64,
    },
}

fn scan_for_transaction<File: ManagedFile>(
    log: &mut File,
    scratch_buffer: &mut Vec<u8>,
    mut block_start: u64,
    scan_forward: bool,
    vault: Option<&dyn AnyVault>,
) -> Result<ScanResult, Error> {
    if scratch_buffer.len() < 4 {
        scratch_buffer.resize(4, 0);
    }
    let file_length = log.length()?;
    Ok(loop {
        if block_start >= file_length {
            return Ok(ScanResult::NotFound {
                nearest_position: block_start,
            });
        }
        log.seek(SeekFrom::Start(block_start))?;
        // Read the page header
        log.read_exact(&mut scratch_buffer[0..4])?;
        #[allow(clippy::match_on_vec_items)]
        match scratch_buffer[0] {
            0 => {
                if block_start == 0 {
                    break ScanResult::NotFound {
                        nearest_position: 0,
                    };
                }
                if scan_forward {
                    block_start += PAGE_SIZE as u64;
                } else {
                    block_start -= PAGE_SIZE as u64;
                }
                continue;
            }
            1 => {
                // The length is the next 3 bytes.
                let length = (scratch_buffer[1] as usize) << 16
                    | (scratch_buffer[2] as usize) << 8
                    | scratch_buffer[3] as usize;
                if scratch_buffer.len() < length {
                    scratch_buffer.resize(length, 0);
                }
                log.read_exact(scratch_buffer)?;
                let payload = &scratch_buffer[0..length];
                let decrypted = match &vault {
                    Some(vault) => Cow::Owned(vault.decrypt(payload)?),
                    None => Cow::Borrowed(payload),
                };
                let entry = LogEntry::deserialize(&decrypted)
                    .map_err(Error::data_integrity)?
                    .into_owned();
                break ScanResult::Found {
                    entry,
                    position: block_start,
                    length: length as u64,
                };
            }
            _ => unreachable!("corrupt transaction log"),
        }
    })
}

#[allow(clippy::redundant_pub_crate)]
pub(crate) struct EntryFetcher<'a> {
    pub state: &'a State,
    pub id: u64,
    pub vault: Option<&'a dyn AnyVault>,
}

impl<'a, File: ManagedFile> FileOp<File> for EntryFetcher<'a> {
    type Output = Result<ScanResult, Error>;
    fn execute(self, log: &mut File) -> Result<ScanResult, Error> {
        let mut scratch = Vec::with_capacity(PAGE_SIZE);
        fetch_entry(log, &mut scratch, self.state, self.id, self.vault)
    }
}

fn fetch_entry<File: ManagedFile>(
    log: &mut File,
    scratch_buffer: &mut Vec<u8>,
    state: &State,
    id: u64,
    vault: Option<&dyn AnyVault>,
) -> Result<ScanResult, Error> {
    if id == 0 {
        return Ok(ScanResult::NotFound {
            nearest_position: 0,
        });
    }

    let mut upper_id = state.next_transaction_id();
    let mut upper_location = state.len();
    if upper_id <= id {
        return Ok(ScanResult::NotFound {
            nearest_position: upper_location,
        });
    }
    let mut lower_id = None;
    let mut lower_location = None;
    loop {
        let guessed_location = if let Some(page) =
            guess_page(id, lower_location, lower_id, upper_location, upper_id)
        {
            page
        } else {
            return Ok(ScanResult::NotFound {
                nearest_position: upper_location,
            });
        };
        if guessed_location == upper_location {
            return Ok(ScanResult::NotFound {
                nearest_position: upper_location,
            });
        }

        // load the transaction at this location
        #[allow(clippy::cast_possible_wrap)]
        let scan_forward = guessed_location >= upper_location;
        match scan_for_transaction(log, scratch_buffer, guessed_location, scan_forward, vault)? {
            ScanResult::Found {
                entry,
                position,
                length,
            } => {
                state.note_transaction_id_status(entry.id, Some(position));
                match entry.id.cmp(&id) {
                    Ordering::Less => {
                        lower_id = Some(entry.id);
                        lower_location = Some(position);
                    }
                    Ordering::Equal => {
                        return Ok(ScanResult::Found {
                            entry,
                            position,
                            length,
                        });
                    }
                    Ordering::Greater => {
                        upper_id = entry.id;
                        upper_location = position;
                    }
                }
            }
            ScanResult::NotFound { nearest_position } => {
                return Ok(ScanResult::NotFound { nearest_position });
            }
        }
    }
}

pub struct EntryScanner<'a, Range: RangeBounds<u64>, Callback: FnMut(LogEntry<'static>) -> bool> {
    pub state: &'a State,
    pub ids: Range,
    pub vault: Option<&'a dyn AnyVault>,
    pub callback: Callback,
}

impl<
        'a,
        Range: RangeBounds<u64>,
        File: ManagedFile,
        Callback: FnMut(LogEntry<'static>) -> bool,
    > FileOp<File> for EntryScanner<'a, Range, Callback>
{
    type Output = Result<(), Error>;
    fn execute(mut self, log: &mut File) -> Self::Output {
        let mut scratch = Vec::with_capacity(PAGE_SIZE);
        let (start_location, start_transaction, start_length) = match self.ids.start_bound() {
            Bound::Included(start_key) | Bound::Excluded(start_key) => {
                match fetch_entry(log, &mut scratch, self.state, *start_key, self.vault)? {
                    ScanResult::Found {
                        entry,
                        position,
                        length,
                    } => (position, Some(entry), length),
                    ScanResult::NotFound { nearest_position } => (nearest_position, None, 0),
                }
            }
            Bound::Unbounded => (0, None, 0),
        };

        if let Some(entry) = start_transaction {
            if self.ids.contains(&entry.id) && !(self.callback)(entry) {
                return Ok(());
            }
        }

        // Continue scanning from this location forward, starting at the next page boundary after the starting transaction
        let mut next_scan_start = next_page_start(start_location + start_length);
        while let ScanResult::Found {
            entry,
            position,
            length,
        } = scan_for_transaction(log, &mut scratch, next_scan_start, true, self.vault)?
        {
            if self.ids.contains(&entry.id) && !(self.callback)(entry) {
                break;
            }
            next_scan_start = next_page_start(position + length);
        }

        Ok(())
    }
}

const fn next_page_start(position: u64) -> u64 {
    let page_size = PAGE_SIZE as u64;
    (position + page_size - 1) / page_size * page_size
}

struct LogWriter<File> {
    state: State,
    transactions: Vec<LogEntry<'static>>,
    vault: Option<Arc<dyn AnyVault>>,
    _file: PhantomData<File>,
}

impl<File: ManagedFile> FileOp<File> for LogWriter<File> {
    type Output = Result<(), Error>;
    fn execute(mut self, log: &mut File) -> Result<(), Error> {
        let mut log_position = self.state.lock_for_write();
        let mut scratch = [0_u8; PAGE_SIZE];
        let mut completed_transactions = Vec::with_capacity(self.transactions.len());
        for transaction in self.transactions.drain(..) {
            completed_transactions.push((transaction.id, Some(*log_position)));
            let mut bytes = transaction.serialize()?;
            if let Some(vault) = &self.vault {
                bytes = vault.encrypt(&bytes)?;
            }
            // Write out the transaction in pages.
            let total_length = bytes.len() + 3;
            let mut offset = 0;
            while offset < bytes.len() {
                // Write the page header
                let header_len = if offset == 0 {
                    // The first page has the length of the payload as the next 3 bytes.
                    let length = u32::try_from(bytes.len())
                        .map_err(|_| Error::from("transaction too large"))?;
                    if length & 0xFF00_0000 != 0 {
                        return Err(Error::from("transaction too large"));
                    }
                    scratch[0] = 1;
                    #[allow(clippy::cast_possible_truncation)]
                    {
                        scratch[1] = (length >> 16) as u8;
                        scratch[2] = (length >> 8) as u8;
                        scratch[3] = (length & 0xFF) as u8;
                    }
                    4
                } else {
                    // Set page_header to have a 0 byte for future pages written.
                    scratch[0] = 0;
                    1
                };

                // Write up to PAGE_SIZE - header_len bytes
                let total_bytes_left = total_length - (offset + 3);
                let bytes_to_write = total_bytes_left.min(PAGE_SIZE - header_len as usize);
                scratch[header_len..bytes_to_write + header_len]
                    .copy_from_slice(&bytes[offset..offset + bytes_to_write]);
                log.write_all(&scratch)?;
                offset += bytes_to_write;
                *log_position += PAGE_SIZE as u64;
            }
        }

        drop(log_position);

        log.flush()?;

        self.state
            .note_transaction_ids_completed(&completed_transactions);

        Ok(())
    }
}

/// An entry in a transaction log.
#[derive(Eq, PartialEq, Debug)]
pub struct LogEntry<'a> {
    /// The unique id of this entry.
    pub id: u64,
    pub(crate) data: Option<Buffer<'a>>,
}

impl<'a> LogEntry<'a> {
    /// Convert this entry into a `'static` lifetime.
    #[must_use]
    pub fn into_owned(self) -> LogEntry<'static> {
        LogEntry {
            id: self.id,
            data: self.data.as_ref().map(Buffer::to_owned),
        }
    }
}

impl<'a> LogEntry<'a> {
    /// Returns the associated data, if any.
    #[must_use]
    pub const fn data(&self) -> Option<&Buffer<'a>> {
        self.data.as_ref()
    }

    /// Sets the associated data that will be stored in the transaction log.
    /// Limited to a length 16,777,208 (2^24 - 8) bytes -- just shy of 16MB.
    pub fn set_data(&mut self, data: impl Into<Buffer<'a>>) -> Result<(), Error> {
        let data = data.into();
        if data.len() <= 2_usize.pow(24) - 8 {
            self.data = Some(data);
            Ok(())
        } else {
            Err(Error::from(ErrorKind::ValueTooLarge))
        }
    }

    pub(crate) fn serialize(&self) -> Result<Vec<u8>, Error> {
        let mut buffer = Vec::new();
        // Transaction ID
        buffer.write_u64::<BigEndian>(self.id)?;
        if let Some(data) = &self.data {
            // The rest of the entry is the data. Since the header of the log entry
            // contains the length, we don't need to waste space encoding it again.
            buffer.write_all(data)?;
        }

        Ok(buffer)
    }

    pub(crate) fn deserialize(mut buffer: &'a [u8]) -> Result<Self, Error> {
        let id = buffer.read_u64::<BigEndian>()?;
        let data = if buffer.is_empty() {
            None
        } else {
            Some(Buffer::from(buffer))
        };
        Ok(Self { id, data })
    }
}

#[test]
fn serialization_tests() {
    let transaction = LogEntry {
        id: 1,
        data: Some(Buffer::from(b"hello")),
    };
    let serialized = transaction.serialize().unwrap();
    let deserialized = LogEntry::deserialize(&serialized).unwrap();
    assert_eq!(transaction, deserialized);

    let transaction = LogEntry {
        id: u64::MAX,
        data: None,
    };
    let serialized = transaction.serialize().unwrap();
    let deserialized = LogEntry::deserialize(&serialized).unwrap();
    assert_eq!(transaction, deserialized);

    // Test the data length limits
    let mut transaction = LogEntry { id: 0, data: None };
    let mut big_data = Vec::new();
    big_data.resize(2_usize.pow(24), 0);
    let mut big_data = Buffer::from(big_data);
    assert!(matches!(
        transaction.set_data(big_data.clone()),
        Err(Error {
            kind: ErrorKind::ValueTooLarge,
            ..
        })
    ));

    // Remove 8 bytes (the transaction id length) and try again.
    let big_data = big_data.read_bytes(big_data.len() - 8).unwrap();
    transaction.set_data(big_data).unwrap();
    let serialized = transaction.serialize().unwrap();
    let deserialized = LogEntry::deserialize(&serialized).unwrap();
    assert_eq!(transaction, deserialized);
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn guess_page(
    looking_for: u64,
    lower_location: Option<u64>,
    lower_id: Option<u64>,
    upper_location: u64,
    upper_id: u64,
) -> Option<u64> {
    debug_assert_ne!(looking_for, upper_id);
    let total_pages = upper_location / PAGE_SIZE as u64;

    if let (Some(lower_location), Some(lower_id)) = (lower_location, lower_id) {
        // Estimate inbetween lower and upper
        let current_page = lower_location / PAGE_SIZE as u64;
        let delta_from_current = looking_for - lower_id;
        let local_avg_per_page = (upper_id - lower_id) as f64 / (total_pages - current_page) as f64;
        let delta_estimated_pages = (delta_from_current as f64 * local_avg_per_page).floor() as u64;
        let guess = lower_location + delta_estimated_pages.max(1) * PAGE_SIZE as u64;
        // If our estimate is that the location is beyond or equal to the upper, we'll guess the page before it.
        Some(if guess >= upper_location {
            upper_location - PAGE_SIZE as u64
        } else {
            guess
        })
    } else if upper_id > looking_for {
        // Go backwards from upper
        let avg_per_page = upper_id as f64 / total_pages as f64;
        let id_delta = upper_id - looking_for;
        let delta_estimated_pages = (id_delta as f64 * avg_per_page).ceil() as u64;
        let delta_bytes = delta_estimated_pages.saturating_mul(PAGE_SIZE as u64);
        Some(upper_location.saturating_sub(delta_bytes))
    } else {
        None
    }
}

#[cfg(test)]
#[allow(clippy::semicolon_if_nothing_returned, clippy::future_not_send)]
mod tests {

    use nanorand::{Pcg64, Rng};
    use tempfile::tempdir;

    use super::*;
    use crate::{
        io::{
            fs::{StdFile, StdFileManager},
            memory::MemoryFile,
            ManagedFile,
        },
        test_util::RotatorVault,
        transaction::TransactionManager,
        ChunkCache,
    };

    #[test]
    fn file_log_file_tests() {
        log_file_tests::<StdFile>("file_log_file", None, None);
    }

    #[test]
    fn memory_log_file_tests() {
        log_file_tests::<MemoryFile>("memory_log_file", None, None);
    }

    fn log_file_tests<File: ManagedFile>(
        file_name: &str,
        vault: Option<Arc<dyn AnyVault>>,
        cache: Option<ChunkCache>,
    ) {
        let temp_dir = crate::test_util::TestDirectory::new(file_name);
        let file_manager = <File::Manager as Default>::default();
        let context = Context {
            file_manager,
            vault,
            cache,
        };
        std::fs::create_dir(&temp_dir).unwrap();
        let log_path = {
            let directory: &Path = &temp_dir;
            directory.join("_transactions")
        };

        for id in 1..=1_000 {
            let state = State::from_path(&log_path);
            TransactionLog::<File>::initialize_state(&state, &context).unwrap();
            let mut transactions =
                TransactionLog::<File>::open(&log_path, state, context.clone()).unwrap();
            assert_eq!(transactions.current_transaction_id(), id);
            let mut tx = transactions.new_transaction([&b"hello"[..]]);

            tx.transaction.data = Some(Buffer::from(id.to_be_bytes()));

            transactions.push(vec![tx.transaction]).unwrap();
            transactions.close().unwrap();
        }

        if context.vault.is_some() {
            // Test that we can't open it without encryption
            let state = State::from_path(&temp_dir);
            assert!(TransactionLog::<File>::initialize_state(&state, &context).is_err());
            let mut transactions = TransactionLog::<File>::open(&log_path, state, context).unwrap();

            for id in 0..1_000 {
                let transaction = transactions.get(id).unwrap();
                match transaction {
                    Some(transaction) => assert_eq!(transaction.id, id),
                    None => {
                        unreachable!("failed to fetch transaction {}", id)
                    }
                }
            }

            // Test scanning
            let mut first_ten = Vec::new();
            transactions
                .scan(.., |entry| {
                    first_ten.push(entry);
                    first_ten.len() < 10
                })
                .unwrap();
            assert_eq!(first_ten.len(), 10);
            let mut after_first = None;
            transactions
                .scan(first_ten[0].id + 1.., |entry| {
                    after_first = Some(entry);
                    false
                })
                .unwrap();
            assert_eq!(after_first.as_ref(), first_ten.get(1));
        }
    }

    #[test]
    fn discontiguous_log_file_tests() {
        let temp_dir = tempdir().unwrap();
        let file_manager = StdFileManager::default();
        let context = Context {
            file_manager,
            vault: None,
            cache: None,
        };
        let log_path = temp_dir.path().join("_transactions");
        let mut rng = Pcg64::new_seed(1);

        let state = State::from_path(&log_path);
        TransactionLog::<StdFile>::initialize_state(&state, &context).unwrap();
        let mut transactions = TransactionLog::<StdFile>::open(&log_path, state, context).unwrap();

        let mut valid_ids = Vec::new();
        for id in 1..=10_000 {
            assert_eq!(transactions.current_transaction_id(), id);
            let tx = transactions.new_transaction([&b"hello"[..]]);
            if rng.generate::<u8>() < 8 {
                // skip a few ids.
                continue;
            }
            valid_ids.push(tx.id);

            transactions.push(vec![tx.transaction]).unwrap();
        }

        for id in valid_ids {
            let transaction = transactions.get(id).unwrap();
            match transaction {
                Some(transaction) => assert_eq!(transaction.id, id),
                None => {
                    unreachable!("failed to fetch transaction {}", id)
                }
            }
        }
    }

    #[test]
    fn file_log_manager_tests() {
        log_manager_tests::<StdFile>("file_log_manager", None, None);
    }

    #[test]
    fn memory_log_manager_tests() {
        log_manager_tests::<MemoryFile>("memory_log_manager", None, None);
    }

    #[test]
    fn file_encrypted_log_manager_tests() {
        log_manager_tests::<StdFile>(
            "encrypted_file_log_manager",
            Some(Arc::new(RotatorVault::new(13))),
            None,
        );
    }

    fn log_manager_tests<File: ManagedFile>(
        file_name: &str,
        vault: Option<Arc<dyn AnyVault>>,
        cache: Option<ChunkCache>,
    ) {
        let temp_dir = crate::test_util::TestDirectory::new(file_name);
        std::fs::create_dir(&temp_dir).unwrap();
        let file_manager = <File::Manager as Default>::default();
        let context = Context {
            file_manager,
            vault,
            cache,
        };
        let manager = TransactionManager::spawn(&temp_dir, context).unwrap();
        assert_eq!(manager.current_transaction_id(), None);
        assert_eq!(manager.len(), 0);
        assert!(manager.is_empty());

        let mut handles = Vec::new();
        for _ in 0..10 {
            let manager = manager.clone();
            handles.push(std::thread::spawn(move || {
                for id in 0_u32..1_000 {
                    let tx = manager.new_transaction([&id.to_be_bytes()[..]]);
                    manager.push(tx).unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(manager.current_transaction_id(), Some(10_000));
        assert_eq!(manager.next_transaction_id(), 10_001);

        assert!(manager
            .transaction_was_successful(manager.current_transaction_id().unwrap())
            .unwrap());

        assert!(!manager
            .transaction_was_successful(manager.next_transaction_id())
            .unwrap());

        let mut ten = None;
        manager
            .scan(10.., |entry| {
                ten = Some(entry);
                false
            })
            .unwrap();
        assert_eq!(ten.unwrap().id, 10);
    }
}
