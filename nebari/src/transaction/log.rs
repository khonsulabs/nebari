use std::{
    borrow::Cow, cmp::Ordering, collections::BTreeMap, io::Write, ops::RangeBounds, path::Path,
    sync::Arc,
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use super::{State, TransactionHandle};
use crate::{
    error::Error,
    storage::{sediment::SedimentFile, BlobStorage},
    transaction::TransactionId,
    vault::AnyVault,
    Context, ErrorKind,
};

use sediment::{
    format::{BatchId, GrainId},
    io::{self},
};

/// A transaction log that records changes for one or more trees.
#[derive(Debug, Clone)]
pub struct TransactionLog<File: io::FileManager> {
    vault: Option<Arc<dyn AnyVault>>,
    state: State,
    log: SedimentFile<File>,
}

impl<File: io::FileManager> TransactionLog<File> {
    /// Opens a transaction log
    pub fn open(log_path: &Path, state: State, context: Context<File>) -> Result<Self, Error> {
        let mut log = SedimentFile::open(log_path, false, context.file_manager.clone())?;
        if !state.is_initialized() {
            Self::initialize_state(&state, &mut log, context.vault())?;
        }
        Ok(Self {
            vault: context.vault,
            state,
            log,
        })
    }

    /// Initializes `state` to contain the information about the transaction log
    /// located at `log_path`.
    fn initialize_state(
        state: &State,
        db: &mut SedimentFile<File>,
        vault: Option<&dyn AnyVault>,
    ) -> Result<(), Error> {
        let commit_log = db.db.commit_log();
        if let Some(latest_entry) = commit_log.last() {
            let latest_batch = match latest_entry
                .embedded_header
                .map(|grain| db.db.get(grain))
                .transpose()?
                .flatten()
            {
                Some(entry) => {
                    let data = match vault {
                        Some(vault) => Cow::Owned(vault.decrypt(&entry)?),
                        None => Cow::Borrowed(entry.as_bytes()),
                    };
                    LogEntryBatch::deserialize(&data)?
                }
                None => return Err(Error::data_integrity("commit log entry missing header")),
            };
            let latest_transaction_id = match latest_batch.entries.iter().map(|(key, _)| *key).max()
            {
                Some(id) => id,
                None => {
                    return Err(Error::data_integrity(
                        "commit log batch had no transactions",
                    ))
                }
            };
            state.initialize(latest_transaction_id);
        } else {
            // Empty database, no commit log entries.
            state.initialize(TransactionId(0));
        }

        Ok(())
    }

    /// Logs one or more transactions. After this call returns, the transaction
    /// log is guaranteed to be fully written to disk.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::TransactionPushedOutOfOrder`] if `handles` is out of
    /// order, or if any handle contains an id older than one already written to
    /// the log.
    pub fn push<'a>(
        &mut self,
        handles: impl IntoIterator<Item = &'a LogEntry<'static>>,
    ) -> Result<(), Error> {
        let mut log_position = self.state.lock_for_write();
        let mut batch = LogEntryBatch::default();
        let mut completed_transactions = Vec::new();
        for handle in handles {
            if handle.id > log_position.last_written_transaction {
                log_position.last_written_transaction = handle.id;
            } else {
                return Err(Error::from(ErrorKind::TransactionPushedOutOfOrder));
            }
            let mut serialized = handle.serialize()?;
            if let Some(vault) = self.vault.as_ref() {
                serialized = vault.encrypt(&serialized)?;
            }

            let grain_id = self.log.write_async(serialized)?;
            batch.entries.insert(handle.id, grain_id);
            completed_transactions.push((handle.id, Some(grain_id)));
        }
        drop(log_position);

        let mut serialized = batch.serialize()?;
        if let Some(vault) = self.vault.as_ref() {
            serialized = vault.encrypt(&serialized)?;
        }
        self.log.write_header_async(serialized)?;
        let commit = self.log.sync()?.unwrap().batch_id();

        self.state.note_log_entry_batch(commit, &batch);
        self.state
            .note_transaction_ids_completed(&completed_transactions);

        Ok(())
    }

    /// Returns the executed transaction with the id provided. Returns None if not found.
    pub fn get(&mut self, id: TransactionId) -> Result<Option<LogEntry<'static>>, Error> {
        if let Some(entry_grain) = self.scan_for(id)? {
            if let Some(data) = self.log.db.get(entry_grain)? {
                let entry = self.deserialize_entry(&data)?;
                return Ok(Some(entry.into_owned()));
            }
        }

        Ok(None)
    }

    fn deserialize_entry(&self, data: &[u8]) -> Result<LogEntry<'static>, Error> {
        let data = match self.vault.as_ref() {
            Some(vault) => Cow::Owned(vault.decrypt(data)?),
            None => Cow::Borrowed(data),
        };
        LogEntry::deserialize(&data).map(LogEntry::into_owned)
    }

    pub fn scan<Callback: FnMut(LogEntry<'static>) -> bool>(
        &mut self,
        ids: impl RangeBounds<TransactionId>,
        mut callback: Callback,
    ) -> Result<(), Error> {
        let commit_log = self.log.db.commit_log();
        'outer: for (batch_id, entry_index) in &commit_log {
            let transaction_ids = self.batch_id_transactions(
                batch_id,
                entry_index
                    .embedded_header
                    .ok_or_else(|| Error::data_integrity("transaction entry missing header"))?,
            )?;
            for (_, grain_id) in transaction_ids.iter().filter(|(id, _)| ids.contains(id)) {
                let entry =
                    self.log.db.get(*grain_id)?.ok_or_else(|| {
                        Error::data_integrity("transaction entry grain data missing")
                    })?;
                let entry = self.deserialize_entry(&entry)?;
                if !callback(entry) {
                    break 'outer;
                }
            }
        }

        Ok(())
    }

    pub(crate) fn scan_for(
        &mut self,
        transaction: TransactionId,
    ) -> Result<Option<GrainId>, Error> {
        if let Some(known_status) = self.state.grain_id_for_transaction(transaction) {
            return Ok(known_status);
        }

        let commit_entries = self.log.db.commit_log().into_iter().collect::<Vec<_>>();
        if commit_entries.is_empty() {
            return Ok(None);
        }

        let mut lower_limit = 0;
        let mut lower_transaction_ids = self.batch_id_transactions(
            commit_entries[0].0,
            commit_entries[0].1.embedded_header.unwrap(),
        )?;
        let mut upper_limit = commit_entries.len() - 1;
        let mut upper_transaction_ids = self.batch_id_transactions(
            commit_entries.last().unwrap().0,
            commit_entries.last().unwrap().1.embedded_header.unwrap(),
        )?;

        loop {
            let delta = upper_limit - lower_limit;
            if transaction > lower_transaction_ids.last().unwrap().0
                && transaction < upper_transaction_ids.first().unwrap().0
                && delta > 1
            {
                let midpoint = lower_limit + delta / 2;
                let transaction_ids = self.batch_id_transactions(
                    commit_entries[midpoint].0,
                    commit_entries[midpoint].1.embedded_header.unwrap(),
                )?;

                match (
                    transaction_ids.first().unwrap().0.cmp(&transaction),
                    transaction_ids.last().unwrap().0.cmp(&transaction),
                ) {
                    (Ordering::Less | Ordering::Equal, Ordering::Greater | Ordering::Equal) => {
                        // Transaction id was within the range of this batch
                        break;
                    }
                    (Ordering::Greater, _) => {
                        upper_limit = midpoint;
                        upper_transaction_ids = transaction_ids;
                    }
                    (_, Ordering::Less) => {
                        lower_limit = midpoint;
                        lower_transaction_ids = transaction_ids;
                    }
                }
            } else {
                break;
            }
        }

        Ok(self.state.grain_id_for_transaction(transaction).flatten())
    }

    fn batch_id_transactions(
        &mut self,
        batch_id: BatchId,
        embedded_header: GrainId,
    ) -> Result<Vec<(TransactionId, GrainId)>, Error> {
        let ids = if let Some(ids) = self.state.batch_id_transactions(batch_id) {
            ids
        } else {
            let data = self.log.db.get(embedded_header)?.unwrap();
            let data = match self.vault.as_ref() {
                Some(vault) => Cow::Owned(vault.decrypt(&data)?),
                None => Cow::Borrowed(data.as_bytes()),
            };
            let batch = LogEntryBatch::deserialize(&data)?;
            self.state.note_log_entry_batch(batch_id, &batch)
        };

        Ok(ids)
    }

    /// Closes the transaction log.
    pub fn close(self) {
        drop(self);
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
    pub fn state(&self) -> &State {
        &self.state
    }
}

#[derive(Default, Debug)]
pub(crate) struct LogEntryBatch {
    pub entries: BTreeMap<TransactionId, GrainId>,
}

impl LogEntryBatch {
    pub(crate) fn serialize(&self) -> Result<Vec<u8>, Error> {
        let mut buffer = Vec::with_capacity(16 * self.entries.len());

        for (transaction_id, grain_id) in &self.entries {
            buffer.write_u64::<BigEndian>(transaction_id.0)?;
            buffer.write_u64::<BigEndian>(grain_id.as_u64())?;
        }

        Ok(buffer)
    }

    pub(crate) fn deserialize(mut buffer: &[u8]) -> Result<Self, Error> {
        let mut entries = BTreeMap::new();
        while !buffer.is_empty() {
            let transaction_id = TransactionId(buffer.read_u64::<BigEndian>()?);
            let grain_id = GrainId::from(buffer.read_u64::<BigEndian>()?);
            entries.insert(transaction_id, grain_id);
        }

        Ok(Self { entries })
    }
}

/// An entry in a transaction log.
#[derive(Eq, PartialEq, Debug)]
pub struct LogEntry<'a> {
    /// The unique id of this entry.
    pub id: TransactionId,
    pub trees_changed: Vec<Cow<'a, [u8]>>,
    pub(crate) data: Option<Cow<'a, [u8]>>,
}

impl<'a> LogEntry<'a> {
    /// Convert this entry into a `'static` lifetime.
    #[must_use]
    pub fn into_owned(self) -> LogEntry<'static> {
        LogEntry {
            id: self.id,
            trees_changed: self
                .trees_changed
                .into_iter()
                .map(|tree| Cow::Owned(tree.into_owned()))
                .collect(),
            data: self.data.map(|data| Cow::Owned(data.into_owned())),
        }
    }
}

impl<'a> LogEntry<'a> {
    /// Returns the associated data, if any.
    #[must_use]
    pub fn data(&self) -> Option<&[u8]> {
        self.data.as_deref()
    }

    /// Sets the associated data that will be stored in the transaction log.
    /// Limited to a length 16,777,216 bytes (16MB).
    pub fn set_data(&mut self, data: impl Into<Cow<'a, [u8]>>) -> Result<(), Error> {
        let data = data.into();
        if data.len() <= 2_usize.pow(24) {
            self.data = Some(data);
            Ok(())
        } else {
            Err(Error::from(ErrorKind::ValueTooLarge))
        }
    }

    pub(crate) fn serialize(&self) -> Result<Vec<u8>, Error> {
        let mut buffer = Vec::with_capacity(
            10 + self
                .trees_changed
                .iter()
                .map(|name| name.len() + 2)
                .sum::<usize>()
                + self.data.as_ref().map_or(0, |data| data.len()),
        );
        // Transaction ID
        buffer.write_u64::<BigEndian>(self.id.0)?;
        buffer.write_u16::<BigEndian>(u16::try_from(self.trees_changed.len())?)?;

        for tree in &self.trees_changed {
            buffer.write_u16::<BigEndian>(u16::try_from(tree.len())?)?;
            buffer.write_all(tree)?;
        }

        if let Some(data) = &self.data {
            // The rest of the entry is the data. Since the header of the log entry
            // contains the length, we don't need to waste space encoding it again.
            buffer.write_all(data)?;
        }

        Ok(buffer)
    }

    pub(crate) fn deserialize(mut buffer: &'a [u8]) -> Result<Self, Error> {
        let id = TransactionId(buffer.read_u64::<BigEndian>()?);
        let number_of_trees = buffer.read_u16::<BigEndian>()?;
        let mut trees_changed = Vec::new();
        for _ in 0..number_of_trees {
            let name_length = usize::from(buffer.read_u16::<BigEndian>()?);
            if name_length > buffer.len() {
                return Err(Error::data_integrity("invalid tree name length"));
            }
            trees_changed.push(Cow::Borrowed(&buffer[..name_length]));
            buffer = &buffer[name_length..];
        }

        let data = if buffer.is_empty() {
            None
        } else {
            Some(Cow::Borrowed(buffer))
        };

        Ok(Self {
            id,
            trees_changed,
            data,
        })
    }
}

#[test]
fn serialization_tests() {
    let transaction = LogEntry {
        id: TransactionId(1),
        trees_changed: vec![Cow::Borrowed(b"a-tree")],
        data: Some(Cow::Borrowed(b"hello")),
    };
    let serialized = transaction.serialize().unwrap();
    let deserialized = LogEntry::deserialize(&serialized).unwrap();
    assert_eq!(transaction, deserialized);

    let transaction = LogEntry {
        id: TransactionId(u64::MAX),
        trees_changed: vec![Cow::Borrowed(b"a-tree"), Cow::Borrowed(b"another-tree")],
        data: None,
    };
    let serialized = transaction.serialize().unwrap();
    let deserialized = LogEntry::deserialize(&serialized).unwrap();
    assert_eq!(transaction, deserialized);

    // Test the data length limits
    let mut transaction = LogEntry {
        id: TransactionId(0),
        trees_changed: vec![Cow::Borrowed(b"a-tree")],
        data: None,
    };
    let mut big_data = Vec::new();
    big_data.resize(2_usize.pow(24) + 1, 0);
    assert!(matches!(
        transaction.set_data(big_data.clone()),
        Err(Error {
            kind: ErrorKind::ValueTooLarge,
            ..
        })
    ));

    // Remove one byte and try again.
    big_data.truncate(big_data.len() - 1);
    transaction.set_data(big_data).unwrap();
    let serialized = transaction.serialize().unwrap();
    let deserialized = LogEntry::deserialize(&serialized).unwrap();
    assert_eq!(transaction, deserialized);
}

#[cfg(test)]
#[allow(clippy::semicolon_if_nothing_returned, clippy::future_not_send)]
mod tests {

    use std::collections::{BTreeSet, HashSet};

    use nanorand::{Pcg64, Rng};
    use sediment::io::{
        any::AnyFileManager, fs::StdFileManager, memory::MemoryFileManager, FileManager,
    };
    use tempfile::tempdir;

    use super::*;
    use crate::{test_util::RotatorVault, transaction::TransactionManager, ChunkCache};

    #[test]
    fn file_log_file_tests() {
        log_file_tests("file_log_file", StdFileManager::default(), None, None);
        log_file_tests(
            "file_log_file_encrypted",
            StdFileManager::default(),
            Some(Arc::new(RotatorVault::new(13))),
            None,
        );
    }

    #[test]
    fn memory_log_file_tests() {
        log_file_tests("memory_log_file", MemoryFileManager::default(), None, None);
        log_file_tests(
            "memory_log_file",
            MemoryFileManager::default(),
            Some(Arc::new(RotatorVault::new(13))),
            None,
        );
    }

    #[test]
    fn any_log_file_tests() {
        log_file_tests("any_file_log_file", AnyFileManager::new_file(), None, None);
        log_file_tests(
            "any_memory_log_file",
            AnyFileManager::new_memory(),
            None,
            None,
        );
    }

    #[allow(clippy::too_many_lines)]
    fn log_file_tests<Manager: io::FileManager>(
        file_name: &str,
        file_manager: Manager,
        vault: Option<Arc<dyn AnyVault>>,
        cache: Option<ChunkCache>,
    ) {
        const ITERS: u64 = 10;
        let temp_dir = crate::test_util::TestDirectory::new(file_name);
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

        let mut rng = Pcg64::new_seed(1);
        let data = (0..4096 * 10).map(|_| rng.generate()).collect::<Vec<u8>>();

        for id in 1..=ITERS {
            println!("{id}");
            let state = State::from_path(&log_path);
            let mut transactions =
                TransactionLog::<Manager>::open(&log_path, state, context.clone()).unwrap();
            assert_eq!(
                transactions.state().next_transaction_id(),
                TransactionId(id)
            );
            let mut tx = transactions.new_transaction([&b"hello"[..]]);

            tx.transaction.data = Some(Cow::Owned(id.to_be_bytes().to_vec()));
            // We want to have varying sizes to try to test the scan algorithm thoroughly.
            #[allow(clippy::cast_possible_truncation)]
            tx.set_data(data[0..id as usize].to_vec()).unwrap();

            assert!(tx.data.as_ref().unwrap().len() > 0);

            transactions.push(&vec![tx.transaction]).unwrap();
            transactions.close();
        }

        let state = State::from_path(&log_path);
        if context.vault.is_none() {
            // Test that we can't open it with encryption. Without
            // https://github.com/khonsulabs/nebari/issues/35, the inverse isn't
            // able to be tested.
            assert!(TransactionLog::<Manager>::open(
                &log_path,
                state.clone(),
                Context {
                    file_manager: context.file_manager.clone(),
                    vault: Some(Arc::new(RotatorVault::new(13))),
                    cache: None
                }
            )
            .is_err());
        }

        let mut transactions = TransactionLog::<Manager>::open(&log_path, state, context).unwrap();

        let out_of_order = transactions.new_transaction([&b"test"[..]]);
        transactions
            .push(&vec![
                transactions.new_transaction([&b"test2"[..]]).transaction,
            ])
            .unwrap();
        assert!(matches!(
            transactions
                .push(&vec![out_of_order.transaction])
                .unwrap_err()
                .kind,
            ErrorKind::TransactionPushedOutOfOrder
        ));

        assert!(transactions.get(TransactionId(0)).unwrap().is_none());
        for id in 1..=ITERS {
            let transaction = transactions.get(TransactionId(id)).unwrap();
            match transaction {
                Some(transaction) => {
                    assert_eq!(transaction.id, TransactionId(id));
                    assert_eq!(
                        &data[..transaction.data().unwrap().len()],
                        transaction.data().unwrap()
                    );
                }
                None => {
                    unreachable!("failed to fetch transaction {}", id)
                }
            }
        }
        assert!(transactions.get(TransactionId(1001)).unwrap().is_none());

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
            .scan(TransactionId(first_ten[0].id.0 + 1).., |entry| {
                after_first = Some(entry);
                false
            })
            .unwrap();
        assert_eq!(after_first.as_ref(), first_ten.get(1));
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
        let mut transactions =
            TransactionLog::<StdFileManager>::open(&log_path, state, context).unwrap();

        let mut valid_ids = HashSet::new();
        for id in 1..=10_000 {
            assert_eq!(
                transactions.state().next_transaction_id(),
                TransactionId(id)
            );
            let tx = transactions.new_transaction([&b"hello"[..]]);
            if rng.generate::<u8>() < 8 {
                // skip a few ids.
                continue;
            }
            valid_ids.insert(tx.id);

            transactions.push(&vec![tx.transaction]).unwrap();
        }

        for id in 1..=10_000 {
            let transaction = transactions.get(TransactionId(id)).unwrap();
            match transaction {
                Some(transaction) => assert_eq!(transaction.id, TransactionId(id)),
                None => {
                    assert!(
                        !valid_ids.contains(&TransactionId(id)),
                        "failed to find {id}"
                    );
                }
            }
        }
    }

    #[test]
    fn file_log_manager_tests() {
        log_manager_tests("file_log_manager", StdFileManager::default(), None, None);
    }

    #[test]
    fn memory_log_manager_tests() {
        log_manager_tests(
            "memory_log_manager",
            MemoryFileManager::default(),
            None,
            None,
        );
    }

    #[test]
    fn any_log_manager_tests() {
        log_manager_tests("any_log_manager", AnyFileManager::new_file(), None, None);
        log_manager_tests("any_log_manager", AnyFileManager::new_memory(), None, None);
    }

    #[test]
    fn file_encrypted_log_manager_tests() {
        log_manager_tests(
            "encrypted_file_log_manager",
            MemoryFileManager::default(),
            Some(Arc::new(RotatorVault::new(13))),
            None,
        );
    }

    fn log_manager_tests<Manager: FileManager>(
        file_name: &str,
        file_manager: Manager,
        vault: Option<Arc<dyn AnyVault>>,
        cache: Option<ChunkCache>,
    ) {
        let temp_dir = crate::test_util::TestDirectory::new(file_name);
        std::fs::create_dir(&temp_dir).unwrap();
        let context = Context {
            file_manager,
            vault,
            cache,
        };
        let manager = TransactionManager::spawn(&temp_dir, context, None).unwrap();
        assert_eq!(manager.current_transaction_id(), None);

        let mut handles = Vec::new();
        for _ in 0..10 {
            let manager = manager.clone();
            handles.push(std::thread::spawn(move || {
                for id in 0_u32..1_000 {
                    let tx = manager.new_transaction([&id.to_be_bytes()[..]]);
                    tx.commit(Vec::new()).unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(
            manager.current_transaction_id(),
            Some(TransactionId(10_000))
        );
        assert_eq!(manager.next_transaction_id(), TransactionId(10_001));

        assert!(manager
            .transaction_was_successful(manager.current_transaction_id().unwrap())
            .unwrap());

        assert!(!manager
            .transaction_was_successful(manager.next_transaction_id())
            .unwrap());

        let mut ten = None;
        manager
            .scan(TransactionId(10).., |entry| {
                ten = Some(entry);
                false
            })
            .unwrap();
        assert_eq!(ten.unwrap().id, TransactionId(10));
    }

    #[test]
    fn file_out_of_order_log_manager_tests() {
        out_of_order_log_manager_tests(
            "file_out_of_order_log_manager",
            StdFileManager::default(),
            None,
            None,
        );
    }

    #[test]
    fn memory_out_of_order_log_manager_tests() {
        out_of_order_log_manager_tests(
            "memory_out_of_order_log_manager",
            MemoryFileManager::default(),
            None,
            None,
        );
    }

    #[test]
    fn any_out_of_order_log_manager_tests() {
        out_of_order_log_manager_tests(
            "any_out_of_order_log_manager",
            AnyFileManager::new_file(),
            None,
            None,
        );
        out_of_order_log_manager_tests(
            "any_out_of_order_log_manager",
            AnyFileManager::new_memory(),
            None,
            None,
        );
    }

    fn out_of_order_log_manager_tests<Manager: FileManager>(
        file_name: &str,
        file_manager: Manager,
        vault: Option<Arc<dyn AnyVault>>,
        cache: Option<ChunkCache>,
    ) {
        let temp_dir = crate::test_util::TestDirectory::new(file_name);
        std::fs::create_dir(&temp_dir).unwrap();
        let context = Context {
            file_manager,
            vault,
            cache,
        };
        let manager = TransactionManager::spawn(&temp_dir, context, None).unwrap();
        let mut rng = Pcg64::new_seed(1);

        for batch in 1..=100_u8 {
            println!("New batch");
            // Generate a bunch of transactions.
            let mut handles = Vec::new();
            for tree in 1..=batch {
                handles.push(manager.new_transaction([&tree.to_be_bytes()[..]]));
            }
            rng.shuffle(&mut handles);
            let (handle_sender, handle_receiver) = flume::unbounded();
            let mut should_commit_handles = Vec::new();
            let mut expected_ids = BTreeSet::new();
            for (index, handle) in handles.into_iter().enumerate() {
                let should_commit_handle = rng.generate::<f32>() > 0.25 || expected_ids.is_empty();
                if should_commit_handle {
                    expected_ids.insert(handle.id);
                }
                should_commit_handles.push(should_commit_handle);
                handle_sender.send((index, handle)).unwrap();
            }
            let should_commit_handles = Arc::new(should_commit_handles);
            let mut threads = Vec::new();
            for _ in 1..=batch {
                let handle_receiver = handle_receiver.clone();
                let should_commit_handles = should_commit_handles.clone();
                threads.push(std::thread::spawn(move || {
                    let (handle_index, handle) = handle_receiver.recv().unwrap();
                    if should_commit_handles[handle_index] {
                        println!("Committing handle {}", handle.id);
                        handle.commit(Vec::new()).unwrap();
                    } else {
                        println!("Dropping handle {}", handle.id);
                        handle.rollback();
                    }
                }));
            }
            for thread in threads {
                thread.join().unwrap();
            }
            manager
                .scan(dbg!(*expected_ids.iter().next().unwrap()).., |tx| {
                    expected_ids.remove(&tx.id);
                    true
                })
                .unwrap();
            assert!(expected_ids.is_empty(), "{:?}", expected_ids);
        }
    }
}
