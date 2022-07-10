use std::{
    borrow::Cow,
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use parking_lot::{Mutex, MutexGuard};
use sediment::format::{BatchId, GrainId};

use super::{LogEntry, TransactionHandle, TreeLock, TreeLocks};
use crate::transaction::{log::LogEntryBatch, TransactionId};

const UNINITIALIZED_ID: u64 = 0;

/// The transaction log state.
#[derive(Clone, Debug)]
pub struct State {
    state: Arc<ActiveState>,
}

#[derive(Debug)]
struct ActiveState {
    pub(crate) path: PathBuf,
    current_transaction_id: AtomicU64,
    tree_locks: Mutex<HashMap<Cow<'static, [u8]>, TreeLock>>,
    log_position: Mutex<LogPosition>,
    batch_id_transactions: Mutex<HashMap<BatchId, Vec<(TransactionId, GrainId)>>>,
    known_completed_transactions: Mutex<HashMap<TransactionId, Option<GrainId>>>,
}

/// The active log position information.
#[derive(Debug)]
pub struct LogPosition {
    /// The last successfully written transaction id.
    pub last_written_transaction: TransactionId,
}

impl Default for LogPosition {
    fn default() -> Self {
        Self {
            last_written_transaction: TransactionId(UNINITIALIZED_ID),
        }
    }
}

impl State {
    /// Creates a new uninitialized state for a transaction log located at `path`.
    pub fn from_path(path: impl AsRef<Path>) -> Self {
        Self {
            state: Arc::new(ActiveState {
                path: path.as_ref().to_owned(),
                tree_locks: Mutex::default(),
                current_transaction_id: AtomicU64::new(UNINITIALIZED_ID),
                log_position: Mutex::default(),
                batch_id_transactions: Mutex::default(),
                known_completed_transactions: Mutex::default(),
            }),
        }
    }

    pub(crate) fn initialize(&self, last_written_transaction: TransactionId) {
        let mut state_position = self.state.log_position.lock();
        self.state
            .current_transaction_id
            .compare_exchange(
                UNINITIALIZED_ID,
                last_written_transaction.0 + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .expect("state already initialized");
        state_position.last_written_transaction = last_written_transaction;
    }

    pub(crate) fn is_initialized(&self) -> bool {
        let position = self.state.log_position.lock();
        !matches!(
            position.last_written_transaction,
            TransactionId(UNINITIALIZED_ID)
        )
    }

    /// Returns the last successfully written transaction id, or None if no
    /// transactions have been recorded yet.
    #[must_use]
    pub fn current_transaction_id(&self) -> Option<TransactionId> {
        let position = self.state.log_position.lock();
        match position.last_written_transaction {
            TransactionId(UNINITIALIZED_ID) => None,
            other => Some(other),
        }
    }

    /// Returns the next transaction id that will be used.
    #[must_use]
    pub fn next_transaction_id(&self) -> TransactionId {
        TransactionId(self.state.current_transaction_id.load(Ordering::SeqCst))
    }

    /// Returns the path to the file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.state.path
    }

    fn fetch_tree_locks<'a>(&self, trees: impl Iterator<Item = &'a [u8]>, locks: &mut TreeLocks) {
        // Sort the trees being locked to ensure no deadlocks can happen. For
        // example, if writer a tries to lock (a, b) and writer b tries to lock
        // (b, a), and both acquire their first lock, they would deadlock. By
        // sorting, the order of locking will never have dependencies that
        // cannot be met by blocking.
        let mut trees = trees.collect::<Vec<_>>();
        trees.sort_unstable();
        let mut tree_locks = self.state.tree_locks.lock();
        for tree in trees {
            if let Some(lock) = tree_locks.get(&Cow::Borrowed(tree)) {
                locks.push(lock.lock());
            } else {
                let lock = TreeLock::new();
                let locked = lock.lock();
                tree_locks.insert(Cow::Owned(tree.to_vec()), lock);
                locks.push(locked);
            }
        }
    }

    /// Creates a new transaction, exclusively locking `trees`. Will block the thread until the trees can be locked.
    #[must_use]
    pub fn new_transaction<
        'a,
        I: IntoIterator<Item = &'a [u8], IntoIter = II>,
        II: ExactSizeIterator<Item = &'a [u8]>,
    >(
        &self,
        trees: I,
    ) -> TransactionHandle {
        let trees = trees.into_iter();
        let mut locked_trees = Vec::with_capacity(trees.len());
        let trees_changed = trees
            .into_iter()
            .map(|buf| Cow::Owned(buf.to_vec()))
            .collect::<Vec<_>>();
        self.fetch_tree_locks(trees_changed.iter().map(|cow| &**cow), &mut locked_trees);

        TransactionHandle {
            locked_trees,
            transaction: LogEntry {
                id: TransactionId(
                    self.state
                        .current_transaction_id
                        .fetch_add(1, Ordering::SeqCst),
                ),
                trees_changed,
                data: None,
            },
        }
    }

    pub(crate) fn note_transaction_id_status(
        &self,
        transaction_id: TransactionId,
        position: Option<GrainId>,
    ) {
        let mut cache = self.state.known_completed_transactions.lock();
        cache.insert(transaction_id, position);
    }

    pub(crate) fn note_transaction_ids_completed(
        &self,
        transaction_ids: &[(TransactionId, Option<GrainId>)],
    ) {
        let mut cache = self.state.known_completed_transactions.lock();
        for (id, position) in transaction_ids {
            cache.insert(*id, *position);
        }
    }

    /// Returns an option representing whether the transaction id has
    /// information cached about it. The inner option contains the cache
    /// contents: either a valid position, or None if the transaction ID
    /// couldn't be found when it was last searched for.
    #[allow(clippy::option_option)]
    pub(crate) fn grain_id_for_transaction(
        &self,
        transaction_id: TransactionId,
    ) -> Option<Option<GrainId>> {
        let cache = self.state.known_completed_transactions.lock();
        cache.get(&transaction_id).copied()
    }

    /// Returns an option representing whether the transaction id has
    /// information cached about it. The inner option contains the cache
    /// contents: either a valid position, or None if the transaction ID
    /// couldn't be found when it was last searched for.
    #[allow(clippy::option_option)]
    pub(crate) fn batch_id_transactions(
        &self,
        batch: BatchId,
    ) -> Option<Vec<(TransactionId, GrainId)>> {
        let mut cache = self.state.batch_id_transactions.lock();
        cache.get(&batch).cloned()
    }

    pub(crate) fn note_log_entry_batch(
        &self,
        batch: BatchId,
        entries: &LogEntryBatch,
    ) -> Vec<(TransactionId, GrainId)> {
        let mut transaction_grains = Vec::new();
        let mut known_completed_transactions = self.state.known_completed_transactions.lock();
        for (transaction_id, grain_id) in &entries.entries {
            known_completed_transactions.insert(*transaction_id, Some(*grain_id));
            transaction_grains.push((*transaction_id, *grain_id));
        }

        let mut batch_id_transactions = self.state.batch_id_transactions.lock();
        batch_id_transactions.insert(batch, transaction_grains.clone());
        transaction_grains
    }
}

impl State {
    pub(crate) fn lock_for_write(&self) -> MutexGuard<'_, LogPosition> {
        self.state.log_position.lock()
    }
}
