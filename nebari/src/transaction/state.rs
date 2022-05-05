use std::{
    borrow::Cow,
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use lru::LruCache;
use parking_lot::{Mutex, MutexGuard};

use super::{LogEntry, TransactionHandle, TreeLock, TreeLocks};
use crate::transaction::TransactionId;

const UNINITIALIZED_ID: u64 = 0;

/// The transaction log state.
#[derive(Clone, Debug)]
pub struct State {
    state: Arc<ActiveState>,
}

#[derive(Debug)]
struct ActiveState {
    path: PathBuf,
    current_transaction_id: AtomicU64,
    tree_locks: Mutex<HashMap<Cow<'static, [u8]>, TreeLock>>,
    log_position: Mutex<LogPosition>,
    known_completed_transactions: Mutex<LruCache<TransactionId, Option<u64>>>,
}

/// The active log position information.
#[derive(Debug)]
pub struct LogPosition {
    /// The offset of the writer within the file.
    pub file_offset: u64,
    /// The last successfully written transaction id.
    pub last_written_transaction: TransactionId,
}

impl Default for LogPosition {
    fn default() -> Self {
        Self {
            file_offset: 0,
            last_written_transaction: TransactionId(UNINITIALIZED_ID),
        }
    }
}

impl State {
    /// Creates a new uninitialized state for a transaction log located at `path`.
    pub fn from_path(path: impl AsRef<Path>) -> Self {
        Self {
            state: Arc::new(ActiveState {
                path: path.as_ref().to_path_buf(),
                tree_locks: Mutex::default(),
                current_transaction_id: AtomicU64::new(UNINITIALIZED_ID),
                log_position: Mutex::new(LogPosition::default()),
                known_completed_transactions: Mutex::new(LruCache::new(1024)),
            }),
        }
    }

    pub(crate) fn initialize(&self, last_written_transaction: TransactionId, log_position: u64) {
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
        state_position.file_offset = log_position;
        state_position.last_written_transaction = last_written_transaction;
    }

    /// Returns the last successfully written transaction id, or None if no
    /// transactions have been recorded yet.
    #[must_use]
    pub fn current_transaction_id(&self) -> Option<TransactionId> {
        match self.state.current_transaction_id.load(Ordering::SeqCst) {
            UNINITIALIZED_ID | 1 => None,
            other => Some(TransactionId(other - 1)),
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

    /// Returns the current length of the log.
    #[must_use]
    pub fn len(&self) -> u64 {
        let position = self.state.log_position.lock();
        position.file_offset
    }

    /// Returns if the log is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
        self.fetch_tree_locks(trees, &mut locked_trees);

        TransactionHandle {
            locked_trees,
            transaction: LogEntry {
                id: TransactionId(
                    self.state
                        .current_transaction_id
                        .fetch_add(1, Ordering::SeqCst),
                ),
                data: None,
            },
        }
    }

    pub(crate) fn note_transaction_id_status(
        &self,
        transaction_id: TransactionId,
        position: Option<u64>,
    ) {
        let mut cache = self.state.known_completed_transactions.lock();
        cache.put(transaction_id, position);
    }

    pub(crate) fn note_transaction_ids_completed(
        &self,
        transaction_ids: &[(TransactionId, Option<u64>)],
    ) {
        let mut cache = self.state.known_completed_transactions.lock();
        for (id, position) in transaction_ids {
            cache.put(*id, *position);
        }
    }

    /// Returns an option representing whether the transaction id has
    /// information cached about it. The inner option contains the cache
    /// contents: either a valid position, or None if the transaction ID
    /// couldn't be found when it was last searched for.
    #[allow(clippy::option_option)]
    pub(crate) fn transaction_id_position(
        &self,
        transaction_id: TransactionId,
    ) -> Option<Option<u64>> {
        let mut cache = self.state.known_completed_transactions.lock();
        cache.get(&transaction_id).copied()
    }
}

impl State {
    pub(crate) fn lock_for_write(&self) -> MutexGuard<'_, LogPosition> {
        self.state.log_position.lock()
    }
}
