use std::{
    collections::HashMap,
    ops::{Deref, DerefMut, RangeBounds},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use sediment::{
    database::PendingCommit,
    format::GrainId,
    io::{self, paths::PathId},
};

use super::{LogEntry, State, TransactionLog};
use crate::{
    error::{Error, InternalError},
    transaction::TransactionId,
    tree::CommittedTreeState,
    Context, ErrorKind, ThreadPool,
};

/// A shared [`TransactionLog`] manager. Allows multiple threads to interact with a single transaction log.
#[derive(Debug, Clone)]
pub struct TransactionManager<Manager: io::FileManager> {
    state: State,
    log: Arc<Mutex<TransactionLog<Manager>>>,
    transaction_sender: flume::Sender<ThreadCommand<Manager>>,
}

impl<Manager> TransactionManager<Manager>
where
    Manager: io::FileManager,
{
    /// Spawns a new transaction manager. The transaction manager runs its own
    /// thread that writes to the transaction log. If a thread pool is provided,
    /// trees provided to `push` will be flushed to disk using the thread pool.
    pub fn spawn(
        directory: &Path,
        context: Context<Manager>,
        thread_pool: Option<ThreadPool<Manager>>,
    ) -> Result<Self, Error> {
        let log_path = Self::log_path(directory);
        let state = State::from_path(&log_path);
        let log = TransactionLog::<Manager>::open(&log_path, state.clone(), context)?;
        let transaction_id = log.state().next_transaction_id();

        let (transaction_sender, receiver) = flume::bounded(32);
        let thread_log = log.clone();
        std::thread::Builder::new()
            .name(String::from("nebari-txlog"))
            .spawn(move || {
                ManagerThread::<Manager>::run(transaction_id, thread_log, receiver, thread_pool);
            })
            .map_err(ErrorKind::message)?;

        Ok(Self {
            state,
            log: Arc::new(Mutex::new(log)),
            transaction_sender,
        })
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
    ) -> ManagedTransaction<Manager> {
        ManagedTransaction {
            transaction: Some(self.state.new_transaction(trees)),
            manager: self.clone(),
        }
    }

    /// Push `transaction` to the log. Once this function returns:
    ///
    /// - All `trees` have been fully flushed to disk
    /// - the transaction log entry has been fully flushed to disk
    /// - All reader states of `trees` have been published with the newly
    ///   updated state.
    ///
    /// Due to potential batching, readers may see multiple transactions
    /// published at once.
    fn push(
        &self,
        transaction: TransactionHandle,
        trees: Vec<CommittedTreeState<Manager>>,
    ) -> Result<(), Error> {
        let (completion_sender, completion_receiver) = flume::bounded(1);
        let TransactionHandle {
            transaction,
            locked_trees,
        } = transaction;
        // This allows the trees to begin being modified while the commit
        // happens.
        drop(locked_trees);
        self.transaction_sender
            .send(ThreadCommand::Commit {
                transaction,
                trees,
                completion_sender,
            })
            .map_err(|_| ErrorKind::Internal(InternalError::TransactionManagerStopped))?;
        completion_receiver.recv().map_err(|_| {
            Error::from(ErrorKind::Internal(
                InternalError::TransactionManagerStopped,
            ))
        })
    }

    /// Scans the transaction log for entries with ids within `range`. Invokes
    /// `callback` for each entry found. The scan will always scan forwards
    /// starting with the lowest ID matching the range.
    pub fn scan<Callback: FnMut(LogEntry<'static>) -> bool>(
        &self,
        range: impl RangeBounds<TransactionId>,
        callback: Callback,
    ) -> Result<(), Error> {
        let mut log = self.log.lock();
        log.scan(range, callback)
    }

    /// Returns true if the transaction id was recorded in the transaction log. This method caches
    pub fn transaction_was_successful(&self, transaction_id: TransactionId) -> Result<bool, Error> {
        self.transaction_grain_id(transaction_id)
            .map(|position| position.is_some())
    }

    /// Returns the location on disk of the transaction, if found.
    fn transaction_grain_id(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<GrainId>, Error> {
        if !transaction_id.valid() {
            Ok(None)
        } else if let Some(position) = self.state.grain_id_for_transaction(transaction_id) {
            Ok(position)
        } else {
            let mut log = self.log.lock();
            if let Some(transaction) = log.scan_for(transaction_id)? {
                Ok(Some(transaction))
            } else {
                self.state.note_transaction_id_status(transaction_id, None);
                Ok(None)
            }
        }
    }

    pub(crate) fn drop_transaction_id(&self, transaction_id: TransactionId) {
        drop(
            self.transaction_sender
                .send(ThreadCommand::Drop(transaction_id)),
        );
    }

    fn log_path(directory: &Path) -> PathBuf {
        directory.join("_transactions")
    }

    /// Returns the current state of the transaction log.
    #[must_use]
    pub fn state(&self) -> &State {
        &**self
    }
}

impl<Manager: io::FileManager> Deref for TransactionManager<Manager> {
    type Target = State;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

enum ThreadCommand<Manager: io::FileManager> {
    Commit {
        transaction: LogEntry<'static>,
        trees: Vec<CommittedTreeState<Manager>>,
        completion_sender: flume::Sender<()>,
    },
    Drop(TransactionId),
}

struct ManagerThread<Manager: io::FileManager> {
    state: ThreadState,
    commands: flume::Receiver<ThreadCommand<Manager>>,
    log: TransactionLog<Manager>,
    pending_transaction_ids: IdSequence,
    last_processed_id: TransactionId,
    transaction_batch: Vec<LogEntry<'static>>,
    transaction_batch_sessions: HashMap<PathId, Vec<PendingCommit<Manager>>>,
    completion_senders: Vec<(flume::Sender<()>, Vec<CommittedTreeState<Manager>>)>,
    thread_pool: Option<ThreadPool<Manager>>,
    batching_deadline: Instant,
}

enum ThreadState {
    Fresh,
    Batching,
    EnsuringSequence,
}

impl<Manager: io::FileManager> ManagerThread<Manager> {
    const BATCH: usize = 16;

    fn run(
        last_processed_transaction: TransactionId,
        log: TransactionLog<Manager>,
        transactions: flume::Receiver<ThreadCommand<Manager>>,
        thread_pool: Option<ThreadPool<Manager>>,
    ) {
        Self {
            state: ThreadState::Fresh,
            commands: transactions,
            last_processed_id: last_processed_transaction,
            pending_transaction_ids: IdSequence::new(last_processed_transaction),
            log,
            transaction_batch: Vec::with_capacity(Self::BATCH),
            completion_senders: Vec::with_capacity(Self::BATCH),
            transaction_batch_sessions: HashMap::with_capacity(Self::BATCH),
            thread_pool,
            batching_deadline: Instant::now(),
        }
        .save_transactions();
    }

    fn save_transactions(mut self) {
        while self.process_next_command() {}
    }

    fn process_next_command(&mut self) -> bool {
        match self.state {
            ThreadState::Fresh => self.process_next_command_fresh(),
            ThreadState::Batching => self.process_next_command_batching(),
            ThreadState::EnsuringSequence => self.process_next_command_ensuring_sequence(),
        }
    }

    fn process_next_command_fresh(&mut self) -> bool {
        match self.commands.recv() {
            Ok(command) => {
                match command {
                    ThreadCommand::Commit {
                        transaction,
                        mut trees,
                        completion_sender,
                    } => {
                        self.pending_transaction_ids.note(transaction.id);
                        if self.pending_transaction_ids.complete() {
                            // Safe to start a new batch
                            self.last_processed_id = transaction.id;
                            self.state = ThreadState::Batching;
                            self.batching_deadline = Instant::now() + Duration::from_micros(100);
                        } else {
                            // Need to wait for IDs
                            self.state = ThreadState::EnsuringSequence;
                        }

                        for tree in &mut trees {
                            let tree_sessions = self
                                .transaction_batch_sessions
                                .entry(tree.path_id.clone())
                                .or_default();
                            tree_sessions.push(tree.session_to_commit.take().unwrap());
                        }
                        self.transaction_batch.push(transaction);

                        self.completion_senders.push((completion_sender, trees));
                    }
                    ThreadCommand::Drop(id) => {
                        self.mark_transaction_handled(id);
                    }
                }
                true
            }
            Err(_) => false,
        }
    }

    fn mark_transaction_handled(&mut self, id: TransactionId) {
        self.pending_transaction_ids.note(id);
        if self.pending_transaction_ids.complete() && !self.transaction_batch.is_empty() {
            self.commit_transaction_batch();
        }
    }

    fn process_next_command_batching(&mut self) -> bool {
        match self.commands.recv_deadline(self.batching_deadline) {
            Ok(command) => {
                match command {
                    ThreadCommand::Commit {
                        transaction,
                        mut trees,
                        completion_sender,
                    } => {
                        // Ensure this transaction can be batched. If not,
                        // commit and enqueue it.
                        self.note_potentially_sequntial_id(transaction.id);

                        for tree in &mut trees {
                            let tree_sessions = self
                                .transaction_batch_sessions
                                .entry(tree.path_id.clone())
                                .or_default();
                            tree_sessions.push(tree.session_to_commit.take().unwrap());
                        }
                        self.transaction_batch.push(transaction);

                        self.completion_senders.push((completion_sender, trees));
                    }
                    ThreadCommand::Drop(id) => {
                        self.note_potentially_sequntial_id(id);
                    }
                }
                true
            }
            Err(flume::RecvTimeoutError::Timeout) => {
                // No more pending transactions are ready.
                self.commit_transaction_batch();
                true
            }
            Err(flume::RecvTimeoutError::Disconnected) => false,
        }
    }

    fn note_potentially_sequntial_id(&mut self, id: TransactionId) {
        self.pending_transaction_ids.note(id);
        if self.pending_transaction_ids.complete() {
            // Safe to start a new batch
            self.last_processed_id = id;
            self.state = ThreadState::Batching;
        } else {
            if !self.transaction_batch.is_empty() {
                self.commit_transaction_batch();
            }
            self.state = ThreadState::EnsuringSequence;
        }
    }

    fn process_next_command_ensuring_sequence(&mut self) -> bool {
        match self.commands.recv() {
            Ok(command) => {
                match command {
                    ThreadCommand::Commit {
                        transaction,
                        mut trees,
                        completion_sender,
                    } => {
                        let transaction_id = transaction.id;

                        for tree in &mut trees {
                            let tree_sessions = self
                                .transaction_batch_sessions
                                .entry(tree.path_id.clone())
                                .or_default();
                            tree_sessions.push(tree.session_to_commit.take().unwrap());
                        }
                        self.transaction_batch.push(transaction);

                        self.completion_senders.push((completion_sender, trees));
                        self.mark_transaction_handled(transaction_id);
                    }
                    ThreadCommand::Drop(id) => {
                        self.mark_transaction_handled(id);
                    }
                }
                true
            }
            Err(_) => false,
        }
    }

    fn commit_transaction_batch(&mut self) {
        let mut transaction_batch = Vec::with_capacity(Self::BATCH);
        std::mem::swap(&mut transaction_batch, &mut self.transaction_batch);
        transaction_batch.sort_unstable_by(|a, b| a.id.cmp(&b.id));

        let mut tree_checkpoint_guards =
            HashMap::with_capacity(self.transaction_batch_sessions.len());

        let thread_commits = if let Some(thread_pool) = self.thread_pool.as_ref() {
            Some(
                thread_pool
                    .finish_all_commits_async(
                        self.transaction_batch_sessions
                            .drain()
                            .flat_map(|(_, sessions)| sessions),
                    )
                    .unwrap(),
            )
        } else {
            // No thread pool, so we have to commit everything in this thread.
            for (path_id, pending_commits) in self.transaction_batch_sessions.drain() {
                for pending_commit in pending_commits {
                    let guard = pending_commit.commit().unwrap();
                    tree_checkpoint_guards.insert(path_id.id, guard);
                }
            }
            None
        };
        self.last_processed_id = transaction_batch.last().unwrap().id;
        self.state = ThreadState::Fresh;
        self.log.push(&transaction_batch).unwrap();

        if let Some(mut thread_commits) = thread_commits {
            thread_commits.wait(&mut tree_checkpoint_guards).unwrap();
        }
        for (completion_sender, trees) in self.completion_senders.drain(..) {
            for tree in trees {
                tree.state.finish_commit(
                    tree_checkpoint_guards.remove(&tree.path_id.id),
                    tree.committed,
                );
            }
            let _ = completion_sender.send(());
        }
    }
}

/// A transaction that is managed by a [`TransactionManager`].
pub struct ManagedTransaction<Manager: io::FileManager> {
    pub(crate) manager: TransactionManager<Manager>,
    pub(crate) transaction: Option<TransactionHandle>,
}

impl<Manager: io::FileManager> Drop for ManagedTransaction<Manager> {
    fn drop(&mut self) {
        if let Some(transaction) = self.transaction.take() {
            self.manager.drop_transaction_id(transaction.id);
        }
    }
}
impl<Manager: io::FileManager> ManagedTransaction<Manager> {
    /// Commits the transaction to the transaction manager that created this
    /// transaction.
    #[allow(clippy::missing_panics_doc)] // Should be unreachable
    pub fn commit(
        mut self,
        committed_trees: Vec<CommittedTreeState<Manager>>,
    ) -> Result<(), Error> {
        let transaction = self.transaction.take().unwrap();
        self.manager.push(transaction, committed_trees)
    }

    /// Rolls the transaction back. It is not necessary to call this function --
    /// transactions will automatically be rolled back when the handle is
    /// dropped, if `commit()` isn't called first.
    pub fn rollback(self) {
        drop(self);
    }
}

impl<Manager: io::FileManager> Deref for ManagedTransaction<Manager> {
    type Target = LogEntry<'static>;

    fn deref(&self) -> &Self::Target {
        self.transaction.as_ref().unwrap()
    }
}

impl<Manager: io::FileManager> DerefMut for ManagedTransaction<Manager> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.transaction.as_mut().unwrap().transaction
    }
}

/// A handle to an executing transaction.
pub struct TransactionHandle {
    /// The transaction being executed.
    pub(crate) transaction: LogEntry<'static>,
    /// The trees locked by this transaction.
    pub(crate) locked_trees: TreeLocks,
}

/// A collection of handles that keep trees locked.
pub type TreeLocks = Vec<TreeLockHandle>;

/// An acquirable lock for a tree.
#[derive(Debug)]
pub struct TreeLock {
    data: Arc<TreeLockData>,
}

impl TreeLock {
    pub(crate) fn new() -> Self {
        Self {
            data: Arc::new(TreeLockData {
                locked: AtomicBool::new(false),
                blocked: Mutex::default(),
            }),
        }
    }

    pub(crate) fn lock(&self) -> TreeLockHandle {
        // Loop until we acquire a lock
        loop {
            // Try to acquire the lock without any possibility of blocking
            if self
                .data
                .locked
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }

            let unblocked_receiver = {
                let mut blocked = self.data.blocked.lock();
                // Now that we've acquired this lock, it's possible the lock has
                // been released. If there are no others waiting, we can re-lock
                // it. If there werealready others waiting, we want to allow
                // them to have a chance to wake up first, so we assume that the
                // lock is locked without checking.
                if blocked.is_empty()
                    && self
                        .data
                        .locked
                        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                {
                    break;
                }

                // Add a new sender to the blocked list, and return it so that
                // we can wait for it to be signalled.
                let (unblocked_sender, unblocked_receiver) = flume::bounded(1);
                blocked.push(unblocked_sender);
                unblocked_receiver
            };
            // Wait for our unblocked signal to be triggered before trying to acquire the lock again.
            let _ = unblocked_receiver.recv();
        }

        TreeLockHandle(Self {
            data: self.data.clone(),
        })
    }
}

#[derive(Debug)]
struct TreeLockData {
    locked: AtomicBool,
    blocked: Mutex<Vec<flume::Sender<()>>>,
}

/// A handle to a lock. Upon dropping, the lock will be released.
#[derive(Debug)]
pub struct TreeLockHandle(TreeLock);

impl Drop for TreeLockHandle {
    fn drop(&mut self) {
        self.0.data.locked.store(false, Ordering::SeqCst);

        let data = self.0.data.clone();
        let mut blocked = data.blocked.lock();
        for blocked in blocked.drain(..) {
            let _ = blocked.send(());
        }
    }
}

impl Deref for TransactionHandle {
    type Target = LogEntry<'static>;

    fn deref(&self) -> &Self::Target {
        &self.transaction
    }
}

impl DerefMut for TransactionHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.transaction
    }
}

struct IdSequence {
    start: u64,
    length: u64,
    statuses: Vec<usize>,
}

impl IdSequence {
    pub const fn new(start: TransactionId) -> Self {
        Self {
            start: start.0,
            length: 0,
            statuses: Vec::new(),
        }
    }

    pub fn note(&mut self, id: TransactionId) {
        self.length = ((id.0 + 1).checked_sub(self.start).unwrap()).max(self.length);
        let offset = usize::try_from(id.0.checked_sub(self.start).unwrap()).unwrap();
        let index = offset / (usize::BITS as usize);
        if self.statuses.len() < index + 1 {
            self.statuses.resize(index + 1, 0);
        }

        let bit_offset = offset as usize % (usize::BITS as usize);
        self.statuses[index] |= 1 << bit_offset;

        self.truncate();
    }

    pub const fn complete(&self) -> bool {
        self.length == 0
    }

    fn truncate(&mut self) {
        while self.length > 0 {
            let mask_bits = usize::try_from(self.length).unwrap();
            let mask_bits = mask_bits.min(usize::BITS as usize);
            let mask = usize::MAX >> (usize::BITS as usize - mask_bits);
            if self.statuses[0] & mask == mask {
                self.statuses.remove(0);
                let mask_bits = u64::try_from(mask_bits).unwrap();
                self.start += mask_bits;
                self.length -= mask_bits;
            } else {
                break;
            }
        }
    }
}

#[test]
fn id_sequence_tests() {
    let mut seq = IdSequence::new(TransactionId(1));
    seq.note(TransactionId(3));
    assert!(!seq.complete());
    seq.note(TransactionId(1));
    assert!(!seq.complete());
    seq.note(TransactionId(2));
    assert!(seq.complete());
    seq.note(TransactionId(4));
    assert!(seq.complete());

    let mut seq = IdSequence::new(TransactionId(0));
    for id in (0..=65).rev() {
        seq.note(TransactionId(id));
        assert_eq!(id == 0, seq.complete());
    }

    let mut seq = IdSequence::new(TransactionId(1));
    for id in (1..=1024).rev() {
        seq.note(TransactionId(id));
        assert_eq!(id == 1, seq.complete());
    }
}
