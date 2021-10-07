use std::{
    ops::{Deref, DerefMut, RangeBounds},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use parking_lot::Mutex;

use super::{log::EntryFetcher, LogEntry, State, TransactionLog};
use crate::{
    error::{Error, InternalError},
    io::{FileManager, ManagedFile, OpenableFile},
    transaction::log::ScanResult,
    Context, ErrorKind,
};

/// A shared [`TransactionLog`] manager. Allows multiple threads to interact with a single transaction log.
#[derive(Debug, Clone)]
pub struct TransactionManager<Manager: FileManager> {
    state: State,
    transaction_sender: flume::Sender<(TransactionHandle, flume::Sender<TreeLocks>)>,
    context: Context<Manager>,
}

impl<Manager: FileManager> TransactionManager<Manager> {
    /// Spawns a new transaction manager. The transaction manager runs its own
    /// thread that writes to the transaction log.
    pub fn spawn(directory: &Path, context: Context<Manager>) -> Result<Self, Error> {
        let (transaction_sender, receiver) = flume::bounded(32);
        let log_path = Self::log_path(directory);

        let (state_sender, state_receiver) = flume::bounded(1);
        let thread_context = context.clone();
        std::thread::Builder::new()
            .name(String::from("nebari-txlog"))
            .spawn(move || {
                transaction_writer_thread::<Manager::File>(
                    state_sender,
                    log_path,
                    receiver,
                    thread_context,
                );
            })
            .map_err(ErrorKind::message)?;

        let state = state_receiver.recv().expect("failed to initialize")?;
        Ok(Self {
            state,
            transaction_sender,
            context,
        })
    }

    /// Push `transaction` to the log. Once this function returns, the
    /// transaction log entry has been fully flushed to disk.
    pub fn push(&self, transaction: TransactionHandle) -> Result<TreeLocks, Error> {
        let (completion_sender, completion_receiver) = flume::bounded(1);
        self.transaction_sender
            .send((transaction, completion_sender))
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
        range: impl RangeBounds<u64>,
        callback: Callback,
    ) -> Result<(), Error> {
        let mut log = TransactionLog::<Manager::File>::read(
            self.state.path(),
            self.state.clone(),
            self.context.clone(),
        )?;
        log.scan(range, callback)
    }

    /// Returns true if the transaction id was recorded in the transaction log. This method caches
    pub fn transaction_was_successful(&self, transaction_id: u64) -> Result<bool, Error> {
        self.transaction_position(transaction_id)
            .map(|position| position.is_some())
    }

    /// Returns the location on disk of the transaction, if found.
    pub fn transaction_position(&self, transaction_id: u64) -> Result<Option<u64>, Error> {
        if transaction_id == 0 {
            Ok(None)
        } else if let Some(position) = self.state.transaction_id_position(transaction_id) {
            Ok(position)
        } else {
            let mut log = self.context.file_manager.read(self.state.path())?;
            let transaction = log.execute(EntryFetcher {
                state: self.state(),
                id: transaction_id,
                vault: self.context.vault(),
            })?;
            match transaction {
                ScanResult::Found { position, .. } => {
                    self.state
                        .note_transaction_id_status(transaction_id, Some(position));
                    Ok(Some(position))
                }
                ScanResult::NotFound { .. } => {
                    self.state.note_transaction_id_status(transaction_id, None);
                    Ok(None)
                }
            }
        }
    }

    fn log_path(directory: &Path) -> PathBuf {
        directory.join("transactions")
    }

    /// Returns the current state of the transaction log.
    #[must_use]
    pub fn state(&self) -> &State {
        &**self
    }
}

impl<Manager: FileManager> Deref for TransactionManager<Manager> {
    type Target = State;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

// TODO: when an error happens, we should try to recover.
#[allow(clippy::needless_pass_by_value)]
fn transaction_writer_thread<File: ManagedFile>(
    state_sender: flume::Sender<Result<State, Error>>,
    log_path: PathBuf,
    transactions: flume::Receiver<(TransactionHandle, flume::Sender<TreeLocks>)>,
    context: Context<File::Manager>,
) {
    const BATCH: usize = 16;

    let state = State::from_path(&log_path);
    if let Err(err) = TransactionLog::<File>::initialize_state(&state, &context) {
        drop(state_sender.send(Err(err)));
        return;
    }

    drop(state_sender.send(Ok(state.clone())));

    let mut log = TransactionLog::<File>::open(&log_path, state, context).unwrap();

    while let Ok(transaction) = transactions.recv() {
        let mut transaction_batch = Vec::with_capacity(BATCH);
        let completion_sender = transaction.1;
        let TransactionHandle {
            transaction,
            locked_trees,
        } = transaction.0;
        transaction_batch.push(transaction);
        let mut completion_senders = Vec::with_capacity(BATCH);
        completion_senders.push((completion_sender, locked_trees));
        for _ in 0..BATCH - 1 {
            match transactions.try_recv() {
                Ok((
                    TransactionHandle {
                        transaction,
                        locked_trees,
                    },
                    sender,
                )) => {
                    transaction_batch.push(transaction);
                    completion_senders.push((sender, locked_trees));
                }
                // At this point either type of error we want to finish writing the transactions we have.
                Err(_) => break,
            }
        }
        log.push(transaction_batch).unwrap();
        for (completion_sender, tree_locks) in completion_senders {
            drop(completion_sender.send(tree_locks));
        }
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
