use std::{
    any::Any,
    borrow::Cow,
    collections::HashMap,
    convert::Infallible,
    fmt::{Debug, Display},
    fs,
    marker::PhantomData,
    ops::RangeBounds,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
};

use flume::Sender;
use once_cell::sync::Lazy;
use parking_lot::Mutex;

use crate::{
    context::Context,
    error::Error,
    io::{FileManager, ManagedFile},
    transaction::{LogEntry, TransactionHandle, TransactionManager},
    tree::{
        self, state::AnyTreeState, KeyEvaluation, Modification, Operation, State,
        TransactableCompaction, TreeFile, TreeRoot, VersionedTreeRoot,
    },
    Buffer, ChunkCache, ErrorKind, Vault,
};

/// A multi-tree transactional B-Tree database.
#[derive(Debug)]
pub struct Roots<File: ManagedFile> {
    data: Arc<Data<File>>,
}

#[derive(Debug)]
struct Data<File: ManagedFile> {
    context: Context<File::Manager>,
    transactions: TransactionManager<File::Manager>,
    thread_pool: ThreadPool<File>,
    path: PathBuf,
    tree_states: Mutex<HashMap<String, Box<dyn AnyTreeState>>>,
}

impl<File: ManagedFile> Roots<File> {
    fn open<P: Into<PathBuf> + Send>(
        path: P,
        context: Context<File::Manager>,
        thread_pool: ThreadPool<File>,
    ) -> Result<Self, Error> {
        let path = path.into();
        if !path.exists() {
            fs::create_dir_all(&path)?;
        } else if !path.is_dir() {
            return Err(Error::from(format!(
                "'{:?}' already exists, but is not a directory.",
                path
            )));
        }

        let transactions = TransactionManager::spawn(&path, context.clone())?;
        Ok(Self {
            data: Arc::new(Data {
                context,
                path,
                transactions,
                thread_pool,
                tree_states: Mutex::default(),
            }),
        })
    }

    /// Returns the path to the database directory.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.data.path
    }

    /// Returns the vault used to encrypt this database.
    #[must_use]
    pub fn context(&self) -> &Context<File::Manager> {
        &self.data.context
    }

    /// Returns the transaction manager for this database.
    #[must_use]
    pub fn transactions(&self) -> &TransactionManager<File::Manager> {
        &self.data.transactions
    }

    /// Opens a tree named `name`.
    // TODO enforce name restrictions.
    pub fn tree<Root: tree::Root, Name: Into<Cow<'static, str>>>(
        &self,
        name: Name,
    ) -> Result<Tree<Root, File>, Error> {
        let name = name.into();
        let path = self.tree_path(&name);
        if !path.exists() {
            self.context().file_manager.append(&path)?;
        }
        let state = self.tree_state(name.clone());
        Ok(Tree {
            roots: self.clone(),
            state,
            name,
        })
    }

    fn tree_path(&self, name: &str) -> PathBuf {
        self.path().join(format!("{}.nebari", name))
    }

    /// Removes a tree. Returns true if a tree was deleted.
    pub fn delete_tree(&self, name: impl Into<Cow<'static, str>>) -> Result<bool, Error> {
        let name = name.into();
        let mut tree_states = self.data.tree_states.lock();
        self.context()
            .file_manager
            .delete(self.tree_path(name.as_ref()))?;
        Ok(tree_states.remove(name.as_ref()).is_some())
    }

    /// Returns a list of all the names of trees contained in this database.
    pub fn tree_names(&self) -> Result<Vec<String>, Error> {
        let mut names = Vec::new();
        for entry in std::fs::read_dir(self.path())? {
            let entry = entry?;
            if let Some(name) = entry.file_name().to_str() {
                if let Some(without_extension) = name.strip_suffix(".nebari") {
                    names.push(without_extension.to_string());
                }
            }
        }
        Ok(names)
    }

    fn tree_state<Root: tree::Root>(&self, name: impl Into<Cow<'static, str>>) -> State<Root> {
        self.tree_states(&[Root::tree(name)])
            .into_iter()
            .next()
            .unwrap()
            .as_ref()
            .as_any()
            .downcast_ref::<State<Root>>()
            .unwrap()
            .clone()
    }

    fn tree_states(&self, names: &[TreeRoot<File>]) -> Vec<Box<dyn AnyTreeState>> {
        let mut tree_states = self.data.tree_states.lock();
        let mut output = Vec::with_capacity(names.len());
        for tree in names {
            let state = tree_states
                .entry(tree.name().to_string())
                .or_insert_with(|| tree.default_state())
                .cloned();
            output.push(state);
        }
        output
    }

    /// Begins a transaction over `trees`. All trees will be exclusively
    /// accessible by the transaction. Dropping the executing transaction will
    /// roll the transaction back.
    pub fn transaction(
        &self,
        trees: &[TreeRoot<File>],
    ) -> Result<ExecutingTransaction<File>, Error> {
        // TODO this extra vec here is annoying. We should have a treename type
        // that we can use instead of str.
        let transaction = self.data.transactions.new_transaction(
            &trees
                .iter()
                .map(|t| t.name().as_bytes())
                .collect::<Vec<_>>(),
        );
        let states = self.tree_states(trees);
        let trees = trees
            .iter()
            .zip(states.into_iter())
            .map(|(tree, state)| {
                tree.begin_transaction(
                    transaction.id,
                    &self.tree_path(tree.name()),
                    state.as_ref(),
                    self.context(),
                    Some(&self.data.transactions),
                )
            })
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(ExecutingTransaction {
            roots: self.clone(),
            transaction: Some(transaction),
            trees,
            transaction_manager: self.data.transactions.clone(),
        })
    }
}

impl<File: ManagedFile> Clone for Roots<File> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
        }
    }
}

/// An executing transaction. While this exists, no other transactions can
/// execute across the same trees as this transaction holds.
#[must_use]
pub struct ExecutingTransaction<File: ManagedFile> {
    roots: Roots<File>,
    transaction_manager: TransactionManager<File::Manager>,
    trees: Vec<Box<dyn AnyTransactionTree<File>>>,
    transaction: Option<TransactionHandle>,
}

impl<File: ManagedFile> ExecutingTransaction<File> {
    /// Returns the [`LogEntry`] for this transaction.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn entry(&self) -> &LogEntry<'static> {
        self.transaction.as_ref().unwrap()
    }

    /// Returns a mutable reference to the [`LogEntry`] for this transaction.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn entry_mut(&mut self) -> &mut LogEntry<'static> {
        self.transaction.as_mut().unwrap()
    }

    /// Commits the transaction. Once this function has returned, all data
    /// updates are guaranteed to be able to be accessed by all other readers as
    /// well as impervious to sudden failures such as a power outage.
    #[allow(clippy::missing_panics_doc)]
    pub fn commit(mut self) -> Result<(), Error> {
        let trees = std::mem::take(&mut self.trees);
        // Write the trees to disk
        let trees = self.roots.data.thread_pool.commit_trees(trees)?;

        // Push the transaction to the log.
        let tree_locks = self
            .transaction_manager
            .push(self.transaction.take().unwrap())?;

        // Publish the tree states, now that the transaction has been fully recorded
        for tree in trees {
            tree.state().publish();
        }

        // Release the locks for the trees, allowing a new transaction to begin.
        drop(tree_locks);

        Ok(())
    }

    /// Accesses a locked tree.
    pub fn tree<Root: tree::Root>(
        &mut self,
        index: usize,
    ) -> Option<&mut TransactionTree<Root, File>> {
        self.trees
            .get_mut(index)
            .and_then(|any_tree| any_tree.as_mut().as_any_mut().downcast_mut())
    }

    fn rollback_tree_states(&mut self) {
        for tree in self.trees.drain(..) {
            tree.rollback();
        }
    }
}

impl<File: ManagedFile> Drop for ExecutingTransaction<File> {
    fn drop(&mut self) {
        if let Some(transaction) = self.transaction.take() {
            self.rollback_tree_states();
            // Now the transaction can be dropped safely, freeing up access to the trees.
            drop(transaction);
        }
    }
}

/// A tree that is modifiable during a transaction.
pub struct TransactionTree<Root: tree::Root, File: ManagedFile> {
    pub(crate) transaction_id: u64,
    pub(crate) tree: TreeFile<Root, File>,
}

pub trait AnyTransactionTree<File: ManagedFile>: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;

    fn state(&self) -> Box<dyn AnyTreeState>;

    fn commit(&mut self) -> Result<(), Error>;
    fn rollback(&self);
}

impl<Root: tree::Root, File: ManagedFile> AnyTransactionTree<File> for TransactionTree<Root, File> {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn state(&self) -> Box<dyn AnyTreeState> {
        Box::new(self.tree.state.clone())
    }

    fn commit(&mut self) -> Result<(), Error> {
        self.tree.commit()
    }

    fn rollback(&self) {
        let mut state = self.tree.state.lock();
        state.rollback(&self.tree.state);
    }
}

impl<File: ManagedFile> TransactionTree<VersionedTreeRoot, File> {
    /// Returns the latest sequence id.
    pub fn current_sequence_id(&self) -> u64 {
        let state = self.tree.state.lock();
        state.root.sequence
    }
}

impl<Root: tree::Root, File: ManagedFile> TransactionTree<Root, File> {
    /// Sets `key` to `value`.
    pub fn set(
        &mut self,
        key: impl Into<Buffer<'static>>,
        value: impl Into<Buffer<'static>>,
    ) -> Result<(), Error> {
        self.tree
            .modify(Modification {
                transaction_id: self.transaction_id,
                keys: vec![key.into()],
                operation: Operation::Set(value.into()),
            })
            .map(|_| {})
    }

    /// Sets `key` to `value`. If a value already exists, it will be returned.
    #[allow(clippy::missing_panics_doc)]
    pub fn replace(
        &mut self,
        key: impl Into<Buffer<'static>>,
        value: impl Into<Buffer<'static>>,
    ) -> Result<Option<Buffer<'static>>, Error> {
        self.tree.replace(key, value, self.transaction_id)
    }

    /// Returns the current value of `key`. This will return updated information
    /// if it has been previously updated within this transaction.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Buffer<'static>>, Error> {
        self.tree.get(key, true)
    }

    /// Removes `key` and returns the existing value, if present.
    pub fn remove(&mut self, key: &[u8]) -> Result<Option<Buffer<'static>>, Error> {
        self.tree.remove(key, self.transaction_id)
    }

    /// Compares the value of `key` against `old`. If the values match, key will
    /// be set to the new value if `new` is `Some` or removed if `new` is
    /// `None`.
    pub fn compare_and_swap(
        &mut self,
        key: &[u8],
        old: Option<&Buffer<'_>>,
        new: Option<Buffer<'_>>,
    ) -> Result<(), CompareAndSwapError> {
        self.tree
            .compare_and_swap(key, old, new, self.transaction_id)
    }

    /// Retrieves the values of `keys`. If any keys are not found, they will be
    /// omitted from the results.
    pub fn get_multiple(
        &mut self,
        keys: &[&[u8]],
    ) -> Result<Vec<(Buffer<'static>, Buffer<'static>)>, Error> {
        self.tree.get_multiple(keys, true)
    }

    /// Retrieves all of the values of keys within `range`.
    pub fn get_range<'bounds, Range: RangeBounds<Buffer<'bounds>> + Debug + 'static>(
        &mut self,
        range: Range,
    ) -> Result<Vec<(Buffer<'static>, Buffer<'static>)>, Error> {
        self.tree.get_range(range, true)
    }

    /// Scans the tree. Each key that is contained `range` will be passed to
    /// `key_evaluator`, which can opt to read the data for the key, skip, or
    /// stop scanning. If `KeyEvaluation::ReadData` is returned, `callback` will
    /// be invoked with the key and stored value. The order in which `callback`
    /// is invoked is not necessarily the same order in which the keys are
    /// found.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(self, key_evaluator, callback))
    )]
    pub fn scan<'b, CallerError, Range, KeyEvaluator, DataCallback>(
        &mut self,
        range: Range,
        forwards: bool,
        mut key_evaluator: KeyEvaluator,
        mut callback: DataCallback,
    ) -> Result<(), AbortError<CallerError>>
    where
        Range: RangeBounds<Buffer<'b>> + Debug + 'static,
        KeyEvaluator: FnMut(&Buffer<'static>) -> KeyEvaluation,
        DataCallback:
            FnMut(Buffer<'static>, Buffer<'static>) -> Result<(), AbortError<CallerError>>,
        CallerError: Display + Debug,
    {
        self.tree
            .scan(range, forwards, true, &mut key_evaluator, &mut callback)
    }

    /// Returns the last  of the tree.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn last_key(&mut self) -> Result<Option<Buffer<'static>>, Error> {
        self.tree.last_key(true)
    }

    /// Returns the last key and value of the tree.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn last(&mut self) -> Result<Option<(Buffer<'static>, Buffer<'static>)>, Error> {
        self.tree.last(true)
    }
}

/// An error returned from `compare_and_swap()`.
#[derive(Debug, thiserror::Error)]
pub enum CompareAndSwapError {
    /// The stored value did not match the conditional value.
    #[error("value did not match. existing value: {0:?}")]
    Conflict(Option<Buffer<'static>>),
    /// Another error occurred while executing the operation.
    #[error("error during compare_and_swap: {0}")]
    Error(#[from] Error),
}

/// A database configuration used to open a database.
#[derive(Debug)]
#[must_use]
pub struct Config<File: ManagedFile> {
    path: PathBuf,
    vault: Option<Arc<dyn Vault>>,
    cache: Option<ChunkCache>,
    file_manager: Option<File::Manager>,
    thread_pool: Option<ThreadPool<File>>,
    _file: PhantomData<File>,
}

impl<File: ManagedFile> Config<File> {
    /// Creates a new config to open a database located at `path`.
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            vault: None,
            cache: None,
            thread_pool: None,
            file_manager: None,
            _file: PhantomData,
        }
    }

    /// Sets the vault to use for this database.
    pub fn vault<V: Vault>(mut self, vault: V) -> Self {
        self.vault = Some(Arc::new(vault));
        self
    }

    /// Sets the chunk cache to use for this database.
    pub fn cache(mut self, cache: ChunkCache) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Sets the file manager.
    pub fn file_manager(mut self, file_manager: File::Manager) -> Self {
        self.file_manager = Some(file_manager);
        self
    }

    /// Uses the `thread_pool` provided instead of creating its own. This will
    /// allow a single thread pool to manage multiple [`Roots`] instances'
    /// transactions.
    pub fn shared_thread_pool(mut self, thread_pool: &ThreadPool<File>) -> Self {
        self.thread_pool = Some(thread_pool.clone());
        self
    }

    /// Opens the database, or creates one if the target path doesn't exist.
    pub fn open(self) -> Result<Roots<File>, Error> {
        Roots::open(
            self.path,
            Context {
                file_manager: self.file_manager.unwrap_or_default(),
                vault: self.vault,
                cache: self.cache,
            },
            self.thread_pool.unwrap_or_default(),
        )
    }
}

/// A named collection of keys and values.
pub struct Tree<Root: tree::Root, File: ManagedFile> {
    roots: Roots<File>,
    state: State<Root>,
    name: Cow<'static, str>,
}

impl<Root: tree::Root, File: ManagedFile> Clone for Tree<Root, File> {
    fn clone(&self) -> Self {
        Self {
            roots: self.roots.clone(),
            state: self.state.clone(),
            name: self.name.clone(),
        }
    }
}

impl<Root: tree::Root, File: ManagedFile> Tree<Root, File> {
    /// Returns the name of the tree.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the path to the file for this tree.
    #[must_use]
    pub fn path(&self) -> PathBuf {
        self.roots.tree_path(self.name())
    }

    /// Returns the number of keys stored in the tree. Does not include deleted keys.
    #[must_use]
    pub fn count(&self) -> u64 {
        let state = self.state.lock();
        state.root.count()
    }

    /// Sets `key` to `value`. This is executed within its own transaction.
    #[allow(clippy::missing_panics_doc)]
    pub fn set(
        &self,
        key: impl Into<Buffer<'static>>,
        value: impl Into<Buffer<'static>>,
    ) -> Result<(), Error> {
        let mut transaction = self.roots.transaction(&[Root::tree(self.name.clone())])?;
        transaction.tree::<Root>(0).unwrap().set(key, value)?;
        transaction.commit()
    }

    /// Retrieves the current value of `key`, if present. Does not reflect any
    /// changes in pending transactions.
    pub fn get(&self, key: &[u8]) -> Result<Option<Buffer<'static>>, Error> {
        catch_compaction_and_retry(|| {
            let mut tree = TreeFile::<Root, File>::read(
                self.path(),
                self.state.clone(),
                self.roots.context(),
                Some(self.roots.transactions()),
            )?;

            tree.get(key, false)
        })
    }

    /// Removes `key` and returns the existing value, if present. This is executed within its own transaction.
    #[allow(clippy::missing_panics_doc)]
    pub fn remove(&self, key: &[u8]) -> Result<Option<Buffer<'static>>, Error> {
        let mut transaction = self.roots.transaction(&[Root::tree(self.name.clone())])?;
        let existing_value = transaction.tree::<Root>(0).unwrap().remove(key)?;
        transaction.commit()?;
        Ok(existing_value)
    }

    /// Compares the value of `key` against `old`. If the values match, key will
    /// be set to the new value if `new` is `Some` or removed if `new` is
    /// `None`. This is executed within its own transaction.
    #[allow(clippy::missing_panics_doc)]
    pub fn compare_and_swap(
        &self,
        key: &[u8],
        old: Option<&Buffer<'_>>,
        new: Option<Buffer<'_>>,
    ) -> Result<(), CompareAndSwapError> {
        let mut transaction = self.roots.transaction(&[Root::tree(self.name.clone())])?;
        transaction
            .tree::<Root>(0)
            .unwrap()
            .compare_and_swap(key, old, new)?;
        transaction.commit()?;
        Ok(())
    }

    /// Retrieves the values of `keys`. If any keys are not found, they will be
    /// omitted from the results.
    pub fn get_multiple(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<(Buffer<'static>, Buffer<'static>)>, Error> {
        catch_compaction_and_retry(|| {
            let mut tree = TreeFile::<Root, File>::read(
                self.path(),
                self.state.clone(),
                self.roots.context(),
                Some(self.roots.transactions()),
            )?;

            tree.get_multiple(keys, false)
        })
    }

    /// Retrieves all of the values of keys within `range`.
    pub fn get_range<'range, Range: RangeBounds<Buffer<'range>> + Clone + Debug + 'static>(
        &self,
        range: Range,
    ) -> Result<Vec<(Buffer<'static>, Buffer<'static>)>, Error> {
        catch_compaction_and_retry(|| {
            let mut tree = TreeFile::<Root, File>::read(
                self.path(),
                self.state.clone(),
                self.roots.context(),
                Some(self.roots.transactions()),
            )?;

            tree.get_range(range.clone(), false)
        })
    }

    /// Scans the tree. Each key that is contained `range` will be passed to
    /// `key_evaluator`, which can opt to read the data for the key, skip, or
    /// stop scanning. If `KeyEvaluation::ReadData` is returned, `callback` will
    /// be invoked with the key and stored value. The order in which `callback`
    /// is invoked is not necessarily the same order in which the keys are
    /// found.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(self, key_evaluator, callback))
    )]
    pub fn scan<'range, CallerError, Range, KeyEvaluator, DataCallback>(
        &self,
        range: Range,
        forwards: bool,
        mut key_evaluator: KeyEvaluator,
        mut callback: DataCallback,
    ) -> Result<(), AbortError<CallerError>>
    where
        Range: RangeBounds<Buffer<'range>> + Clone + Debug + 'static,
        KeyEvaluator: FnMut(&Buffer<'static>) -> KeyEvaluation,
        DataCallback:
            FnMut(Buffer<'static>, Buffer<'static>) -> Result<(), AbortError<CallerError>>,
        CallerError: Display + Debug,
    {
        catch_compaction_and_retry_abortable(move || {
            let mut tree = TreeFile::<Root, File>::read(
                self.path(),
                self.state.clone(),
                self.roots.context(),
                Some(self.roots.transactions()),
            )?;

            tree.scan(
                range.clone(),
                forwards,
                false,
                &mut key_evaluator,
                &mut callback,
            )
        })
    }

    /// Returns the last key of the tree.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn last_key(&self) -> Result<Option<Buffer<'static>>, Error> {
        catch_compaction_and_retry(|| {
            let mut tree = TreeFile::<Root, File>::read(
                self.path(),
                self.state.clone(),
                self.roots.context(),
                Some(self.roots.transactions()),
            )?;

            tree.last_key(false)
        })
    }

    /// Returns the last key and value of the tree.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn last(&self) -> Result<Option<(Buffer<'static>, Buffer<'static>)>, Error> {
        catch_compaction_and_retry(|| {
            let mut tree = TreeFile::<Root, File>::read(
                self.path(),
                self.state.clone(),
                self.roots.context(),
                Some(self.roots.transactions()),
            )?;

            tree.last(false)
        })
    }

    /// Rewrites the database to remove data that is no longer current. Because
    /// Nebari uses an append-only format, this is helpful in reducing disk
    /// usage.
    ///
    /// See [`TreeFile::compact()`](crate::tree::TreeFile::compact) for more
    /// information.
    pub fn compact(&self) -> Result<(), Error> {
        let tree = TreeFile::<Root, File>::read(
            self.path(),
            self.state.clone(),
            self.roots.context(),
            Some(self.roots.transactions()),
        )?;
        tree.compact(
            &self.roots.context().file_manager,
            Some(TransactableCompaction {
                name: self.name.as_ref(),
                manager: self.roots.transactions(),
            }),
        )?;
        Ok(())
    }
}

/// An error that could come from user code or Nebari.
#[derive(thiserror::Error, Debug)]
pub enum AbortError<CallerError: Display + Debug> {
    /// An error unrelated to Nebari occurred.
    #[error("other error: {0}")]
    Other(CallerError),
    /// An error from Roots occurred.
    #[error("database error: {0}")]
    Nebari(#[from] Error),
}

impl AbortError<Infallible> {
    /// Unwraps the error contained within an infallible abort error.
    #[must_use]
    pub fn infallible(self) -> Error {
        match self {
            AbortError::Other(_) => unreachable!(),
            AbortError::Nebari(error) => error,
        }
    }
}

/// A thread pool that commits transactions to disk in parallel.
#[derive(Debug)]
pub struct ThreadPool<File>
where
    File: ManagedFile,
{
    sender: flume::Sender<ThreadCommit<File>>,
    receiver: flume::Receiver<ThreadCommit<File>>,
    thread_count: Arc<AtomicU16>,
}

impl<File: ManagedFile> ThreadPool<File> {
    fn commit_trees(
        &self,
        mut trees: Vec<Box<dyn AnyTransactionTree<File>>>,
    ) -> Result<Vec<Box<dyn AnyTransactionTree<File>>>, Error> {
        static CPU_COUNT: Lazy<usize> = Lazy::new(num_cpus::get);

        // If we only have one tree, there's no reason to split IO across
        // threads. If we have multiple trees, we should split even with one
        // cpu: if one thread blocks, the other can continue executing.
        if trees.len() == 1 {
            trees[0].commit()?;
            Ok(trees)
        } else {
            // Push the trees so that any existing threads can begin processing the queue.
            let (completion_sender, completion_receiver) = flume::unbounded();
            let tree_count = trees.len();
            for tree in trees {
                self.sender.send(ThreadCommit {
                    tree,
                    completion_sender: completion_sender.clone(),
                })?;
            }

            // Scale the queue if needed.
            let desired_threads = tree_count.min(*CPU_COUNT);
            loop {
                let thread_count = self.thread_count.load(Ordering::SeqCst);
                if (thread_count as usize) >= desired_threads {
                    break;
                }

                // Spawn a thread, but ensure that we don't spin up too many threads if another thread is committing at the same time.
                if self
                    .thread_count
                    .compare_exchange(
                        thread_count,
                        thread_count + 1,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    let commit_receiver = self.receiver.clone();
                    std::thread::Builder::new()
                        .name(String::from("roots-txwriter"))
                        .spawn(move || transaction_commit_thread(commit_receiver))
                        .unwrap();
                }
            }

            // Wait for our results
            let mut results = Vec::with_capacity(tree_count);
            for _ in 0..tree_count {
                results.push(completion_receiver.recv()??);
            }

            Ok(results)
        }
    }
}

impl<File: ManagedFile> Clone for ThreadPool<File> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            receiver: self.receiver.clone(),
            thread_count: self.thread_count.clone(),
        }
    }
}

impl<File: ManagedFile> Default for ThreadPool<File> {
    fn default() -> Self {
        let (sender, receiver) = flume::unbounded();
        Self {
            sender,
            receiver,
            thread_count: Arc::new(AtomicU16::new(0)),
        }
    }
}

#[allow(clippy::needless_pass_by_value)]
fn transaction_commit_thread<File: ManagedFile>(receiver: flume::Receiver<ThreadCommit<File>>) {
    while let Ok(ThreadCommit {
        mut tree,
        completion_sender,
    }) = receiver.recv()
    {
        let result = tree.commit().map(move |_| tree);
        drop(completion_sender.send(result));
    }
}

struct ThreadCommit<File>
where
    File: ManagedFile,
{
    tree: Box<dyn AnyTransactionTree<File>>,
    completion_sender: Sender<Result<Box<dyn AnyTransactionTree<File>>, Error>>,
}

fn catch_compaction_and_retry<R, F: Fn() -> Result<R, Error>>(func: F) -> Result<R, Error> {
    loop {
        match func() {
            Ok(result) => return Ok(result),
            Err(error) => {
                if matches!(error.kind, ErrorKind::TreeCompacted) {
                    continue;
                }

                return Err(error);
            }
        }
    }
}

fn catch_compaction_and_retry_abortable<
    R,
    E: Display + Debug,
    F: FnMut() -> Result<R, AbortError<E>>,
>(
    mut func: F,
) -> Result<R, AbortError<E>> {
    loop {
        match func() {
            Ok(result) => return Ok(result),
            Err(AbortError::Nebari(error)) => {
                if matches!(error.kind, ErrorKind::TreeCompacted) {
                    continue;
                }

                return Err(AbortError::Nebari(error));
            }
            Err(other) => return Err(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use byteorder::{BigEndian, ByteOrder};
    use tempfile::tempdir;

    use super::*;
    use crate::{
        io::{fs::StdFile, memory::MemoryFile, ManagedFile},
        tree::{Root, UnversionedTreeRoot},
    };

    fn basic_get_set<File: ManagedFile>() {
        let tempdir = tempdir().unwrap();
        let roots = Config::<File>::new(tempdir.path()).open().unwrap();

        let tree = roots.tree::<VersionedTreeRoot, _>("test").unwrap();
        tree.set(b"test", b"value").unwrap();
        let result = tree.get(b"test").unwrap().expect("key not found");

        assert_eq!(result.as_slice(), b"value");
    }

    #[test]
    fn memory_basic_get_set() {
        basic_get_set::<MemoryFile>();
    }

    #[test]
    fn std_basic_get_set() {
        basic_get_set::<StdFile>();
    }

    #[test]
    fn basic_transaction_isolation_test() {
        let tempdir = tempdir().unwrap();

        let roots = Config::<StdFile>::new(tempdir.path()).open().unwrap();
        let tree = roots.tree::<VersionedTreeRoot, _>("test").unwrap();
        tree.set(b"test", b"value").unwrap();

        // Begin a transaction
        let mut transaction = roots
            .transaction(&[VersionedTreeRoot::tree("test")])
            .unwrap();

        // Replace the key with a new value.
        transaction
            .tree::<VersionedTreeRoot>(0)
            .unwrap()
            .set(b"test", b"updated value")
            .unwrap();

        // Check that the transaction can read the new value
        let result = transaction
            .tree::<VersionedTreeRoot>(0)
            .unwrap()
            .get(b"test")
            .unwrap()
            .expect("key not found");
        assert_eq!(result.as_slice(), b"updated value");

        // Ensure that existing read-access doesn't see the new value
        let result = tree.get(b"test").unwrap().expect("key not found");
        assert_eq!(result.as_slice(), b"value");

        // Commit the transaction
        transaction.commit().unwrap();

        // Ensure that the reader now sees the new value
        let result = tree.get(b"test").unwrap().expect("key not found");
        assert_eq!(result.as_slice(), b"updated value");
    }

    #[test]
    fn basic_transaction_rollback_test() {
        let tempdir = tempdir().unwrap();

        let roots = Config::<StdFile>::new(tempdir.path()).open().unwrap();
        let tree = roots.tree::<VersionedTreeRoot, _>("test").unwrap();
        tree.set(b"test", b"value").unwrap();

        // Begin a transaction
        let mut transaction = roots
            .transaction(&[VersionedTreeRoot::tree("test")])
            .unwrap();

        // Replace the key with a new value.
        transaction
            .tree::<VersionedTreeRoot>(0)
            .unwrap()
            .set(b"test", b"updated value")
            .unwrap();

        // Roll the transaction back
        drop(transaction);

        // Ensure that the reader still sees the old value
        let result = tree.get(b"test").unwrap().expect("key not found");
        assert_eq!(result.as_slice(), b"value");

        // Begin a new transaction
        let mut transaction = roots
            .transaction(&[VersionedTreeRoot::tree("test")])
            .unwrap();
        // Check that the transaction has the original value
        let result = transaction
            .tree::<VersionedTreeRoot>(0)
            .unwrap()
            .get(b"test")
            .unwrap()
            .expect("key not found");
        assert_eq!(result.as_slice(), b"value");
    }

    #[test]
    fn compact_test_versioned() {
        compact_test::<VersionedTreeRoot>();
    }

    #[test]
    fn compact_test_unversioned() {
        compact_test::<UnversionedTreeRoot>();
    }

    fn compact_test<R: Root>() {
        const OPERATION_COUNT: usize = 256;
        const WORKER_COUNT: usize = 4;
        let tempdir = tempdir().unwrap();

        let roots = Config::<StdFile>::new(tempdir.path()).open().unwrap();
        let tree = roots.tree::<R, _>("test").unwrap();
        tree.set("foo", b"bar").unwrap();

        // Spawn a pool of threads that will perform a series of operations
        let mut threads = Vec::new();
        for worker in 0..WORKER_COUNT {
            let tree = tree.clone();
            threads.push(std::thread::spawn(move || {
                for relative_id in 0..OPERATION_COUNT {
                    let absolute_id = (worker * OPERATION_COUNT + relative_id) as u64;
                    tree.set(absolute_id.to_be_bytes(), absolute_id.to_be_bytes())
                        .unwrap();
                    let value = tree
                        .remove(&absolute_id.to_be_bytes())
                        .unwrap()
                        .ok_or_else(|| panic!("value not found: {:?}", absolute_id))
                        .unwrap();
                    assert_eq!(BigEndian::read_u64(&value), absolute_id);
                    tree.set(absolute_id.to_be_bytes(), absolute_id.to_be_bytes())
                        .unwrap();
                    let newer_value = tree
                        .get(&absolute_id.to_be_bytes())
                        .unwrap()
                        .expect("couldn't find found");
                    assert_eq!(value, newer_value);
                }
            }));
        }

        threads.push(std::thread::spawn(move || {
            // While those workers are running, this thread is going to continually
            // execute compaction.
            while dbg!(tree.count()) < (OPERATION_COUNT * WORKER_COUNT) as u64 {
                tree.compact().unwrap();
            }
        }));

        for thread in threads {
            thread.join().unwrap();
        }
    }
}
