use std::{
    any::Any,
    borrow::{Borrow, Cow},
    collections::{hash_map, HashMap},
    fmt::{Debug, Display},
    fs,
    ops::{Deref, DerefMut, RangeBounds},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
};

use flume::Sender;
use once_cell::sync::Lazy;
use parking_lot::{MappedMutexGuard, Mutex, MutexGuard};
use sediment::{
    database::{CheckpointGuard, PendingCommit},
    io::{self, fs::StdFileManager, paths::PathId},
};

use crate::{
    context::Context,
    error::Error,
    storage::sediment::SedimentFile,
    transaction::{LogEntry, ManagedTransaction, TransactionId, TransactionManager},
    tree::{
        self,
        root::{AnyReducer, AnyTreeRoot},
        state::AnyTreeState,
        CommittedTreeState, EmbeddedIndex, KeySequence, Modification, ModificationResult,
        Operation, PersistenceMode, ScanEvaluation, SequenceEntry, SequenceId, SequenceIndex,
        State, TreeEntry, TreeFile, TreeRoot, TreeValueIndex, VersionedTreeRoot,
    },
    vault::AnyVault,
    AbortError, ArcBytes, ChunkCache, CompareAndSwapError, ErrorKind,
};

/// A multi-tree transactional B-Tree database.
#[derive(Debug)]
pub struct Roots<File: io::FileManager> {
    data: Arc<Data<File>>,
}

#[derive(Debug)]
struct Data<File: io::FileManager> {
    context: Context<File>,
    transactions: TransactionManager<File>,
    thread_pool: ThreadPool<File>,
    path: PathBuf,
    tree_states: Mutex<HashMap<String, Box<dyn AnyTreeState>>>,
    trees: Mutex<HashMap<String, SedimentFile<File>>>,
}

impl<File: io::FileManager> Roots<File> {
    fn open<P: Into<PathBuf> + Send>(
        path: P,
        context: Context<File>,
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

        let transactions =
            TransactionManager::spawn(&path, context.clone(), Some(thread_pool.clone()))?;
        Ok(Self {
            data: Arc::new(Data {
                context,
                path,
                transactions,
                thread_pool,
                tree_states: Mutex::default(),
                trees: Mutex::default(),
            }),
        })
    }

    /// Returns the path to the database directory.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.data.path
    }

    /// Returns the vault used to encrypt this database.
    pub fn context(&self) -> &Context<File> {
        &self.data.context
    }

    /// Returns the transaction manager for this database.
    #[must_use]
    pub fn transactions(&self) -> &TransactionManager<File> {
        &self.data.transactions
    }

    /// Opens a tree named `name`.
    ///
    /// ## Errors
    ///
    /// - [`InvalidTreeName`](ErrorKind::InvalidTreeName): The name contained an
    ///   invalid character. For a full list of valid characters, see the
    ///   documentation on [`InvalidTreeName`](ErrorKind::InvalidTreeName).
    pub fn tree<Root: tree::Root>(
        &self,
        root: TreeRoot<Root, File>,
    ) -> Result<Tree<Root, File>, Error> {
        check_name(&root.name)?;
        let database = self.tree_file(&root.name)?;
        let state = self.tree_state(root.clone());
        Ok(Tree {
            roots: self.clone(),
            database,
            state,
            vault: root.vault,
            reducer: root.reducer,
            name: root.name,
        })
    }

    fn tree_file(&self, name: &str) -> Result<SedimentFile<File>, Error> {
        let mut trees = self.data.trees.lock();
        if let Some(db) = trees.get(name) {
            Ok(db.clone())
        } else {
            let path = self.tree_path(name);
            let db = SedimentFile::open(&path, true, self.data.context.file_manager.clone())?;
            trees.insert(name.to_string(), db.clone());
            Ok(db)
        }
    }

    fn tree_path(&self, name: &str) -> PathId {
        self.context()
            .file_manager
            .resolve_path(self.path().join(format!("{}.nebari", name)))
    }

    /// Removes a tree. Returns true if a tree was deleted.
    pub fn delete_tree(&self, name: impl Into<Cow<'static, str>>) -> Result<bool, Error> {
        let name = name.into();
        let mut tree_states = self.data.tree_states.lock();
        self.context()
            .file_manager
            .delete(&self.tree_path(name.as_ref()))?;
        Ok(tree_states.remove(name.as_ref()).is_some())
    }

    /// Returns a list of all the names of trees contained in this database.
    pub fn tree_names(&self) -> Result<Vec<String>, Error> {
        let mut names = Vec::new();
        // TODO use the file manager
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

    fn tree_state<Root: tree::Root>(&self, root: TreeRoot<Root, File>) -> State<Root> {
        self.tree_states(&[root])
            .into_iter()
            .next()
            .unwrap()
            .as_ref()
            .as_any()
            .downcast_ref::<State<Root>>()
            .unwrap()
            .clone()
    }

    fn tree_states<R: Borrow<T>, T: AnyTreeRoot<File> + ?Sized>(
        &self,
        trees: &[R],
    ) -> Vec<Box<dyn AnyTreeState>> {
        let mut tree_states = self.data.tree_states.lock();
        let mut output = Vec::with_capacity(trees.len());
        for tree in trees {
            let state = tree_states
                .entry(tree.borrow().name().to_string())
                .or_insert_with(|| tree.borrow().default_state())
                .cloned();
            output.push(state);
        }
        output
    }

    /// Begins a transaction over `trees`. All trees will be exclusively
    /// accessible by the transaction. Dropping the executing transaction will
    /// roll the transaction back.
    ///
    /// ## Errors
    ///
    /// - [`InvalidTreeName`](ErrorKind::InvalidTreeName): A tree name contained
    ///   an invalid character. For a full list of valid characters, see the
    ///   documentation on [`InvalidTreeName`](ErrorKind::InvalidTreeName).
    pub fn transaction<R: Borrow<T>, T: AnyTreeRoot<File> + ?Sized>(
        &self,
        trees: &[R],
    ) -> Result<ExecutingTransaction<File>, Error> {
        let mut files = Vec::with_capacity(trees.len());
        for tree in trees {
            check_name(tree.borrow().name()).map(|_| tree.borrow().name().as_bytes())?;
            files.push(self.tree_file(tree.borrow().name())?);
        }
        let transaction = self
            .data
            .transactions
            .new_transaction(trees.iter().map(|t| t.borrow().name().as_bytes()));
        let states = self.tree_states(trees);
        let trees = trees
            .iter()
            .zip(states)
            .zip(files)
            .map(|((tree, state), file)| {
                tree.borrow()
                    .begin_transaction(
                        transaction.id,
                        file,
                        state.as_ref(),
                        self.context(),
                        Some(&self.data.transactions),
                    )
                    .map(UnlockedTransactionTree::new)
            })
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(ExecutingTransaction {
            roots: self.clone(),
            transaction: Some(transaction),
            trees,
        })
    }
}

fn check_name(name: &str) -> Result<(), Error> {
    if name != "_transactions"
        && name
            .bytes()
            .all(|c| matches!(c as char, 'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '.' | '_'))
    {
        Ok(())
    } else {
        Err(Error::from(ErrorKind::InvalidTreeName))
    }
}

impl<File: io::FileManager> Clone for Roots<File> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
        }
    }
}

/// An executing transaction. While this exists, no other transactions can
/// execute across the same trees as this transaction holds.
#[must_use]
pub struct ExecutingTransaction<File: io::FileManager> {
    roots: Roots<File>,
    trees: Vec<UnlockedTransactionTree<File>>,
    transaction: Option<ManagedTransaction<File>>,
}

/// A tree that belongs to an [`ExecutingTransaction`].
#[must_use]
pub struct UnlockedTransactionTree<File: io::FileManager>(Mutex<Box<dyn AnyTransactionTree<File>>>);

impl<File: io::FileManager> UnlockedTransactionTree<File> {
    fn new(file: Box<dyn AnyTransactionTree<File>>) -> Self {
        Self(Mutex::new(file))
    }

    /// Locks this tree so that operations can be performed against it.
    ///
    /// # Panics
    ///
    /// This function panics if `Root` does not match the type specified when
    /// starting the transaction.
    pub fn lock<Root: tree::Root>(&self) -> LockedTransactionTree<'_, Root, File> {
        LockedTransactionTree(MutexGuard::map(self.0.lock(), |tree| {
            tree.as_mut().as_any_mut().downcast_mut().unwrap()
        }))
    }
}

/// A locked transaction tree. This transactional tree is exclusively available
/// for writing and reading to the thread that locks it.
#[must_use]
pub struct LockedTransactionTree<'transaction, Root: tree::Root, File: io::FileManager>(
    MappedMutexGuard<'transaction, TransactionTree<Root, File>>,
);

impl<'transaction, Root: tree::Root, File: io::FileManager> Deref
    for LockedTransactionTree<'transaction, Root, File>
{
    type Target = TransactionTree<Root, File>;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl<'transaction, Root: tree::Root, File: io::FileManager> DerefMut
    for LockedTransactionTree<'transaction, Root, File>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.0
    }
}

impl<File: io::FileManager> ExecutingTransaction<File> {
    /// Returns the [`LogEntry`] for this transaction.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn entry(&self) -> &LogEntry<'static> {
        self.transaction
            .as_ref()
            .and_then(|tx| tx.transaction.as_ref())
            .unwrap()
    }

    /// Returns a mutable reference to the [`LogEntry`] for this transaction.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn entry_mut(&mut self) -> &mut LogEntry<'static> {
        self.transaction
            .as_mut()
            .and_then(|tx| tx.transaction.as_mut())
            .unwrap()
    }

    /// Commits the transaction. Once this function has returned, all data
    /// updates are guaranteed to be able to be accessed by all other readers as
    /// well as impervious to sudden failures such as a power outage.
    #[allow(clippy::missing_panics_doc)]
    pub fn commit(mut self) -> Result<(), Error> {
        let trees = std::mem::take(&mut self.trees);
        // Write the trees to disk
        let commit_states = self.roots.data.thread_pool.commit_trees(trees)?;

        // Push the transaction to the log. This releases other writers to this
        // tree. The transaction manager will publish the new state of each tree
        // once the transaction is confirmed.
        let transaction = self.transaction.take().unwrap();
        transaction.commit(commit_states)?;

        Ok(())
    }

    /// Rolls the transaction back. It is not necessary to call this function --
    /// transactions will automatically be rolled back when the transaction is
    /// dropped, if `commit()` isn't called first.
    pub fn rollback(self) {
        drop(self);
    }

    /// Accesses a locked tree.
    pub fn tree<Root: tree::Root>(
        &self,
        index: usize,
    ) -> Option<LockedTransactionTree<'_, Root, File>> {
        self.unlocked_tree(index).map(UnlockedTransactionTree::lock)
    }

    /// Accesses an unlocked tree. Note: If you clone an
    /// [`UnlockedTransactionTree`], you must make sure to drop all instances
    /// before calling commit.
    #[must_use]
    pub fn unlocked_tree(&self, index: usize) -> Option<&UnlockedTransactionTree<File>> {
        self.trees.get(index)
    }

    fn rollback_tree_states(&mut self) {
        for tree in self.trees.drain(..) {
            let tree = tree.0.lock();
            tree.rollback();
        }
    }
}

impl<File: io::FileManager> Drop for ExecutingTransaction<File> {
    fn drop(&mut self) {
        if let Some(transaction) = self.transaction.take() {
            self.rollback_tree_states();
            // Now the transaction can be dropped safely, freeing up access to the trees.
            drop(transaction);
        }
    }
}

/// A tree that is modifiable during a transaction.
pub struct TransactionTree<Root: tree::Root, File: io::FileManager> {
    pub(crate) transaction_id: TransactionId,
    /// The underlying tree file.
    pub tree: TreeFile<Root, File>,
}

pub trait AnyTransactionTree<File: io::FileManager>: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;

    fn state(&self) -> Box<dyn AnyTreeState>;

    fn commit(&mut self) -> Result<Option<CommittedTreeState<File>>, Error>;
    fn rollback(&self);
}

impl<Root: tree::Root, File: io::FileManager> AnyTransactionTree<File>
    for TransactionTree<Root, File>
{
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn state(&self) -> Box<dyn AnyTreeState> {
        Box::new(self.tree.state.clone())
    }

    fn commit(&mut self) -> Result<Option<CommittedTreeState<File>>, Error> {
        self.tree.begin_commit()
    }

    fn rollback(&self) {
        let mut state = self.tree.state.lock();
        state.rollback(&self.tree.state);
    }
}

impl<File: io::FileManager, Index> TransactionTree<VersionedTreeRoot<Index>, File>
where
    Index: Clone + EmbeddedIndex<ArcBytes<'static>> + Debug + 'static,
{
    /// Returns the latest sequence id.
    #[must_use]
    pub fn current_sequence_id(&self) -> SequenceId {
        let state = self.tree.state.lock();
        state.root.sequence
    }

    /// Scans the tree for keys that are contained within `range`. If `forwards`
    /// is true, scanning starts at the lowest sort-order key and scans forward.
    /// Otherwise, scanning starts at the highest sort-order key and scans
    /// backwards. `key_evaluator` is invoked for each key as it is encountered.
    /// For all [`ScanEvaluation::ReadData`] results returned, `callback` will be
    /// invoked with the key and values. The callback may not be invoked in the
    /// same order as the keys are scanned.
    pub fn scan_sequences<CallerError, Range, KeyEvaluator, DataCallback>(
        &mut self,
        range: Range,
        forwards: bool,
        key_evaluator: &mut KeyEvaluator,
        data_callback: &mut DataCallback,
    ) -> Result<(), AbortError<CallerError>>
    where
        Range: RangeBounds<SequenceId> + Debug + 'static,
        KeyEvaluator: FnMut(KeySequence<Index>) -> ScanEvaluation,
        DataCallback:
            FnMut(KeySequence<Index>, ArcBytes<'static>) -> Result<(), AbortError<CallerError>>,
        CallerError: Display + Debug,
    {
        self.tree
            .scan_sequences(range, forwards, true, key_evaluator, data_callback)
    }

    /// Retrieves the keys and values associated with one or more `sequences`.
    /// The value retrieved is the value of the key at the given [`SequenceId`].
    /// If a sequence is not found, it will not appear in the result map. If
    /// the value was removed, None is returned for the value.
    pub fn get_multiple_by_sequence<Sequences>(
        &mut self,
        sequences: Sequences,
    ) -> Result<HashMap<SequenceId, (ArcBytes<'static>, Option<ArcBytes<'static>>)>, Error>
    where
        Sequences: Iterator<Item = SequenceId>,
    {
        self.tree.get_multiple_by_sequence(sequences, true)
    }

    /// Retrieves the keys and indexes associated with one or more `sequences`.
    /// The value retrieved is the value of the key at the given [`SequenceId`].
    /// If a sequence is not found, it will not appear in the result list.
    pub fn get_multiple_indexes_by_sequence<Sequences>(
        &mut self,
        sequences: Sequences,
    ) -> Result<Vec<SequenceIndex<Index>>, Error>
    where
        Sequences: Iterator<Item = SequenceId>,
    {
        self.tree.get_multiple_indexes_by_sequence(sequences, true)
    }

    /// Retrieves the keys, values, and indexes associated with one or more
    /// `sequences`. The value retrieved is the value of the key at the given
    /// [`SequenceId`]. If a sequence is not found, it will not appear in the
    /// result list.
    pub fn get_multiple_with_indexes_by_sequence<Sequences>(
        &mut self,
        sequences: Sequences,
    ) -> Result<HashMap<SequenceId, SequenceEntry<Index>>, Error>
    where
        Sequences: Iterator<Item = SequenceId>,
    {
        self.tree
            .get_multiple_with_indexes_by_sequence(sequences, true)
    }
}

impl<Root: tree::Root, File: io::FileManager> TransactionTree<Root, File> {
    /// Sets `key` to `value`. Returns the newly created index for this key.
    pub fn set(
        &mut self,
        key: impl Into<ArcBytes<'static>>,
        value: impl Into<Root::Value>,
    ) -> Result<Root::Index, Error> {
        self.tree.set(
            PersistenceMode::Transactional(self.transaction_id),
            key,
            value,
        )
    }

    /// Executes a modification. Returns a list of all changed keys.
    pub fn modify<'a>(
        &mut self,
        keys: Vec<ArcBytes<'a>>,
        operation: Operation<'a, Root::Value, Root::Index>,
    ) -> Result<Vec<ModificationResult<Root::Index>>, Error> {
        self.tree.modify(Modification {
            keys,
            persistence_mode: PersistenceMode::Transactional(self.transaction_id),
            operation,
        })
    }

    /// Sets `key` to `value`. Returns a tuple containing two elements:
    ///
    /// - The previously stored value, if a value was already present.
    /// - The new/updated index for this key.
    pub fn replace(
        &mut self,
        key: impl Into<ArcBytes<'static>>,
        value: impl Into<Root::Value>,
    ) -> Result<(Option<Root::Value>, Root::Index), Error> {
        self.tree.replace(key, value, self.transaction_id)
    }

    /// Returns the current value of `key`. This will return updated information
    /// if it has been previously updated within this transaction.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Root::Value>, Error> {
        self.tree.get(key, true)
    }

    /// Returns the current index of `key`. This will return updated information
    /// if it has been previously updated within this transaction.
    pub fn get_index(&mut self, key: &[u8]) -> Result<Option<Root::Index>, Error> {
        self.tree.get_index(key, true)
    }

    /// Returns the current value and index of `key`. This will return updated
    /// information if it has been previously updated within this transaction.
    pub fn get_with_index(&mut self, key: &[u8]) -> Result<Option<TreeValueIndex<Root>>, Error> {
        self.tree.get_with_index(key, true)
    }

    /// Removes `key` and returns the existing value amd index, if present.
    pub fn remove(&mut self, key: &[u8]) -> Result<Option<TreeValueIndex<Root>>, Error> {
        self.tree.remove(key, self.transaction_id)
    }

    /// Compares the value of `key` against `old`. If the values match, key will
    /// be set to the new value if `new` is `Some` or removed if `new` is
    /// `None`.
    pub fn compare_and_swap<Old>(
        &mut self,
        key: &[u8],
        old: Option<&Old>,
        new: Option<Root::Value>,
    ) -> Result<(), CompareAndSwapError<Root::Value>>
    where
        Old: PartialEq,
        Root::Value: AsRef<Old> + Clone,
    {
        self.tree
            .compare_and_swap(key, old, new, self.transaction_id)
    }

    /// Retrieves the values of `keys`. If any keys are not found, they will be
    /// omitted from the results. Keys are required to be pre-sorted.
    pub fn get_multiple<'keys, KeysIntoIter, KeysIter>(
        &mut self,
        keys: KeysIntoIter,
    ) -> Result<Vec<(ArcBytes<'static>, Root::Value)>, Error>
    where
        KeysIntoIter: IntoIterator<Item = &'keys [u8], IntoIter = KeysIter>,
        KeysIter: Iterator<Item = &'keys [u8]> + ExactSizeIterator,
    {
        self.tree.get_multiple(keys, true)
    }

    /// Retrieves the indexes of `keys`. If any keys are not found, they will be
    /// omitted from the results. Keys are required to be pre-sorted.
    pub fn get_multiple_indexes<'keys, KeysIntoIter, KeysIter>(
        &mut self,
        keys: KeysIntoIter,
    ) -> Result<Vec<(ArcBytes<'static>, Root::Index)>, Error>
    where
        KeysIntoIter: IntoIterator<Item = &'keys [u8], IntoIter = KeysIter>,
        KeysIter: Iterator<Item = &'keys [u8]> + ExactSizeIterator,
    {
        self.tree.get_multiple_indexes(keys, true)
    }

    /// Retrieves the values and indexes of `keys`. If any keys are not found,
    /// they will be omitted from the results. Keys are required to be
    /// pre-sorted.
    pub fn get_multiple_with_indexes<'keys, KeysIntoIter, KeysIter>(
        &mut self,
        keys: KeysIntoIter,
    ) -> Result<Vec<TreeEntry<Root>>, Error>
    where
        KeysIntoIter: IntoIterator<Item = &'keys [u8], IntoIter = KeysIter>,
        KeysIter: Iterator<Item = &'keys [u8]> + ExactSizeIterator,
    {
        self.tree.get_multiple_with_indexes(keys, true)
    }

    /// Retrieves all of the values of keys within `range`.
    pub fn get_range<'keys, KeyRangeBounds>(
        &mut self,
        range: &'keys KeyRangeBounds,
    ) -> Result<Vec<(ArcBytes<'static>, Root::Value)>, Error>
    where
        KeyRangeBounds: RangeBounds<&'keys [u8]> + Debug + ?Sized,
    {
        self.tree.get_range(range, true)
    }

    /// Retrieves all of the indexes of keys within `range`.
    pub fn get_range_indexes<'keys, KeyRangeBounds>(
        &mut self,
        range: &'keys KeyRangeBounds,
    ) -> Result<Vec<(ArcBytes<'static>, Root::Index)>, Error>
    where
        KeyRangeBounds: RangeBounds<&'keys [u8]> + Debug + ?Sized,
    {
        self.tree.get_range_indexes(range, true)
    }

    /// Retrieves all of the values and indexes of keys within `range`.
    pub fn get_range_with_indexes<'keys, KeyRangeBounds>(
        &mut self,
        range: &'keys KeyRangeBounds,
    ) -> Result<Vec<TreeEntry<Root>>, Error>
    where
        KeyRangeBounds: RangeBounds<&'keys [u8]> + Debug + ?Sized,
    {
        self.tree.get_range_with_indexes(range, true)
    }

    /// Scans the tree across all nodes that might contain nodes within `range`.
    ///
    /// If `forwards` is true, the tree is scanned in ascending order.
    /// Otherwise, the tree is scanned in descending order.
    ///
    /// `node_evaluator` is invoked for each [`Interior`](crate::tree::Interior)
    /// node to determine if the node should be traversed. The parameters to the
    /// callback are:
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
        tracing::instrument(skip(self, node_evaluator, key_evaluator, callback))
    )]
    pub fn scan<'b, 'keys, CallerError, KeyRangeBounds, NodeEvaluator, KeyEvaluator, DataCallback>(
        &mut self,
        range: &'keys KeyRangeBounds,
        forwards: bool,
        mut node_evaluator: NodeEvaluator,
        mut key_evaluator: KeyEvaluator,
        mut callback: DataCallback,
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
        self.tree.scan(
            range,
            forwards,
            true,
            &mut node_evaluator,
            &mut key_evaluator,
            &mut callback,
        )
    }

    /// Returns the reduced index over the provided range. This is an
    /// aggregation function that builds atop the `scan()` operation which calls
    /// [`Reducer::reduce()`](crate::tree::Reducer::reduce) and
    /// [`Reducer::rereduce()`](crate::tree::Reducer::rereduce) on all matching
    /// indexes stored within the nodes of this tree, producing a single
    /// aggregated [`Root::ReducedIndex`](tree::Root::ReducedIndex) value.
    ///
    /// If no keys match, the returned result is what
    /// [`Reducer::rereduce()`](crate::tree::Reducer::rereduce) returns when an
    /// empty slice is provided.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn reduce<'keys, KeyRangeBounds>(
        &mut self,
        range: &'keys KeyRangeBounds,
    ) -> Result<Option<Root::ReducedIndex>, Error>
    where
        KeyRangeBounds: RangeBounds<&'keys [u8]> + Debug + Clone + ?Sized,
    {
        self.tree.reduce(range, true)
    }

    /// Returns the first key of the tree.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn first_key(&mut self) -> Result<Option<ArcBytes<'static>>, Error> {
        self.tree.first_key(true)
    }

    /// Returns the first key and value of the tree.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn first(&mut self) -> Result<Option<(ArcBytes<'static>, Root::Value)>, Error> {
        self.tree.first(true)
    }

    /// Returns the last key of the tree.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn last_key(&mut self) -> Result<Option<ArcBytes<'static>>, Error> {
        self.tree.last_key(true)
    }

    /// Returns the last key and value of the tree.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn last(&mut self) -> Result<Option<(ArcBytes<'static>, Root::Value)>, Error> {
        self.tree.last(true)
    }
}

/// A database configuration used to open a database.
#[derive(Debug)]
#[must_use]
pub struct Config<M: io::FileManager> {
    path: PathBuf,
    vault: Option<Arc<dyn AnyVault>>,
    cache: Option<ChunkCache>,
    file_manager: Option<M>,
    thread_pool: Option<ThreadPool<M>>,
}

impl<M: io::FileManager> Clone for Config<M> {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            vault: self.vault.clone(),
            cache: self.cache.clone(),
            file_manager: self.file_manager.clone(),
            thread_pool: self.thread_pool.clone(),
        }
    }
}

impl Config<StdFileManager> {
    /// Creates a new config to open a database located at `path`.
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            vault: None,
            cache: None,
            thread_pool: None,
            file_manager: None,
        }
    }

    /// Returns a default configuration to open a database located at `path`.
    pub fn default_for<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            vault: None,
            cache: Some(ChunkCache::new(2000, 65536)),
            thread_pool: Some(ThreadPool::default()),
            file_manager: None,
        }
    }

    /// Sets the file manager.
    ///
    /// ## Panics
    ///
    /// Panics if called after a shared thread pool has been set.
    pub fn file_manager<M: io::FileManager>(self, file_manager: M) -> Config<M> {
        assert!(self.thread_pool.is_none());
        Config {
            path: self.path,
            vault: self.vault,
            cache: self.cache,
            file_manager: Some(file_manager),
            thread_pool: None,
        }
    }
}

impl<M: io::FileManager> Config<M> {
    /// Sets the vault to use for this database.
    pub fn vault<V: AnyVault>(mut self, vault: V) -> Self {
        self.vault = Some(Arc::new(vault));
        self
    }

    /// Sets the chunk cache to use for this database.
    #[allow(clippy::missing_const_for_fn)] // destructors
    pub fn cache(mut self, cache: ChunkCache) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Uses the `thread_pool` provided instead of creating its own. This will
    /// allow a single thread pool to manage multiple [`Roots`] instances'
    /// transactions.
    pub fn shared_thread_pool(mut self, thread_pool: &ThreadPool<M>) -> Self {
        self.thread_pool = Some(thread_pool.clone());
        self
    }

    /// Opens the database, or creates one if the target path doesn't exist.
    pub fn open(self) -> Result<Roots<M>, Error> {
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
pub struct Tree<Root: tree::Root, File: io::FileManager> {
    roots: Roots<File>,
    database: SedimentFile<File>,
    state: State<Root>,
    reducer: Arc<dyn AnyReducer>,
    vault: Option<Arc<dyn AnyVault>>,
    name: Cow<'static, str>,
}

impl<Root: tree::Root, File: io::FileManager> Clone for Tree<Root, File> {
    fn clone(&self) -> Self {
        Self {
            roots: self.roots.clone(),
            database: self.database.clone(),
            state: self.state.clone(),
            vault: self.vault.clone(),
            reducer: self.reducer.clone(),
            name: self.name.clone(),
        }
    }
}

impl<Root: tree::Root, File: io::FileManager> Tree<Root, File> {
    /// Returns the name of the tree.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the path to the file for this tree.
    #[must_use]
    pub fn path(&self) -> &Path {
        self.database.db.path()
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
        key: impl Into<ArcBytes<'static>>,
        value: impl Into<Root::Value>,
    ) -> Result<(), Error> {
        let transaction = self.begin_transaction()?;
        transaction.tree::<Root>(0).unwrap().set(key, value)?;
        transaction.commit()
    }

    fn begin_transaction(&self) -> Result<ExecutingTransaction<File>, Error> {
        let reducer = self
            .reducer
            .as_ref()
            .as_any()
            .downcast_ref::<Root::Reducer>()
            .unwrap()
            .clone();
        let mut root = Root::tree_with_reducer(self.name.clone(), reducer);
        if let Some(vault) = &self.vault {
            root.vault = Some(vault.clone());
        }
        self.roots.transaction(&[root])
    }

    /// Returns a [`TreeFile`] for lower-level operations within the context of
    /// Roots.
    ///
    /// Using this direct access, it is possible to circumvent some of the
    /// safety provided by Roots (e.g., passing an incorrect value for
    /// `in_transaction`). This function is provided for those who are
    /// implementing custom roots and wish to expose functionality through
    /// Roots.
    pub fn open_for_read(&self) -> Result<TreeFile<Root, File>, Error> {
        self.open_for_write()
    }

    /// Returns a [`TreeFile`] for lower-level operations within the context of
    /// Roots.
    ///
    /// Using this direct access, it is possible to circumvent some of the
    /// safety provided by Roots (e.g., passing an incorrect value for
    /// `in_transaction`). This function is provided for those who are
    /// implementing custom roots and wish to expose functionality through
    /// Roots.
    pub fn open_for_write(&self) -> Result<TreeFile<Root, File>, Error> {
        let context = self.vault.as_ref().map_or_else(
            || Cow::Borrowed(self.roots.context()),
            |vault| Cow::Owned(self.roots.context().clone().with_any_vault(vault.clone())),
        );

        TreeFile::<Root, File>::open_sediment_file(
            self.database.clone(),
            self.state.clone(),
            &context,
            Some(self.roots.transactions()),
        )
    }

    /// Retrieves the current value of `key`, if present. Does not reflect any
    /// changes in pending transactions.
    pub fn get(&self, key: &[u8]) -> Result<Option<Root::Value>, Error> {
        catch_compaction_and_retry(|| {
            let mut tree = match self.open_for_read() {
                Ok(tree) => tree,
                Err(err) if err.kind.is_file_not_found() => return Ok(None),
                Err(err) => return Err(err),
            };

            tree.get(key, false)
        })
    }

    /// Retrieves the current index of `key`, if present. Does not reflect any
    /// changes in pending transactions.
    pub fn get_index(&self, key: &[u8]) -> Result<Option<Root::Index>, Error> {
        catch_compaction_and_retry(|| {
            let mut tree = match self.open_for_read() {
                Ok(tree) => tree,
                Err(err) if err.kind.is_file_not_found() => return Ok(None),
                Err(err) => return Err(err),
            };

            tree.get_index(key, false)
        })
    }

    /// Retrieves the current value and index of `key`, if present. Does not reflect any
    /// changes in pending transactions.
    pub fn get_with_index(&self, key: &[u8]) -> Result<Option<TreeValueIndex<Root>>, Error> {
        catch_compaction_and_retry(|| {
            let mut tree = match self.open_for_read() {
                Ok(tree) => tree,
                Err(err) if err.kind.is_file_not_found() => return Ok(None),
                Err(err) => return Err(err),
            };

            tree.get_with_index(key, false)
        })
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
    ) -> Result<(Option<Root::Value>, Root::Index), Error> {
        let transaction = self.begin_transaction()?;
        let existing_value = transaction.tree::<Root>(0).unwrap().replace(key, value)?;
        transaction.commit()?;
        Ok(existing_value)
    }

    /// Executes a modification. Returns a list of all changed keys.
    #[allow(clippy::missing_panics_doc)]
    pub fn modify<'a>(
        &mut self,
        keys: Vec<ArcBytes<'a>>,
        operation: Operation<'a, Root::Value, Root::Index>,
    ) -> Result<Vec<ModificationResult<Root::Index>>, Error> {
        let transaction = self.begin_transaction()?;
        let results = transaction
            .tree::<Root>(0)
            .unwrap()
            .modify(keys, operation)?;
        transaction.commit()?;
        Ok(results)
    }

    /// Removes `key` and returns the existing value and index, if present. This
    /// is executed within its own transaction.
    #[allow(clippy::missing_panics_doc)]
    pub fn remove(&self, key: &[u8]) -> Result<Option<TreeValueIndex<Root>>, Error> {
        let transaction = self.begin_transaction()?;
        let existing_value = transaction.tree::<Root>(0).unwrap().remove(key)?;
        transaction.commit()?;
        Ok(existing_value)
    }

    /// Compares the value of `key` against `old`. If the values match, key will
    /// be set to the new value if `new` is `Some` or removed if `new` is
    /// `None`. This is executed within its own transaction.
    #[allow(clippy::missing_panics_doc)]
    pub fn compare_and_swap<Old>(
        &self,
        key: &[u8],
        old: Option<&Old>,
        new: Option<Root::Value>,
    ) -> Result<(), CompareAndSwapError<Root::Value>>
    where
        Old: PartialEq,
        Root::Value: AsRef<Old> + Clone,
    {
        let transaction = self.begin_transaction()?;
        transaction
            .tree::<Root>(0)
            .unwrap()
            .compare_and_swap(key, old, new)?;
        transaction.commit()?;
        Ok(())
    }

    /// Retrieves the values of `keys`. If any keys are not found, they will be
    /// omitted from the results. Keys are required to be pre-sorted.
    #[allow(clippy::needless_pass_by_value)]
    pub fn get_multiple<'keys, Keys>(
        &self,
        keys: Keys,
    ) -> Result<Vec<(ArcBytes<'static>, Root::Value)>, Error>
    where
        Keys: Iterator<Item = &'keys [u8]> + ExactSizeIterator + Clone,
    {
        catch_compaction_and_retry(|| {
            let mut tree = match self.open_for_read() {
                Ok(tree) => tree,
                Err(err) if err.kind.is_file_not_found() => return Ok(Vec::new()),
                Err(err) => return Err(err),
            };

            tree.get_multiple(keys.clone(), false)
        })
    }

    /// Retrieves the indexes of `keys`. If any keys are not found, they will be
    /// omitted from the results. Keys are required to be pre-sorted.
    #[allow(clippy::needless_pass_by_value)]
    pub fn get_multiple_indexes<'keys, KeysIntoIter, KeysIter>(
        &self,
        keys: KeysIntoIter,
    ) -> Result<Vec<(ArcBytes<'static>, Root::Index)>, Error>
    where
        KeysIntoIter: IntoIterator<Item = &'keys [u8], IntoIter = KeysIter> + Clone,
        KeysIter: Iterator<Item = &'keys [u8]> + ExactSizeIterator,
    {
        catch_compaction_and_retry(|| {
            let mut tree = match self.open_for_read() {
                Ok(tree) => tree,
                Err(err) if err.kind.is_file_not_found() => return Ok(Vec::new()),
                Err(err) => return Err(err),
            };

            tree.get_multiple_indexes(keys.clone(), false)
        })
    }

    /// Retrieves the values and indexes of `keys`. If any keys are not found,
    /// they will be omitted from the results. Keys are required to be
    /// pre-sorted.
    #[allow(clippy::needless_pass_by_value)]
    pub fn get_multiple_with_indexes<'keys, KeysIntoIter, KeysIter>(
        &self,
        keys: KeysIntoIter,
    ) -> Result<Vec<TreeEntry<Root>>, Error>
    where
        KeysIntoIter: IntoIterator<Item = &'keys [u8], IntoIter = KeysIter> + Clone,
        KeysIter: Iterator<Item = &'keys [u8]> + ExactSizeIterator,
    {
        catch_compaction_and_retry(|| {
            let mut tree = match self.open_for_read() {
                Ok(tree) => tree,
                Err(err) if err.kind.is_file_not_found() => return Ok(Vec::new()),
                Err(err) => return Err(err),
            };

            tree.get_multiple_with_indexes(keys.clone(), false)
        })
    }

    /// Retrieves all of the values of keys within `range`.
    pub fn get_range<'keys, KeyRangeBounds>(
        &self,
        range: &'keys KeyRangeBounds,
    ) -> Result<Vec<(ArcBytes<'static>, Root::Value)>, Error>
    where
        KeyRangeBounds: RangeBounds<&'keys [u8]> + Debug + Clone + ?Sized,
    {
        catch_compaction_and_retry(|| {
            let mut tree = match self.open_for_read() {
                Ok(tree) => tree,
                Err(err) if err.kind.is_file_not_found() => return Ok(Vec::new()),
                Err(err) => return Err(err),
            };

            tree.get_range(range, false)
        })
    }

    /// Retrieves all of the indexes of keys within `range`.
    pub fn get_range_indexes<'keys, KeyRangeBounds>(
        &self,
        range: &'keys KeyRangeBounds,
    ) -> Result<Vec<(ArcBytes<'static>, Root::Index)>, Error>
    where
        KeyRangeBounds: RangeBounds<&'keys [u8]> + Debug + ?Sized,
    {
        catch_compaction_and_retry(|| {
            let mut tree = match self.open_for_read() {
                Ok(tree) => tree,
                Err(err) if err.kind.is_file_not_found() => return Ok(Vec::new()),
                Err(err) => return Err(err),
            };

            tree.get_range_indexes(range, false)
        })
    }

    /// Retrieves all of the values and indexes of keys within `range`.
    pub fn get_range_with_indexes<'keys, KeyRangeBounds>(
        &self,
        range: &'keys KeyRangeBounds,
    ) -> Result<Vec<TreeEntry<Root>>, Error>
    where
        KeyRangeBounds: RangeBounds<&'keys [u8]> + Debug + ?Sized,
    {
        catch_compaction_and_retry(|| {
            let mut tree = match self.open_for_read() {
                Ok(tree) => tree,
                Err(err) if err.kind.is_file_not_found() => return Ok(Vec::new()),
                Err(err) => return Err(err),
            };

            tree.get_range_with_indexes(range, false)
        })
    }

    /// Scans the tree across all nodes that might contain nodes within `range`.
    ///
    /// If `forwards` is true, the tree is scanned in ascending order.
    /// Otherwise, the tree is scanned in descending order.
    ///
    /// `node_evaluator` is invoked for each [`Interior`](crate::tree::Interior) node to determine if
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
        tracing::instrument(skip(self, node_evaluator, key_evaluator, callback))
    )]
    pub fn scan<'keys, CallerError, KeyRangeBounds, NodeEvaluator, KeyEvaluator, DataCallback>(
        &self,
        range: &'keys KeyRangeBounds,
        forwards: bool,
        mut node_evaluator: NodeEvaluator,
        mut key_evaluator: KeyEvaluator,
        mut callback: DataCallback,
    ) -> Result<(), AbortError<CallerError>>
    where
        KeyRangeBounds: RangeBounds<&'keys [u8]> + Debug + Clone + ?Sized,
        NodeEvaluator: FnMut(&ArcBytes<'static>, &Root::ReducedIndex, usize) -> ScanEvaluation,
        KeyEvaluator: FnMut(&ArcBytes<'static>, &Root::Index) -> ScanEvaluation,
        DataCallback: FnMut(
            ArcBytes<'static>,
            &Root::Index,
            Root::Value,
        ) -> Result<(), AbortError<CallerError>>,
        CallerError: Display + Debug,
    {
        catch_compaction_and_retry_abortable(move || {
            let mut tree = match self.open_for_read() {
                Ok(tree) => tree,
                Err(err) if err.kind.is_file_not_found() => return Ok(()),
                Err(err) => return Err(AbortError::from(err)),
            };

            tree.scan(
                range,
                forwards,
                false,
                &mut node_evaluator,
                &mut key_evaluator,
                &mut callback,
            )
        })
    }

    /// Returns the reduced index over the provided range. This is an
    /// aggregation function that builds atop the `scan()` operation which calls
    /// [`Reducer::reduce()`](crate::tree::Reducer::reduce) and
    /// [`Reducer::rereduce()`](crate::tree::Reducer::rereduce) on all matching
    /// indexes stored within the nodes of this tree, producing a single
    /// aggregated [`Root::ReducedIndex`](tree::Root::ReducedIndex) value.
    ///
    /// If no keys match, the returned result is what
    /// [`Reducer::rereduce()`](crate::tree::Reducer::rereduce) returns when an
    /// empty slice is provided.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn reduce<'keys, KeyRangeBounds>(
        &self,
        range: &'keys KeyRangeBounds,
    ) -> Result<Option<Root::ReducedIndex>, Error>
    where
        KeyRangeBounds: RangeBounds<&'keys [u8]> + Debug + Clone + ?Sized,
    {
        catch_compaction_and_retry(move || {
            let mut tree = match self.open_for_read() {
                Ok(tree) => tree,
                Err(err) if err.kind.is_file_not_found() => return Ok(None),
                Err(err) => return Err(err),
            };

            tree.reduce(range, false)
        })
    }

    /// Returns the first key of the tree.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn first_key(&self) -> Result<Option<ArcBytes<'static>>, Error> {
        catch_compaction_and_retry(|| {
            let mut tree = match self.open_for_read() {
                Ok(tree) => tree,
                Err(err) if err.kind.is_file_not_found() => return Ok(None),
                Err(err) => return Err(err),
            };

            tree.first_key(false)
        })
    }

    /// Returns the first key and value of the tree.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn first(&self) -> Result<Option<(ArcBytes<'static>, Root::Value)>, Error> {
        catch_compaction_and_retry(|| {
            let mut tree = match self.open_for_read() {
                Ok(tree) => tree,
                Err(err) if err.kind.is_file_not_found() => return Ok(None),
                Err(err) => return Err(err),
            };

            tree.first(false)
        })
    }

    /// Returns the last key of the tree.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn last_key(&self) -> Result<Option<ArcBytes<'static>>, Error> {
        catch_compaction_and_retry(|| {
            let mut tree = match self.open_for_read() {
                Ok(tree) => tree,
                Err(err) if err.kind.is_file_not_found() => return Ok(None),
                Err(err) => return Err(err),
            };

            tree.last_key(false)
        })
    }

    /// Returns the last key and value of the tree.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn last(&self) -> Result<Option<(ArcBytes<'static>, Root::Value)>, Error> {
        catch_compaction_and_retry(|| {
            let mut tree = match self.open_for_read() {
                Ok(tree) => tree,
                Err(err) if err.kind.is_file_not_found() => return Ok(None),
                Err(err) => return Err(err),
            };

            tree.last(false)
        })
    }

    // /// Rewrites the database to remove data that is no longer current. Because
    // /// Nebari uses an append-only format, this is helpful in reducing disk
    // /// usage.
    // ///
    // /// See [`TreeFile::compact()`](crate::tree::TreeFile::compact) for more
    // /// information.
    // pub fn compact(&self) -> Result<(), Error> {
    //     let tree = match self.open_for_read() {
    //         Ok(tree) => tree,
    //         Err(err) if err.kind.is_file_not_found() => return Ok(()),
    //         Err(err) => return Err(err),
    //     };
    //     tree.compact(
    //         &self.roots.context().file_manager,
    //         Some(TransactableCompaction {
    //             name: self.name.as_ref(),
    //             manager: self.roots.transactions(),
    //         }),
    //     )?;
    //     Ok(())
    // }
}

impl<Root: tree::Root, File: io::FileManager> AnyTreeRoot<File> for Tree<Root, File> {
    fn name(&self) -> &str {
        &self.name
    }

    fn default_state(&self) -> Box<dyn AnyTreeState> {
        Box::new(State::<Root>::new(
            None,
            None,
            Root::default_with(
                self.reducer
                    .as_ref()
                    .as_any()
                    .downcast_ref::<Root::Reducer>()
                    .unwrap()
                    .clone(),
            ),
        ))
    }

    fn begin_transaction(
        &self,
        transaction_id: TransactionId,
        file: SedimentFile<File>,
        state: &dyn AnyTreeState,
        context: &Context<File>,
        transactions: Option<&TransactionManager<File>>,
    ) -> Result<Box<dyn AnyTransactionTree<File>>, Error> {
        let context = self.vault.as_ref().map_or_else(
            || Cow::Borrowed(context),
            |vault| Cow::Owned(context.clone().with_any_vault(vault.clone())),
        );
        let tree = TreeFile::open_sediment_file(
            file,
            state
                .as_any()
                .downcast_ref::<State<Root>>()
                .unwrap()
                .clone(),
            &context,
            transactions,
        )?;

        Ok(Box::new(TransactionTree {
            transaction_id,
            tree,
        }))
    }
}

impl<File: io::FileManager, Index> Tree<VersionedTreeRoot<Index>, File>
where
    Index: EmbeddedIndex<ArcBytes<'static>> + Clone + Debug + 'static,
{
    /// Returns the latest sequence id.
    #[must_use]
    pub fn current_sequence_id(&self) -> SequenceId {
        let state = self.state.read();
        state.root.sequence
    }

    /// Scans the tree for keys that are contained within `range`. If `forwards`
    /// is true, scanning starts at the lowest sort-order key and scans forward.
    /// Otherwise, scanning starts at the highest sort-order key and scans
    /// backwards. `key_evaluator` is invoked for each key as it is encountered.
    /// For all [`ScanEvaluation::ReadData`] results returned, `callback` will be
    /// invoked with the key and values. The callback may not be invoked in the
    /// same order as the keys are scanned.
    pub fn scan_sequences<CallerError, Range, KeyEvaluator, DataCallback>(
        &self,
        range: Range,
        forwards: bool,
        mut key_evaluator: KeyEvaluator,
        mut data_callback: DataCallback,
    ) -> Result<(), AbortError<CallerError>>
    where
        Range: Clone + RangeBounds<SequenceId> + Debug + 'static,
        KeyEvaluator: FnMut(KeySequence<Index>) -> ScanEvaluation,
        DataCallback:
            FnMut(KeySequence<Index>, ArcBytes<'static>) -> Result<(), AbortError<CallerError>>,
        CallerError: Display + Debug,
    {
        catch_compaction_and_retry_abortable(|| {
            let mut tree = TreeFile::<VersionedTreeRoot<Index>, File>::open_sediment_file(
                self.database.clone(),
                self.state.clone(),
                self.roots.context(),
                Some(self.roots.transactions()),
            )?;

            tree.scan_sequences(
                range.clone(),
                forwards,
                false,
                &mut key_evaluator,
                &mut data_callback,
            )
        })
    }

    /// Retrieves the keys and values associated with one or more `sequences`.
    /// The value retrieved is the value of the key at the given [`SequenceId`].
    /// If a sequence is not found, it will not appear in the result map. If
    /// the value was removed, None is returned for the value.
    #[allow(clippy::needless_pass_by_value)]
    pub fn get_multiple_by_sequence<Sequences>(
        &mut self,
        sequences: Sequences,
    ) -> Result<HashMap<SequenceId, (ArcBytes<'static>, Option<ArcBytes<'static>>)>, Error>
    where
        Sequences: Iterator<Item = SequenceId> + Clone,
    {
        catch_compaction_and_retry(|| {
            let mut tree = TreeFile::<VersionedTreeRoot<Index>, File>::open_sediment_file(
                self.database.clone(),
                self.state.clone(),
                self.roots.context(),
                Some(self.roots.transactions()),
            )?;

            tree.get_multiple_by_sequence(sequences.clone(), false)
        })
    }

    /// Retrieves the keys and indexes associated with one or more `sequences`.
    /// The value retrieved is the value of the key at the given [`SequenceId`].
    /// If a sequence is not found, it will not appear in the result list.
    #[allow(clippy::needless_pass_by_value)]
    pub fn get_multiple_indexes_by_sequence<Sequences>(
        &mut self,
        sequences: Sequences,
    ) -> Result<Vec<SequenceIndex<Index>>, Error>
    where
        Sequences: Iterator<Item = SequenceId> + Clone,
    {
        catch_compaction_and_retry(|| {
            let mut tree = TreeFile::<VersionedTreeRoot<Index>, File>::open_sediment_file(
                self.database.clone(),
                self.state.clone(),
                self.roots.context(),
                Some(self.roots.transactions()),
            )?;

            tree.get_multiple_indexes_by_sequence(sequences.clone(), false)
        })
    }

    /// Retrieves the keys, values, and indexes associated with one or more
    /// `sequences`. The value retrieved is the value of the key at the given
    /// [`SequenceId`]. If a sequence is not found, it will not appear in the
    /// result list.
    #[allow(clippy::needless_pass_by_value)]
    pub fn get_multiple_with_indexes_by_sequence<Sequences>(
        &mut self,
        sequences: Sequences,
    ) -> Result<HashMap<SequenceId, SequenceEntry<Index>>, Error>
    where
        Sequences: Iterator<Item = SequenceId> + Clone,
    {
        catch_compaction_and_retry(|| {
            let mut tree = TreeFile::<VersionedTreeRoot<Index>, File>::open_sediment_file(
                self.database.clone(),
                self.state.clone(),
                self.roots.context(),
                Some(self.roots.transactions()),
            )?;

            tree.get_multiple_with_indexes_by_sequence(sequences.clone(), false)
        })
    }
}

/// A thread pool that commits transactions to disk in parallel.
#[derive(Debug)]
pub struct ThreadPool<File>
where
    File: io::FileManager,
{
    sender: flume::Sender<ThreadCommand<File>>,
    receiver: flume::Receiver<ThreadCommand<File>>,
    thread_count: Arc<AtomicU16>,
    maximum_threads: usize,
}

impl<File: io::FileManager> ThreadPool<File> {
    /// Returns a thread pool that will spawn up to `maximum_threads` to process
    /// file operations.
    #[must_use]
    pub fn new(maximum_threads: usize) -> Self {
        let (sender, receiver) = flume::unbounded();
        Self {
            sender,
            receiver,
            thread_count: Arc::new(AtomicU16::new(0)),
            maximum_threads,
        }
    }

    fn commit_trees(
        &self,
        trees: Vec<UnlockedTransactionTree<File>>,
    ) -> Result<Vec<CommittedTreeState<File>>, Error> {
        // If we only have one tree, there's no reason to split IO across
        // threads. If we have multiple trees, we should split even with one
        // cpu: if one thread blocks, the other can continue executing.
        let mut committed_trees = Vec::default();
        // Push the trees so that any existing threads can begin processing the queue.
        let (completion_sender, completion_receiver) = flume::unbounded();
        let tree_count = trees.len();
        for tree in trees {
            self.sender.send(ThreadCommand::Commit(ThreadCommit {
                tree,
                completion_sender: completion_sender.clone(),
            }))?;
        }

        self.spawn_threads_if_needed(tree_count);

        // Wait for our results
        for _ in 0..tree_count {
            if let Some(committed_state) = completion_receiver.recv()?? {
                committed_trees.push(committed_state);
            }
        }

        Ok(committed_trees)
    }

    fn spawn_threads_if_needed(&self, new_count: usize) {
        let desired_threads = new_count.min(self.maximum_threads);
        loop {
            let thread_count = self.thread_count.load(Ordering::SeqCst);
            if (thread_count as usize) >= desired_threads {
                break;
            }

            // Spawn a thread, but ensure that we don't spin up too many threads
            // if another thread is committing at the same time.
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
                    .spawn(move || thread_pool_worker(commit_receiver))
                    .unwrap();
            }
        }
    }

    pub(crate) fn finish_all_commits_async<PendingCommits: Iterator<Item = PendingCommit<File>>>(
        &self,
        all_commits: PendingCommits,
    ) -> Result<CommitAllHandle, Error> {
        let (completion_sender, completion_receiver) = flume::unbounded();
        let mut sync_count = 0;
        for pending_commit in all_commits {
            sync_count += 1;
            self.sender
                .send(ThreadCommand::FinishCommit(ThreadFinishCommit {
                    pending_commit,
                    completion_sender: completion_sender.clone(),
                }))?;
        }

        self.spawn_threads_if_needed(sync_count);

        Ok(CommitAllHandle {
            sync_count,
            completion_receiver,
        })
    }
}

pub struct CommitAllHandle {
    sync_count: usize,
    completion_receiver: flume::Receiver<Result<(u64, CheckpointGuard), Error>>,
}

impl CommitAllHandle {
    pub fn wait(
        &mut self,
        tree_checkpoint_guards: &mut HashMap<u64, CheckpointGuard>,
    ) -> Result<(), Error> {
        // let guard = pending_commit.commit().unwrap();
        // let entry = tree_checkpoint_guards.entry(path_id.id);
        // if let hash_map::Entry::Occupied(existing_guard) = entry {

        // }
        while self.sync_count > 0 {
            let (path_id, guard) = self.completion_receiver.recv()??;
            match tree_checkpoint_guards.entry(path_id) {
                hash_map::Entry::Occupied(mut entry) => {
                    if &guard > entry.get() {
                        entry.insert(guard);
                    }
                }
                hash_map::Entry::Vacant(entry) => {
                    entry.insert(guard);
                }
            }
            self.sync_count -= 1;
        }
        Ok(())
    }
}

impl<File: io::FileManager> Default for ThreadPool<File> {
    fn default() -> Self {
        static CPU_COUNT: Lazy<usize> = Lazy::new(num_cpus::get);
        Self::new(*CPU_COUNT)
    }
}

impl<File: io::FileManager> Clone for ThreadPool<File> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            receiver: self.receiver.clone(),
            thread_count: self.thread_count.clone(),
            maximum_threads: self.maximum_threads,
        }
    }
}

#[allow(clippy::needless_pass_by_value)]
fn thread_pool_worker<File: io::FileManager>(receiver: flume::Receiver<ThreadCommand<File>>) {
    while let Ok(command) = receiver.recv() {
        match command {
            ThreadCommand::Commit(ThreadCommit {
                tree,
                completion_sender,
            }) => {
                let mut locked = tree.0.lock();
                let result = locked.commit();
                drop(completion_sender.send(result));
            }
            ThreadCommand::FinishCommit(ThreadFinishCommit {
                pending_commit,
                completion_sender,
            }) => {
                let path_id = pending_commit.path_id().id;
                drop(
                    completion_sender.send(
                        pending_commit
                            .commit()
                            .map(|guard| (path_id, guard))
                            .map_err(Error::from),
                    ),
                );
            }
        }
    }
}

enum ThreadCommand<File>
where
    File: io::FileManager,
{
    Commit(ThreadCommit<File>),
    FinishCommit(ThreadFinishCommit<File>),
}

struct ThreadCommit<File>
where
    File: io::FileManager,
{
    tree: UnlockedTransactionTree<File>,
    completion_sender: Sender<Result<Option<CommittedTreeState<File>>, Error>>,
}

struct ThreadFinishCommit<File>
where
    File: io::FileManager,
{
    pending_commit: PendingCommit<File>,
    completion_sender: Sender<Result<(u64, CheckpointGuard), Error>>,
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
    use tempfile::tempdir;

    use super::*;
    use crate::{
        test_util::RotatorVault,
        tree::{Root, Versioned},
    };
    use sediment::io::{fs::StdFileManager, memory::MemoryFileManager, FileManager};

    fn basic_get_set<M: FileManager>(file_manager: M) {
        let tempdir = tempdir().unwrap();
        let roots = Config::new(tempdir.path())
            .file_manager(file_manager)
            .open()
            .unwrap();

        let tree = roots.tree(Versioned::tree("test")).unwrap();
        tree.set(b"test", b"value").unwrap();
        let result = tree.get(b"test").unwrap().expect("key not found");

        assert_eq!(result, b"value");
    }

    #[test]
    fn memory_basic_get_set() {
        basic_get_set(MemoryFileManager::default());
    }

    #[test]
    fn std_basic_get_set() {
        basic_get_set(StdFileManager::default());
    }

    #[test]
    fn basic_transaction_isolation_test() {
        let tempdir = tempdir().unwrap();

        let roots = Config::<StdFileManager>::new(tempdir.path())
            .open()
            .unwrap();
        let tree = roots.tree(Versioned::tree("test")).unwrap();
        tree.set(b"test", b"value").unwrap();

        // Begin a transaction
        let transaction = roots.transaction(&[Versioned::tree("test")]).unwrap();

        // Replace the key with a new value.
        transaction
            .tree::<Versioned>(0)
            .unwrap()
            .set(b"test", b"updated value")
            .unwrap();

        // Check that the transaction can read the new value
        let result = transaction
            .tree::<Versioned>(0)
            .unwrap()
            .get(b"test")
            .unwrap()
            .expect("key not found");
        assert_eq!(result, b"updated value");

        // Ensure that existing read-access doesn't see the new value
        let result = tree.get(b"test").unwrap().expect("key not found");
        assert_eq!(result, b"value");

        // Commit the transaction
        transaction.commit().unwrap();

        // Ensure that the reader now sees the new value
        let result = tree.get(b"test").unwrap().expect("key not found");
        assert_eq!(result, b"updated value");
    }

    #[test]
    fn basic_transaction_rollback_test() {
        let tempdir = tempdir().unwrap();

        let roots = Config::<StdFileManager>::new(tempdir.path())
            .open()
            .unwrap();
        let tree = roots.tree(Versioned::tree("test")).unwrap();
        tree.set(b"test", b"value").unwrap();

        // Begin a transaction
        let transaction = roots.transaction(&[Versioned::tree("test")]).unwrap();

        // Replace the key with a new value.
        transaction
            .tree::<Versioned>(0)
            .unwrap()
            .set(b"test", b"updated value")
            .unwrap();

        // Roll the transaction back
        transaction.rollback();

        // Ensure that the reader still sees the old value
        let result = tree.get(b"test").unwrap().expect("key not found");
        assert_eq!(result, b"value");

        // Begin a new transaction
        let transaction = roots.transaction(&[Versioned::tree("test")]).unwrap();
        // Check that the transaction has the original value
        let result = transaction
            .tree::<Versioned>(0)
            .unwrap()
            .get(b"test")
            .unwrap()
            .expect("key not found");
        assert_eq!(result, b"value");
    }

    // #[test]
    // fn std_compact_test_versioned() {
    //     compact_test::<Versioned, _>(StdFileManager::default());
    // }

    // #[test]
    // fn std_compact_test_unversioned() {
    //     compact_test::<Unversioned, _>(StdFileManager::default());
    // }

    // #[test]
    // fn memory_compact_test_versioned() {
    //     compact_test::<Versioned, _>(MemoryFileManager::default());
    // }

    // #[test]
    // fn memory_compact_test_unversioned() {
    //     compact_test::<Unversioned, _>(MemoryFileManager::default());
    // }

    // #[test]
    // fn any_compact_test_versioned() {
    //     compact_test::<Versioned, _>(AnyFileManager::std());
    //     compact_test::<Versioned, _>(AnyFileManager::memory());
    // }

    // #[test]
    // fn any_compact_test_unversioned() {
    //     compact_test::<Unversioned, _>(AnyFileManager::std());
    //     compact_test::<Unversioned, _>(AnyFileManager::memory());
    // }

    // fn compact_test<R: Root<Value = ArcBytes<'static>>, M: FileManager>(file_manager: M)
    // where
    //     R::Reducer: Default,
    // {
    //     const OPERATION_COUNT: usize = 256;
    //     const WORKER_COUNT: usize = 4;
    //     let tempdir = tempdir().unwrap();

    //     let roots = Config::new(dbg!(tempdir.path()))
    //         .file_manager(file_manager)
    //         .open()
    //         .unwrap();
    //     let tree = roots.tree(R::tree("test")).unwrap();
    //     tree.set("foo", b"bar").unwrap();

    //     // Spawn a pool of threads that will perform a series of operations
    //     let mut threads = Vec::new();
    //     for worker in 0..WORKER_COUNT {
    //         let tree = tree.clone();
    //         threads.push(std::thread::spawn(move || {
    //             for relative_id in 0..OPERATION_COUNT {
    //                 let absolute_id = (worker * OPERATION_COUNT + relative_id) as u64;
    //                 tree.set(absolute_id.to_be_bytes(), absolute_id.to_be_bytes())
    //                     .unwrap();
    //                 let ValueIndex { value, .. } = tree
    //                     .remove(&absolute_id.to_be_bytes())
    //                     .unwrap()
    //                     .ok_or_else(|| panic!("value not found: {:?}", absolute_id))
    //                     .unwrap();
    //                 assert_eq!(BigEndian::read_u64(&value), absolute_id);
    //                 tree.set(absolute_id.to_be_bytes(), absolute_id.to_be_bytes())
    //                     .unwrap();
    //                 let newer_value = tree
    //                     .get(&absolute_id.to_be_bytes())
    //                     .unwrap()
    //                     .expect("couldn't find found");
    //                 assert_eq!(value, newer_value);
    //             }
    //         }));
    //     }

    //     threads.push(std::thread::spawn(move || {
    //         // While those workers are running, this thread is going to continually
    //         // execute compaction.
    //         while tree.count() < (OPERATION_COUNT * WORKER_COUNT) as u64 {
    //             tree.compact().unwrap();
    //         }
    //     }));

    //     for thread in threads {
    //         thread.join().unwrap();
    //     }
    // }

    #[test]
    fn name_tests() {
        assert!(check_name("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_-.").is_ok());
        assert!(check_name("=").is_err());
        assert!(check_name("_transactions").is_err());
    }

    #[test]
    fn context_encryption_tests() {
        let tempdir = tempdir().unwrap();

        // Encrypt a tree using default encryption via the context
        {
            let roots = Config::<StdFileManager>::new(tempdir.path())
                .vault(RotatorVault::new(13))
                .open()
                .unwrap();
            let tree = roots.tree(Versioned::tree("test")).unwrap();
            tree.set(b"test", b"value").unwrap();
            let other_tree = roots
                .tree(Versioned::tree("test-otherkey").with_vault(RotatorVault::new(42)))
                .unwrap();
            other_tree.set(b"test", b"other").unwrap();
        }
        // Try to access the tree with the vault again.
        {
            let roots = Config::<StdFileManager>::new(tempdir.path())
                .vault(RotatorVault::new(13))
                .open()
                .unwrap();
            let tree = roots.tree(Versioned::tree("test")).unwrap();
            let value = tree.get(b"test").unwrap();
            assert_eq!(value.as_deref(), Some(&b"value"[..]));

            // Verify we can't read the other tree without the right vault
            let bad_tree = roots.tree(Versioned::tree("test-otherkey")).unwrap();
            assert!(bad_tree.get(b"test").is_err());

            // And test retrieving the other key with the correct vault
            let tree = roots
                .tree(Versioned::tree("test-otherkey").with_vault(RotatorVault::new(42)))
                .unwrap();
            let value = tree.get(b"test").unwrap();
            assert_eq!(value.as_deref(), Some(&b"other"[..]));
        }
        {
            assert!(Config::<StdFileManager>::new(tempdir.path())
                .open()
                .is_err());
            let roots = Config::<StdFileManager>::new(tempdir.path())
                .vault(RotatorVault::new(13))
                .open()
                .unwrap();
            // Try to access the tree with an incorrect vault
            let bad_tree = roots.tree(Versioned::tree("test-otherkey")).unwrap();
            assert!(bad_tree.get(b"test").is_err());
        }
    }

    #[test]
    fn too_large_transaction() {
        let tempdir = tempdir().unwrap();

        let config = Config::<StdFileManager>::new(tempdir.path());
        {
            let roots = config.clone().open().unwrap();

            let mut transaction = roots.transaction(&[Versioned::tree("test")]).unwrap();

            // Write some data to the tree.
            transaction
                .tree::<Versioned>(0)
                .unwrap()
                .set(b"test", vec![0; 16 * 1024 * 1024])
                .unwrap();

            // Issue a transaction that's too large.
            assert!(matches!(
                transaction
                    .entry_mut()
                    .set_data(vec![0; 16 * 1024 * 1024 + 1])
                    .unwrap_err()
                    .kind,
                ErrorKind::ValueTooLarge
            ));
            // Roll the transaction back
            transaction.rollback();
        }
        // Ensure that we can still write to the tree.
        {
            let roots = config.open().unwrap();

            let transaction = roots.transaction(&[Versioned::tree("test")]).unwrap();

            // Write some data to the tree
            transaction
                .tree::<Versioned>(0)
                .unwrap()
                .set(b"test", b"updated value")
                .unwrap();

            transaction.commit().unwrap();
        }
    }
}
