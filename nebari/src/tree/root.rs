use std::{
    any::Any,
    borrow::Cow,
    collections::HashMap,
    fmt::{Debug, Display},
    marker::PhantomData,
    ops::RangeBounds,
    path::Path,
};

use crate::{
    error::Error,
    io::ManagedFile,
    roots::AnyTransactionTree,
    transaction::TransactionManager,
    tree::{
        btree_entry::ScanArgs, state::AnyTreeState, KeyEvaluation, KeyRange, Modification,
        PageHeader, PagedWriter, State, TreeFile,
    },
    AbortError, Buffer, ChunkCache, Context, TransactionTree, Vault,
};

/// A B-Tree root implementation.
pub trait Root: Default + Debug + Send + Sync + Clone + 'static {
    /// The unique header byte for this root.
    const HEADER: PageHeader;

    /// Returns the number of values contained in this tree, not including
    /// deleted records.
    fn count(&self) -> u64;

    /// Returns a reference to a named tree that contains this type of root.
    fn tree<File: ManagedFile>(name: impl Into<Cow<'static, str>>) -> TreeRoot<File> {
        TreeRoot(
            name.into(),
            Box::new(TreeRootInner::<File, Self>(PhantomData)),
        )
    }

    /// Returns true if the root needs to be saved.
    fn dirty(&self) -> bool;

    /// Returns true if the tree is initialized.
    fn initialized(&self) -> bool;

    /// Resets the state to the default, initialized state. After calling this,
    /// `Self::initialized()` will return true.
    fn initialize_default(&mut self);

    /// Serialize the root and return the bytes. Writes any additional data to
    /// `paged_writer` in the process of serialization.
    fn serialize<File: ManagedFile>(
        &mut self,
        paged_writer: &mut PagedWriter<'_, File>,
        output: &mut Vec<u8>,
    ) -> Result<(), Error>;

    /// Deserializes the root from `bytes`.
    fn deserialize(bytes: Buffer<'_>) -> Result<Self, Error>;

    /// Returns the current transaction id.
    fn transaction_id(&self) -> u64;

    /// Modifies the tree.
    fn modify<'a, 'w, File: ManagedFile>(
        &'a mut self,
        modification: Modification<'_, Buffer<'static>>,
        writer: &'a mut PagedWriter<'w, File>,
    ) -> Result<(), Error>;

    /// Iterates over the tree looking for `keys`. `keys` must be sorted.
    /// `key_evaluator` is invoked for each key as it is found, allowing for
    /// decisions on how to handle each key. `key_reader` will be invoked for
    /// each key that is requested to be read, but it might be invoked at a
    /// later time and in a different order.
    fn get_multiple<File: ManagedFile, KeyEvaluator, KeyReader>(
        &self,
        keys: &mut KeyRange<'_>,
        key_evaluator: &mut KeyEvaluator,
        key_reader: &mut KeyReader,
        file: &mut File,
        vault: Option<&dyn Vault>,
        cache: Option<&ChunkCache>,
    ) -> Result<(), Error>
    where
        KeyEvaluator: FnMut(&Buffer<'static>) -> KeyEvaluation,
        KeyReader: FnMut(Buffer<'static>, Buffer<'static>) -> Result<(), Error>;

    /// Scans the tree over `range`. `args.key_evaluator` is invoked for each key as
    /// it is found, allowing for decisions on how to handle each key.
    /// `args.key_reader` will be invoked for each key that is requested to be read,
    /// but it might be invoked at a later time and in a different order.
    fn scan<
        'keys,
        CallerError: Display + Debug,
        File: ManagedFile,
        KeyRangeBounds,
        KeyEvaluator,
        KeyReader,
    >(
        &self,
        range: &KeyRangeBounds,
        args: &mut ScanArgs<Buffer<'static>, CallerError, KeyEvaluator, KeyReader>,
        file: &mut File,
        vault: Option<&dyn Vault>,
        cache: Option<&ChunkCache>,
    ) -> Result<(), AbortError<CallerError>>
    where
        KeyEvaluator: FnMut(&Buffer<'static>) -> KeyEvaluation,
        KeyReader: FnMut(Buffer<'static>, Buffer<'static>) -> Result<(), AbortError<CallerError>>,
        KeyRangeBounds: RangeBounds<Buffer<'keys>> + Debug;

    /// Copies all data from `file` into `writer`, updating `self` with the new
    /// file positions.
    fn copy_data_to<File: ManagedFile>(
        &mut self,
        include_nodes: bool,
        file: &mut File,
        copied_chunks: &mut HashMap<u64, u64>,
        writer: &mut PagedWriter<'_, File>,
        vault: Option<&dyn Vault>,
    ) -> Result<(), Error>;
}

/// A named tree root.
pub struct TreeRoot<File: ManagedFile>(Cow<'static, str>, Box<dyn AnyTreeRootInner<File>>);
struct TreeRootInner<File: ManagedFile, R: Root>(PhantomData<(File, R)>);

trait AnyTreeRootInner<File: ManagedFile>: Any {
    fn as_any(&self) -> &dyn Any
    where
        Self: Sized,
    {
        self
    }
    fn default_state(&self) -> Box<dyn AnyTreeState>;
    fn begin_transaction(
        &self,
        transaction_id: u64,
        file_path: &Path,
        state: &dyn AnyTreeState,
        context: &Context<File::Manager>,
        transactions: Option<&TransactionManager<File::Manager>>,
    ) -> Result<Box<dyn AnyTransactionTree<File>>, Error>;
}

impl<File: ManagedFile, R: Root> AnyTreeRootInner<File> for TreeRootInner<File, R> {
    fn default_state(&self) -> Box<dyn AnyTreeState> {
        Box::new(State::<R>::default())
    }

    fn begin_transaction(
        &self,
        transaction_id: u64,
        file_path: &Path,
        state: &dyn AnyTreeState,
        context: &Context<File::Manager>,
        transactions: Option<&TransactionManager<File::Manager>>,
    ) -> Result<Box<dyn AnyTransactionTree<File>>, Error> {
        let tree = TreeFile::write(
            file_path,
            state.as_any().downcast_ref::<State<R>>().unwrap().clone(),
            context,
            transactions,
        )?;

        Ok(Box::new(TransactionTree {
            transaction_id,
            tree,
        }))
    }
}

impl<File: ManagedFile> TreeRoot<File> {
    /// Returns the name of the tree.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.0
    }

    /// Returns the default state for this root.
    #[must_use]
    pub fn default_state(&self) -> Box<dyn AnyTreeState> {
        self.1.default_state()
    }

    /// Begins a transaction.
    pub fn begin_transaction(
        &self,
        transaction_id: u64,
        file_path: &Path,
        state: &dyn AnyTreeState,
        context: &Context<File::Manager>,
        transactions: Option<&TransactionManager<File::Manager>>,
    ) -> Result<Box<dyn AnyTransactionTree<File>>, Error> {
        self.1
            .begin_transaction(transaction_id, file_path, state, context, transactions)
    }
}
