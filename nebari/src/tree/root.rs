use std::{
    any::Any,
    borrow::Cow,
    collections::HashMap,
    fmt::{Debug, Display},
    marker::PhantomData,
    ops::RangeBounds,
    path::Path,
    sync::Arc,
};

use crate::{
    error::Error,
    io::{File, ManagedFile},
    roots::AnyTransactionTree,
    transaction::{TransactionId, TransactionManager},
    tree::{
        btree_entry::ScanArgs, state::AnyTreeState, Modification, PageHeader, PagedWriter, Reducer,
        ScanEvaluation, State, TreeFile,
    },
    vault::AnyVault,
    AbortError, ArcBytes, ChunkCache, Context, TransactionTree, Vault,
};

/// A B-Tree root implementation.
pub trait Root: Debug + Send + Sync + Clone + 'static {
    /// The unique header byte for this root.
    const HEADER: PageHeader;

    /// The primary index type contained within this root.
    type Index: Clone + Debug + 'static;
    /// The primary index type contained within this root.
    type ReducedIndex: Clone + Debug + 'static;
    /// The reducer that reduces `Index`es and re-reduces `ReducedIndex`es.
    type Reducer: Reducer<Self::Index, Self::ReducedIndex> + 'static;

    /// Returns a new instance with the provided reducer.
    fn default_with(reducer: Self::Reducer) -> Self;

    /// Returns the instance's reducer.
    fn reducer(&self) -> &Self::Reducer;

    /// Returns the number of values contained in this tree, not including
    /// deleted records.
    fn count(&self) -> u64;

    /// Returns a reference to a named tree that contains this type of root.
    fn tree<File: ManagedFile>(name: impl Into<Cow<'static, str>>) -> TreeRoot<Self, File>
    where
        Self::Reducer: Default,
    {
        TreeRoot {
            name: name.into(),
            vault: None,
            reducer: Arc::new(<Self::Reducer as Default>::default()),
            _phantom: PhantomData,
        }
    }

    /// Returns a reference to a named tree that contains this type of root.
    fn tree_with_reducer<File: ManagedFile>(
        name: impl Into<Cow<'static, str>>,
        reducer: Self::Reducer,
    ) -> TreeRoot<Self, File> {
        TreeRoot {
            name: name.into(),
            vault: None,
            reducer: Arc::new(reducer),
            _phantom: PhantomData,
        }
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
    fn serialize(
        &mut self,
        paged_writer: &mut PagedWriter<'_>,
        output: &mut Vec<u8>,
    ) -> Result<(), Error>;

    /// Deserializes the root from `bytes`.
    fn deserialize(bytes: ArcBytes<'_>, reducer: Self::Reducer) -> Result<Self, Error>;

    /// Returns the current transaction id.
    fn transaction_id(&self) -> TransactionId;

    /// Modifies the tree.
    fn modify<'a, 'w>(
        &'a mut self,
        modification: Modification<'_, ArcBytes<'static>>,
        writer: &'a mut PagedWriter<'w>,
        max_order: Option<usize>,
    ) -> Result<(), Error>;

    /// Iterates over the tree looking for `keys`. `keys` must be sorted.
    /// `key_evaluator` is invoked for each key as it is found, allowing for
    /// decisions on how to handle each key. `key_reader` will be invoked for
    /// each key that is requested to be read, but it might be invoked at a
    /// later time and in a different order.
    fn get_multiple<'keys, KeyEvaluator, KeyReader, Keys>(
        &self,
        keys: &mut Keys,
        key_evaluator: &mut KeyEvaluator,
        key_reader: &mut KeyReader,
        file: &mut dyn File,
        vault: Option<&dyn AnyVault>,
        cache: Option<&ChunkCache>,
    ) -> Result<(), Error>
    where
        KeyEvaluator: FnMut(&ArcBytes<'static>) -> ScanEvaluation,
        KeyReader: FnMut(ArcBytes<'static>, ArcBytes<'static>) -> Result<(), Error>,
        Keys: Iterator<Item = &'keys [u8]>;

    /// Scans the tree over `range`. `args.key_evaluator` is invoked for each key as
    /// it is found, allowing for decisions on how to handle each key.
    /// `args.key_reader` will be invoked for each key that is requested to be read,
    /// but it might be invoked at a later time and in a different order.
    fn scan<
        'keys,
        CallerError: Display + Debug,
        NodeEvaluator,
        KeyRangeBounds,
        KeyEvaluator,
        ScanDataCallback,
    >(
        &self,
        range: &'keys KeyRangeBounds,
        args: &mut ScanArgs<
            Self::Index,
            Self::ReducedIndex,
            CallerError,
            NodeEvaluator,
            KeyEvaluator,
            ScanDataCallback,
        >,
        file: &mut dyn File,
        vault: Option<&dyn AnyVault>,
        cache: Option<&ChunkCache>,
    ) -> Result<bool, AbortError<CallerError>>
    where
        NodeEvaluator: FnMut(&ArcBytes<'static>, &Self::ReducedIndex, usize) -> ScanEvaluation,
        KeyEvaluator: FnMut(&ArcBytes<'static>, &Self::Index) -> ScanEvaluation,
        KeyRangeBounds: RangeBounds<&'keys [u8]> + Debug + ?Sized,
        ScanDataCallback: FnMut(
            ArcBytes<'static>,
            &Self::Index,
            ArcBytes<'static>,
        ) -> Result<(), AbortError<CallerError>>;

    /// Copies all data from `file` into `writer`, updating `self` with the new
    /// file positions.
    fn copy_data_to(
        &mut self,
        include_nodes: bool,
        file: &mut dyn File,
        copied_chunks: &mut HashMap<u64, u64>,
        writer: &mut PagedWriter<'_>,
        vault: Option<&dyn AnyVault>,
    ) -> Result<(), Error>;
}

/// A named tree with a specific root type.
#[must_use]
pub struct TreeRoot<R: Root, File: ManagedFile> {
    /// The name of the tree.
    pub name: Cow<'static, str>,
    /// The vault to use when opening the tree. If not set, the `Context` will
    /// provide the vault.
    pub vault: Option<Arc<dyn AnyVault>>,
    /// The [`Reducer`] for this tree.
    pub(crate) reducer: Arc<dyn AnyReducer>,
    _phantom: PhantomData<(R, File)>,
}

pub trait AnyReducer: Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;
}

impl<T> AnyReducer for T
where
    T: Any + Send + Sync + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl<R: Root, File: ManagedFile> TreeRoot<R, File> {
    /// Replaces the vault currenty set with `vault`.
    pub fn with_vault<V: Vault>(mut self, vault: V) -> Self {
        self.vault = Some(Arc::new(vault));
        self
    }
}

impl<R: Root, File: ManagedFile> Clone for TreeRoot<R, File> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            vault: self.vault.clone(),
            reducer: self.reducer.clone(),
            _phantom: PhantomData,
        }
    }
}

/// A named tree that can be used in a transaction.
pub trait AnyTreeRoot<File: ManagedFile> {
    /// The name of the tree.
    fn name(&self) -> &str;
    /// The default state for the underlying root type.
    fn default_state(&self) -> Box<dyn AnyTreeState>;
    /// Begins a transaction on this tree.
    fn begin_transaction(
        &self,
        transaction_id: TransactionId,
        file_path: &Path,
        state: &dyn AnyTreeState,
        context: &Context<File::Manager>,
        transactions: Option<&TransactionManager<File::Manager>>,
    ) -> Result<Box<dyn AnyTransactionTree<File>>, Error>;
}

impl<R: Root, File: ManagedFile> AnyTreeRoot<File> for TreeRoot<R, File> {
    fn name(&self) -> &str {
        &self.name
    }

    fn default_state(&self) -> Box<dyn AnyTreeState> {
        Box::new(State::<R>::new(
            None,
            None,
            R::default_with(
                self.reducer
                    .as_ref()
                    .as_any()
                    .downcast_ref::<R::Reducer>()
                    .unwrap()
                    .clone(),
            ),
        ))
    }

    fn begin_transaction(
        &self,
        transaction_id: TransactionId,
        file_path: &Path,
        state: &dyn AnyTreeState,
        context: &Context<File::Manager>,
        transactions: Option<&TransactionManager<File::Manager>>,
    ) -> Result<Box<dyn AnyTransactionTree<File>>, Error> {
        let context = self.vault.as_ref().map_or_else(
            || Cow::Borrowed(context),
            |vault| Cow::Owned(context.clone().with_any_vault(vault.clone())),
        );
        let tree = TreeFile::write(
            file_path,
            state.as_any().downcast_ref::<State<R>>().unwrap().clone(),
            &context,
            transactions,
        )?;

        Ok(Box::new(TransactionTree {
            transaction_id,
            tree,
        }))
    }
}
