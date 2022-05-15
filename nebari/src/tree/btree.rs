use std::{
    collections::HashMap,
    convert::Infallible,
    fmt::{Debug, Display},
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

use byteorder::{ReadBytesExt, WriteBytesExt};

use super::{
    interior::Interior,
    key_entry::KeyEntry,
    modify::{Modification, Operation},
    serialization::BinarySerialization,
    ChangeResult, KeyRange, PagedWriter,
};
use crate::{
    chunk_cache::CacheEntry,
    error::Error,
    io::File,
    tree::{key_entry::PositionIndex, read_chunk, versioned::Children, ScanEvaluation},
    vault::AnyVault,
    AbortError, ArcBytes, ChunkCache, ErrorKind,
};

/// A B-Tree entry that stores a list of key-`Index` pairs.
#[derive(Clone, Debug)]
pub struct BTreeEntry<Index, ReducedIndex> {
    /// Whether the node contains unsaved changes.
    pub dirty: bool,
    /// The B-Tree node.
    pub node: BTreeNode<Index, ReducedIndex>,
}

/// A B-Tree entry that stores a list of key-`Index` pairs.
#[derive(Clone, Debug)]
pub enum BTreeNode<Index, ReducedIndex> {
    /// An uninitialized node. Only used temporarily during modification.
    Uninitialized,
    /// An inline value. Overall, the B-Tree entry is a key-value pair.
    Leaf(Vec<KeyEntry<Index>>),
    /// An interior node that contains pointers to other nodes.
    Interior(Vec<Interior<Index, ReducedIndex>>),
}

impl<Index, ReducedIndex> From<BTreeNode<Index, ReducedIndex>> for BTreeEntry<Index, ReducedIndex> {
    fn from(node: BTreeNode<Index, ReducedIndex>) -> Self {
        Self { node, dirty: true }
    }
}

impl<Index, ReducedIndex> Default for BTreeEntry<Index, ReducedIndex> {
    fn default() -> Self {
        Self::from(BTreeNode::Leaf(Vec::new()))
    }
}

/// Reduces one or more `Index`es or instances of `Self` into a single `Self`
/// value.
///
/// This trait is used to collect statistics within the B-Tree. The `Index`
/// value is stored at the [`KeyEntry`] level, and each [`Interior`] entry
/// contains `Self`, which is calculated by calling [`Reducer::reduce()`] on all
/// stored `Index`es.
///
/// When an `Interior` node points to other interior nodes, it calculates the
/// stored value by calling [`Reducer::rereduce()`] with all the stored `Self`
/// values.
pub trait Reducer<Index, ReducedIndex = Index>: Debug + Clone + Send + Sync {
    /// Reduces one or more indexes into a single reduced index.
    fn reduce<'a, Indexes, IndexesIter>(&self, indexes: Indexes) -> ReducedIndex
    where
        Index: 'a,
        Indexes: IntoIterator<Item = &'a Index, IntoIter = IndexesIter> + ExactSizeIterator,
        IndexesIter: Iterator<Item = &'a Index> + ExactSizeIterator + Clone;

    /// Reduces one or more previously-reduced indexes into a single reduced index.
    fn rereduce<'a, ReducedIndexes, ReducedIndexesIter>(
        &self,
        values: ReducedIndexes,
    ) -> ReducedIndex
    where
        Self: 'a,
        ReducedIndex: 'a,
        ReducedIndexes: IntoIterator<Item = &'a ReducedIndex, IntoIter = ReducedIndexesIter>
            + ExactSizeIterator,
        ReducedIndexesIter: Iterator<Item = &'a ReducedIndex> + ExactSizeIterator + Clone;
}

/// Creates an `Index` from a key and value.
pub trait Indexer<Value, Index>: Debug + Send + Sync {
    /// Index the key and value.
    fn index(&self, key: &ArcBytes<'_>, value: Option<&Value>) -> Index;
}

impl<Index> Reducer<Index, ()> for () {
    fn reduce<'a, Indexes, IndexesIter>(&self, _indexes: Indexes) -> Self
    where
        Index: 'a,
        Indexes: IntoIterator<Item = &'a Index, IntoIter = IndexesIter> + ExactSizeIterator,
        IndexesIter: Iterator<Item = &'a Index> + ExactSizeIterator + Clone,
    {
    }

    fn rereduce<'a, ReducedIndexes, ReducedIndexesIter>(&self, _values: ReducedIndexes) -> Self
    where
        Self: 'a,
        ReducedIndexes:
            IntoIterator<Item = &'a (), IntoIter = ReducedIndexesIter> + ExactSizeIterator,
        ReducedIndexesIter: Iterator<Item = &'a ()> + ExactSizeIterator + Clone,
    {
    }
}

/// A context for a modification of a [`BTreeNode`].
pub struct ModificationContext<Value, Index, ReducedIndex, Indexer, Loader, IndexReducer>
where
    Indexer: FnMut(
        &ArcBytes<'_>,
        Option<&Value>,
        Option<&Index>,
        &mut PagedWriter<'_>,
    ) -> Result<KeyOperation<Index>, Error>,
    IndexReducer: Reducer<Index, ReducedIndex>,
    Loader: FnMut(&Index, &mut PagedWriter<'_>) -> Result<Option<Value>, Error>,
{
    /// The maximum number of children allowed per node.
    pub current_order: usize,
    /// The minimum number of children allowed per node.
    pub minimum_children: usize,
    /// The indexing function that produces `Index`es for a given `Value`.
    ///
    /// The parameters to this function are:
    ///
    /// - The entry's key
    /// - The entry's value, if the value is present.
    /// - The existing `Index`, if the value was previously present.
    /// - The writer used to read and write chunks to the file.
    pub indexer: Indexer,
    /// A function that is called to load a `Value` from an `Index`.
    ///
    /// The parameters to this function are:
    ///
    /// - The index to load a value for.
    /// - The paged writer that can be used to read chunks from the file.
    pub loader: Loader,
    /// A [`Reducer`] that reduces many `Index`es or many `ReducedIndex`es into
    /// a single `ReducedIndex` value.
    pub reducer: IndexReducer,
    _phantom: PhantomData<(Value, Index, ReducedIndex)>,
}

impl<Value, Index, ReducedIndex, Indexer, Loader, IndexReducer>
    ModificationContext<Value, Index, ReducedIndex, Indexer, Loader, IndexReducer>
where
    Indexer: FnMut(
        &ArcBytes<'_>,
        Option<&Value>,
        Option<&Index>,
        &mut PagedWriter<'_>,
    ) -> Result<KeyOperation<Index>, Error>,
    IndexReducer: Reducer<Index, ReducedIndex>,
    Loader: FnMut(&Index, &mut PagedWriter<'_>) -> Result<Option<Value>, Error>,
{
    /// Returns a new context.
    pub fn new(
        current_order: usize,
        minimum_children: usize,
        indexer: Indexer,
        loader: Loader,
        reducer: IndexReducer,
    ) -> Self {
        Self {
            current_order,
            minimum_children,
            indexer,
            loader,
            reducer,
            _phantom: PhantomData,
        }
    }
}

#[cfg(any(debug_assertions, feature = "paranoid"))]
macro_rules! assert_children_order {
    ($children:expr) => {
        assert_eq!(
            $children
                .windows(2)
                .find_map(|w| (w[0].key > w[1].key).then(|| (&w[0].key, &w[1].key))),
            None
        );
    };
}
#[cfg(not(any(debug_assertions, feature = "paranoid")))]
macro_rules! assert_children_order {
    ($children:expr) => {};
}

impl<Index, ReducedIndex> BTreeEntry<Index, ReducedIndex>
where
    Index: PositionIndex + Clone + BinarySerialization + Debug + 'static,
    ReducedIndex: Clone + BinarySerialization + Debug + 'static,
{
    /// Modifies this entry. This function may not apply all of `modification`,
    /// as the entry may enter a state in which the parent of this entry needs
    /// to act upon. For example, if this node grows too large, the modification
    /// operation will return [`ChangeResult::Split`].
    ///
    /// When each operation is performed, it is removed from `modification`. To
    /// ensure all modifications are executed, call this functino repeatedly
    /// until `modification` has no keys remaining.
    pub fn modify<IndexedType, Indexer, Loader, IndexReducer>(
        &mut self,
        modification: &mut Modification<'_, IndexedType, Index>,
        context: &mut ModificationContext<
            IndexedType,
            Index,
            ReducedIndex,
            Indexer,
            Loader,
            IndexReducer,
        >,
        max_key: Option<&ArcBytes<'_>>,
        writer: &mut PagedWriter<'_>,
    ) -> Result<ChangeResult, Error>
    where
        Indexer: FnMut(
            &ArcBytes<'_>,
            Option<&IndexedType>,
            Option<&Index>,
            &mut PagedWriter<'_>,
        ) -> Result<KeyOperation<Index>, Error>,
        IndexReducer: Reducer<Index, ReducedIndex>,
        Loader: FnMut(&Index, &mut PagedWriter<'_>) -> Result<Option<IndexedType>, Error>,
    {
        match &mut self.node {
            BTreeNode::Leaf(children) => {
                if Self::modify_leaf(children, modification, context, max_key, writer)? {
                    self.dirty = true;
                    Ok(Self::clean_up_leaf(
                        children,
                        context.current_order,
                        context.minimum_children,
                    ))
                } else {
                    Ok(ChangeResult::Unchanged)
                }
            }
            BTreeNode::Interior(children) => {
                match Self::modify_interior(children, modification, context, max_key, writer)? {
                    ChangeResult::Changed => {
                        self.dirty = true;
                        Ok(Self::clean_up_interior(
                            children,
                            context.current_order,
                            context.minimum_children,
                        ))
                    }
                    other => Ok(other),
                }
            }
            BTreeNode::Uninitialized => unreachable!(),
        }
    }

    fn clean_up_leaf(
        children: &mut [KeyEntry<Index>],
        current_order: usize,
        minimum_children: usize,
    ) -> ChangeResult {
        let child_count = children.len();
        assert_children_order!(children);

        if child_count > current_order {
            ChangeResult::Split
        } else if child_count == 0 {
            ChangeResult::Remove
        } else if child_count < minimum_children {
            ChangeResult::Absorb
        } else {
            ChangeResult::Changed
        }
    }

    fn clean_up_interior(
        children: &mut [Interior<Index, ReducedIndex>],
        current_order: usize,
        minimum_children: usize,
    ) -> ChangeResult {
        let child_count = children.len();
        assert_children_order!(children);

        if child_count > current_order {
            ChangeResult::Split
        } else if child_count == 0 {
            ChangeResult::Remove
        } else if child_count < minimum_children {
            ChangeResult::Absorb
        } else {
            ChangeResult::Changed
        }
    }

    #[allow(clippy::too_many_lines)] // TODO refactor, too many lines
    fn modify_leaf<IndexedType, Indexer, Loader, IndexReducer>(
        children: &mut Vec<KeyEntry<Index>>,
        modification: &mut Modification<'_, IndexedType, Index>,
        context: &mut ModificationContext<
            IndexedType,
            Index,
            ReducedIndex,
            Indexer,
            Loader,
            IndexReducer,
        >,
        max_key: Option<&ArcBytes<'_>>,
        writer: &mut PagedWriter<'_>,
    ) -> Result<bool, Error>
    where
        Indexer: FnMut(
            &ArcBytes<'_>,
            Option<&IndexedType>,
            Option<&Index>,
            &mut PagedWriter<'_>,
        ) -> Result<KeyOperation<Index>, Error>,
        IndexReducer: Reducer<Index, ReducedIndex>,
        Loader: FnMut(&Index, &mut PagedWriter<'_>) -> Result<Option<IndexedType>, Error>,
    {
        let mut last_index = 0;
        let mut any_changes = false;
        let max_len = children.len().max(context.current_order);
        while !modification.keys.is_empty() && children.len() <= max_len {
            let key = modification.keys.last().unwrap();
            if max_key.map(|max_key| key > max_key).unwrap_or_default() {
                break;
            }

            let search_result = children[last_index..].binary_search_by(|child| child.key.cmp(key));
            match search_result {
                Ok(matching_index) => {
                    let key = modification.keys.pop().unwrap();
                    last_index += matching_index;
                    let index = match &mut modification.operation {
                        Operation::Set(value) => (context.indexer)(
                            &key,
                            Some(value),
                            Some(&children[last_index].index),
                            writer,
                        )?,
                        Operation::SetEach(values) => (context.indexer)(
                            &key,
                            Some(&values.pop().ok_or_else(|| {
                                ErrorKind::message("need the same number of keys as values")
                            })?),
                            Some(&children[last_index].index),
                            writer,
                        )?,
                        Operation::Remove => (context.indexer)(
                            &key,
                            None,
                            Some(&children[last_index].index),
                            writer,
                        )?,
                        Operation::CompareSwap(callback) => {
                            let current_index = &children[last_index].index;
                            let existing_value = (context.loader)(current_index, writer)?;
                            match callback(&key, Some(current_index), existing_value) {
                                KeyOperation::Skip => KeyOperation::Skip,
                                KeyOperation::Set(new_value) => (context.indexer)(
                                    &key,
                                    Some(&new_value),
                                    Some(current_index),
                                    writer,
                                )?,
                                KeyOperation::Remove => {
                                    (context.indexer)(&key, None, Some(current_index), writer)?
                                }
                            }
                        }
                    };

                    match index {
                        KeyOperation::Skip => {}
                        KeyOperation::Set(index) => {
                            children[last_index] = KeyEntry {
                                key: key.into_owned(),
                                index,
                            };
                            any_changes = true;
                        }
                        KeyOperation::Remove => {
                            children.remove(last_index);
                            any_changes = true;
                        }
                    }
                }
                Err(insert_at) => {
                    last_index += insert_at;
                    let key = modification.keys.pop().unwrap();
                    let operation = match &mut modification.operation {
                        Operation::Set(new_value) => {
                            (context.indexer)(&key, Some(new_value), None, writer)?
                        }
                        Operation::SetEach(new_values) => (context.indexer)(
                            &key,
                            Some(&new_values.pop().ok_or_else(|| {
                                ErrorKind::message("need the same number of keys as values")
                            })?),
                            None,
                            writer,
                        )?,
                        Operation::Remove => {
                            // The key doesn't exist, so a remove is a no-op.
                            KeyOperation::Remove
                        }
                        Operation::CompareSwap(callback) => match callback(&key, None, None) {
                            KeyOperation::Skip => KeyOperation::Skip,
                            KeyOperation::Set(new_value) => {
                                (context.indexer)(&key, Some(&new_value), None, writer)?
                            }
                            KeyOperation::Remove => (context.indexer)(&key, None, None, writer)?,
                        },
                    };
                    match operation {
                        KeyOperation::Set(index) => {
                            // New node.
                            if children.capacity() < children.len() + 1
                                && context.current_order > children.len()
                            {
                                children.reserve(context.current_order - children.len());
                            }
                            children.insert(
                                last_index,
                                KeyEntry {
                                    key: key.into_owned(),
                                    index,
                                },
                            );
                            any_changes = true;
                        }
                        KeyOperation::Skip | KeyOperation::Remove => {}
                    }
                }
            }
            assert_children_order!(children);
        }
        Ok(any_changes)
    }

    fn modify_interior<IndexedType, Indexer, Loader, IndexReducer>(
        children: &mut Vec<Interior<Index, ReducedIndex>>,
        modification: &mut Modification<'_, IndexedType, Index>,
        context: &mut ModificationContext<
            IndexedType,
            Index,
            ReducedIndex,
            Indexer,
            Loader,
            IndexReducer,
        >,
        max_key: Option<&ArcBytes<'_>>,
        writer: &mut PagedWriter<'_>,
    ) -> Result<ChangeResult, Error>
    where
        Indexer: FnMut(
            &ArcBytes<'_>,
            Option<&IndexedType>,
            Option<&Index>,
            &mut PagedWriter<'_>,
        ) -> Result<KeyOperation<Index>, Error>,
        IndexReducer: Reducer<Index, ReducedIndex>,
        Loader: FnMut(&Index, &mut PagedWriter<'_>) -> Result<Option<IndexedType>, Error>,
    {
        let mut last_index = 0;
        let mut any_changes = false;
        while let Some(key) = modification.keys.last().cloned() {
            if last_index >= children.len()
                || max_key.map(|max_key| &key > max_key).unwrap_or_default()
            {
                break;
            }
            let (containing_node_index, pushing_end) = children[last_index..]
                .binary_search_by(|child| child.key.cmp(&key))
                .map_or_else(
                    |not_found| {
                        if not_found > 0 && not_found + last_index == children.len() {
                            // If we can't find a key less than what would fit
                            // within our children, this key will become the new key
                            // of the last child.
                            (not_found - 1, true)
                        } else {
                            (not_found, false)
                        }
                    },
                    |found| (found, false),
                );

            last_index += containing_node_index;
            let child = &mut children[last_index];
            child.position.load(
                writer.file,
                false,
                writer.vault,
                writer.cache,
                Some(context.current_order),
            )?;
            let child_entry = child.position.get_mut().unwrap();
            let max_key_for_modification = if pushing_end {
                // The new key is being added to the end of the node.
                key
            } else if let Some(max_key) = max_key {
                child.key.clone().min(max_key.clone())
            } else {
                child.key.clone()
            };
            let (change_result, should_backup) = Self::process_interior_change_result(
                child_entry.modify(
                    modification,
                    context,
                    Some(&max_key_for_modification),
                    writer,
                )?,
                last_index,
                children,
                context,
                writer,
            )?;
            assert_children_order!(children);
            match change_result {
                ChangeResult::Unchanged => {}
                ChangeResult::Split => unreachable!(),
                ChangeResult::Absorb => {
                    return Ok(ChangeResult::Absorb);
                }
                ChangeResult::Remove => {
                    any_changes = true;
                }
                ChangeResult::Changed => any_changes = true,
            }
            if should_backup && last_index > 0 {
                last_index -= 1;
            }
        }
        Ok(if any_changes {
            ChangeResult::Changed
        } else {
            ChangeResult::Unchanged
        })
    }

    fn process_interior_change_result<IndexedType, Indexer, Loader, IndexReducer>(
        result: ChangeResult,
        child_index: usize,
        children: &mut Vec<Interior<Index, ReducedIndex>>,
        context: &ModificationContext<
            IndexedType,
            Index,
            ReducedIndex,
            Indexer,
            Loader,
            IndexReducer,
        >,
        writer: &mut PagedWriter<'_>,
    ) -> Result<(ChangeResult, bool), Error>
    where
        Indexer: FnMut(
            &ArcBytes<'_>,
            Option<&IndexedType>,
            Option<&Index>,
            &mut PagedWriter<'_>,
        ) -> Result<KeyOperation<Index>, Error>,
        IndexReducer: Reducer<Index, ReducedIndex>,
        Loader: FnMut(&Index, &mut PagedWriter<'_>) -> Result<Option<IndexedType>, Error>,
    {
        let can_absorb = children.len() > 1;
        match (result, can_absorb) {
            (ChangeResult::Unchanged, _) => Ok((ChangeResult::Unchanged, false)),
            (ChangeResult::Changed, _) | (ChangeResult::Absorb, false) => {
                let child = &mut children[child_index];
                let child_entry = child.position.get_mut().unwrap();
                child.key = child_entry.max_key().clone();
                child.stats = child_entry.stats(&context.reducer);
                Ok((ChangeResult::Changed, result == ChangeResult::Absorb))
            }
            (ChangeResult::Split, _) => {
                Self::process_interior_split(child_index, children, context, writer)
            }
            (ChangeResult::Absorb, true) => {
                Self::process_absorb(child_index, children, context, writer)
            }
            (ChangeResult::Remove, _) => {
                children.remove(child_index);

                Ok((ChangeResult::Changed, true))
            }
        }
    }

    fn process_absorb<IndexedType, Indexer, Loader, IndexReducer>(
        child_index: usize,
        children: &mut Vec<Interior<Index, ReducedIndex>>,
        context: &ModificationContext<
            IndexedType,
            Index,
            ReducedIndex,
            Indexer,
            Loader,
            IndexReducer,
        >,
        writer: &mut PagedWriter<'_>,
    ) -> Result<(ChangeResult, bool), Error>
    where
        Indexer: FnMut(
            &ArcBytes<'_>,
            Option<&IndexedType>,
            Option<&Index>,
            &mut PagedWriter<'_>,
        ) -> Result<KeyOperation<Index>, Error>,
        IndexReducer: Reducer<Index, ReducedIndex>,
        Loader: FnMut(&Index, &mut PagedWriter<'_>) -> Result<Option<IndexedType>, Error>,
    {
        let (insert_on_top, sponge_index) = if child_index > 0 {
            (true, child_index - 1)
        } else {
            (false, child_index)
        };

        if sponge_index < children.len() - 1 {
            let mut removed_child = children.remove(child_index);
            let sponge = children.get_mut(sponge_index).unwrap();

            let removed_child = removed_child.position.get_mut().unwrap();
            let leaves = match &mut removed_child.node {
                BTreeNode::Leaf(leaves) => Children::Leaves(std::mem::take(leaves)),
                BTreeNode::Interior(interiors) => Children::Interiors(std::mem::take(interiors)),
                BTreeNode::Uninitialized => unreachable!(),
            };

            sponge.position.load(
                writer.file,
                false,
                writer.vault,
                writer.cache,
                Some(context.current_order),
            )?;
            let sponge_entry = sponge.position.get_mut().unwrap();
            match Self::process_interior_change_result(
                sponge_entry.absorb(
                    leaves,
                    insert_on_top,
                    context.current_order,
                    context.minimum_children,
                    writer,
                )?,
                sponge_index,
                children,
                context,
                writer,
            )? {
                (ChangeResult::Absorb | ChangeResult::Split | ChangeResult::Unchanged, _) => {
                    unreachable!()
                }
                (ChangeResult::Remove, _) => Ok((ChangeResult::Remove, true)),
                (ChangeResult::Changed, _) => Ok((ChangeResult::Changed, true)),
            }
        } else {
            Ok((ChangeResult::Unchanged, false))
        }
    }

    fn process_interior_split<IndexedType, Indexer, Loader, IndexReducer>(
        child_index: usize,
        children: &mut Vec<Interior<Index, ReducedIndex>>,
        context: &ModificationContext<
            IndexedType,
            Index,
            ReducedIndex,
            Indexer,
            Loader,
            IndexReducer,
        >,
        writer: &mut PagedWriter<'_>,
    ) -> Result<(ChangeResult, bool), Error>
    where
        Indexer: FnMut(
            &ArcBytes<'_>,
            Option<&IndexedType>,
            Option<&Index>,
            &mut PagedWriter<'_>,
        ) -> Result<KeyOperation<Index>, Error>,
        IndexReducer: Reducer<Index, ReducedIndex>,
        Loader: FnMut(&Index, &mut PagedWriter<'_>) -> Result<Option<IndexedType>, Error>,
    {
        let mut should_backup = false;
        // Before adding a new node, we want to first try to use neighboring
        // nodes to absorb enough such that a split is no longer needed. With
        // the dynamic ordering used, we will likely end up with nodes that have
        // less than the minimum number of nodes, if we were to clean them up.
        // This stealing mechanism helps bring node utilization up without
        // requiring much additional IO. Ultimately, the best time to optimize
        // the tree will be during a compaction phase.
        if child_index > 0 {
            match Self::steal_children_from_start(child_index, children, context, writer)? {
                (ChangeResult::Unchanged, steal_results_in_should_backup) => {
                    should_backup = steal_results_in_should_backup;
                }
                (ChangeResult::Changed, should_backup) => {
                    return Ok((ChangeResult::Changed, should_backup))
                }
                _ => unreachable!(),
            }
        }

        if child_index + 1 < children.len() {
            match Self::steal_children_from_end(child_index, children, context, writer)? {
                ChangeResult::Unchanged => {}
                ChangeResult::Changed => return Ok((ChangeResult::Changed, should_backup)),
                _ => unreachable!(),
            }
        }

        let child = children[child_index].position.get_mut().unwrap();
        child.dirty = true;
        let next_node = match &mut child.node {
            BTreeNode::Leaf(children) => {
                let upper = children
                    .splice((children.len() + 1) / 2.., std::iter::empty())
                    .collect::<Vec<_>>();
                Self::from(BTreeNode::Leaf(upper))
            }
            BTreeNode::Interior(children) => {
                let upper = children
                    .splice((children.len() + 1) / 2.., std::iter::empty())
                    .collect::<Vec<_>>();
                Self::from(BTreeNode::Interior(upper))
            }
            BTreeNode::Uninitialized => unimplemented!(),
        };
        let (max_key, stats) = { (child.max_key().clone(), child.stats(&context.reducer)) };
        children[child_index].key = max_key;
        children[child_index].stats = stats;

        children.insert(child_index + 1, Interior::new(next_node, &context.reducer));

        Ok((ChangeResult::Changed, should_backup))
    }

    fn steal_children_from_start<IndexedType, Indexer, Loader, IndexReducer>(
        child_index: usize,
        children: &mut [Interior<Index, ReducedIndex>],
        context: &ModificationContext<
            IndexedType,
            Index,
            ReducedIndex,
            Indexer,
            Loader,
            IndexReducer,
        >,
        writer: &mut PagedWriter<'_>,
    ) -> Result<(ChangeResult, bool), Error>
    where
        Indexer: FnMut(
            &ArcBytes<'_>,
            Option<&IndexedType>,
            Option<&Index>,
            &mut PagedWriter<'_>,
        ) -> Result<KeyOperation<Index>, Error>,
        IndexReducer: Reducer<Index, ReducedIndex>,
        Loader: FnMut(&Index, &mut PagedWriter<'_>) -> Result<Option<IndexedType>, Error>,
    {
        let mut should_backup = false;
        // Check the previous child to see if it can accept any of this child.
        children[child_index - 1].position.load(
            writer.file,
            false,
            writer.vault,
            writer.cache,
            Some(context.current_order),
        )?;
        let previous_child_count = children[child_index - 1].position.get().unwrap().count();
        if let Some(free_space) = context.current_order.checked_sub(previous_child_count) {
            if free_space > 0 {
                should_backup = true;
                // First, take the children from the node that reported it needed to be split.
                let stolen_children =
                    match &mut children[child_index].position.get_mut().unwrap().node {
                        BTreeNode::Leaf(children) => {
                            let eligible_amount =
                                children.len().saturating_sub(context.minimum_children);
                            let amount_to_steal = free_space.min(eligible_amount);
                            Children::Leaves(
                                children
                                    .splice(0..amount_to_steal, std::iter::empty())
                                    .collect(),
                            )
                        }
                        BTreeNode::Interior(children) => {
                            let eligible_amount =
                                children.len().saturating_sub(context.minimum_children);
                            let amount_to_steal = free_space.max(eligible_amount);
                            Children::Interiors(
                                children
                                    .splice(0..amount_to_steal, std::iter::empty())
                                    .collect(),
                            )
                        }
                        BTreeNode::Uninitialized => unreachable!(),
                    };
                // Extend the previous node with the new values
                let previous_child = children[child_index - 1].position.get_mut().unwrap();
                match (&mut previous_child.node, stolen_children) {
                    (BTreeNode::Leaf(children), Children::Leaves(new_entries)) => {
                        children.extend(new_entries);
                    }
                    (BTreeNode::Interior(children), Children::Interiors(new_entries)) => {
                        children.extend(new_entries);
                    }
                    _ => unreachable!(),
                }
                // Update the statistics for the previous child.
                previous_child.dirty = true;
                let (max_key, stats) = {
                    (
                        previous_child.max_key().clone(),
                        previous_child.stats(&context.reducer),
                    )
                };
                children[child_index - 1].key = max_key;
                children[child_index - 1].stats = stats;

                // Update the current child.
                let child = children[child_index].position.get_mut().unwrap();
                child.dirty = true;
                let (max_key, stats) = { (child.max_key().clone(), child.stats(&context.reducer)) };
                children[child_index].key = max_key;
                children[child_index].stats = stats;
                // If we couldn't balance enough, the caller will still need to
                // balance further.
                if child.count() <= context.current_order {
                    return Ok((ChangeResult::Changed, should_backup));
                }
            }
        }

        Ok((ChangeResult::Unchanged, should_backup))
    }

    fn steal_children_from_end<IndexedType, Indexer, Loader, IndexReducer>(
        child_index: usize,
        children: &mut [Interior<Index, ReducedIndex>],
        context: &ModificationContext<
            IndexedType,
            Index,
            ReducedIndex,
            Indexer,
            Loader,
            IndexReducer,
        >,
        writer: &mut PagedWriter<'_>,
    ) -> Result<ChangeResult, Error>
    where
        Indexer: FnMut(
            &ArcBytes<'_>,
            Option<&IndexedType>,
            Option<&Index>,
            &mut PagedWriter<'_>,
        ) -> Result<KeyOperation<Index>, Error>,
        IndexReducer: Reducer<Index, ReducedIndex>,
        Loader: FnMut(&Index, &mut PagedWriter<'_>) -> Result<Option<IndexedType>, Error>,
    {
        // Check the previous child to see if it can accept any of this child.
        children[child_index + 1].position.load(
            writer.file,
            false,
            writer.vault,
            writer.cache,
            Some(context.current_order),
        )?;
        let next_child_count = children[child_index + 1].position.get().unwrap().count();
        if let Some(free_space) = context.current_order.checked_sub(next_child_count) {
            if free_space > 0 {
                // First, take the children from the node that reported it needed to be split.
                let stolen_children =
                    match &mut children[child_index].position.get_mut().unwrap().node {
                        BTreeNode::Leaf(children) => {
                            let eligible_amount =
                                children.len().saturating_sub(context.minimum_children);
                            let amount_to_steal = free_space.min(eligible_amount);
                            Children::Leaves(
                                children
                                    .splice(children.len() - amount_to_steal.., std::iter::empty())
                                    .collect(),
                            )
                        }
                        BTreeNode::Interior(children) => {
                            let eligible_amount =
                                children.len().saturating_sub(context.minimum_children);
                            let amount_to_steal = free_space.max(eligible_amount);
                            Children::Interiors(
                                children
                                    .splice(children.len() - amount_to_steal.., std::iter::empty())
                                    .collect(),
                            )
                        }
                        BTreeNode::Uninitialized => unreachable!(),
                    };
                // Extend the previous node with the new values
                let next_child = children[child_index + 1].position.get_mut().unwrap();
                match (&mut next_child.node, stolen_children) {
                    (BTreeNode::Leaf(children), Children::Leaves(new_entries)) => {
                        children.splice(0..0, new_entries);
                    }
                    (BTreeNode::Interior(children), Children::Interiors(new_entries)) => {
                        children.splice(0..0, new_entries);
                    }
                    _ => unreachable!(),
                }
                // Update the statistics for the previous child.
                next_child.dirty = true;
                let (max_key, stats) = {
                    (
                        next_child.max_key().clone(),
                        next_child.stats(&context.reducer),
                    )
                };
                children[child_index + 1].key = max_key;
                children[child_index + 1].stats = stats;

                // Update the current child.
                let child = children[child_index].position.get_mut().unwrap();
                child.dirty = true;
                let (max_key, stats) = { (child.max_key().clone(), child.stats(&context.reducer)) };
                children[child_index].key = max_key;
                children[child_index].stats = stats;
                // If we couldn't balance enough, the caller will still need to
                // balance further.
                if child.count() <= context.current_order {
                    return Ok(ChangeResult::Changed);
                }
            }
        }

        Ok(ChangeResult::Unchanged)
    }

    fn count(&self) -> usize {
        match &self.node {
            BTreeNode::Uninitialized => unreachable!(),
            BTreeNode::Leaf(children) => children.len(),
            BTreeNode::Interior(children) => children.len(),
        }
    }

    fn absorb(
        &mut self,
        children: Children<Index, ReducedIndex>,
        insert_at_top: bool,
        current_order: usize,
        minimum_children: usize,
        writer: &mut PagedWriter<'_>,
    ) -> Result<ChangeResult, Error> {
        self.dirty = true;
        match (&mut self.node, children) {
            (BTreeNode::Leaf(existing_children), Children::Leaves(leaves)) => {
                if insert_at_top {
                    existing_children.extend(leaves);
                } else {
                    existing_children.splice(0..0, leaves);
                }

                Ok(Self::clean_up_leaf(
                    existing_children,
                    current_order,
                    minimum_children,
                ))
            }
            (BTreeNode::Interior(existing_children), Children::Leaves(leaves)) => {
                // Pass the leaves along to the first or last child.
                let sponge = if insert_at_top {
                    existing_children.last_mut().unwrap()
                } else {
                    existing_children.first_mut().unwrap()
                };
                sponge.position.load(
                    writer.file,
                    false,
                    writer.vault,
                    writer.cache,
                    Some(current_order),
                )?;
                let sponge = sponge.position.get_mut().unwrap();
                sponge.absorb(
                    Children::Leaves(leaves),
                    insert_at_top,
                    current_order,
                    minimum_children,
                    writer,
                )
            }
            (BTreeNode::Interior(existing_children), Children::Interiors(interiors)) => {
                if insert_at_top {
                    existing_children.extend(interiors);
                } else {
                    existing_children.splice(0..0, interiors);
                }

                Ok(Self::clean_up_interior(
                    existing_children,
                    current_order,
                    minimum_children,
                ))
            }
            (BTreeNode::Leaf(_), Children::Interiors(_)) | (BTreeNode::Uninitialized, _) => {
                unreachable!()
            }
        }
    }

    pub(crate) fn split<R>(
        &mut self,
        reducer: &R,
    ) -> (Interior<Index, ReducedIndex>, Interior<Index, ReducedIndex>)
    where
        R: Reducer<Index, ReducedIndex>,
    {
        let mut old_node = Self::from(BTreeNode::Uninitialized);
        std::mem::swap(self, &mut old_node);
        let (lower, upper) = match old_node.node {
            BTreeNode::Leaf(mut children) => {
                let upper = children
                    .splice((children.len() + 1) / 2.., std::iter::empty())
                    .collect::<Vec<_>>();
                let lower = Self::from(BTreeNode::Leaf(children));
                let upper = Self::from(BTreeNode::Leaf(upper));

                (lower, upper)
            }
            BTreeNode::Interior(mut children) => {
                let upper = children
                    .splice((children.len() + 1) / 2.., std::iter::empty())
                    .collect::<Vec<_>>();
                let lower = Self::from(BTreeNode::Interior(children));
                let upper = Self::from(BTreeNode::Interior(upper));

                (lower, upper)
            }
            BTreeNode::Uninitialized => unreachable!(),
        };

        (Interior::new(lower, reducer), Interior::new(upper, reducer))
    }

    /// Replaces this node with a [`BTreeNode::Interior`] containing two
    /// children nodes that were created by splitting the children of this node.
    ///
    /// This function should only be called on a root node.
    pub fn split_root<R>(&mut self, reducer: &R)
    where
        R: Reducer<Index, ReducedIndex>,
    {
        let (lower, upper) = self.split(reducer);
        self.node = BTreeNode::Interior(vec![lower, upper]);
    }

    /// Returns the collected statistics for this node.
    #[must_use]
    pub fn stats<R: Reducer<Index, ReducedIndex>>(&self, reducer: &R) -> ReducedIndex {
        match &self.node {
            BTreeNode::Leaf(children) => reducer.reduce(children.iter().map(|c| &c.index)),
            BTreeNode::Interior(children) => reducer.rereduce(children.iter().map(|c| &c.stats)),
            BTreeNode::Uninitialized => unreachable!(),
        }
    }

    /// Returns the highest-ordered key contained in this node.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn max_key(&self) -> &ArcBytes<'static> {
        match &self.node {
            BTreeNode::Leaf(children) => &children.last().unwrap().key,
            BTreeNode::Interior(children) => &children.last().unwrap().key,
            BTreeNode::Uninitialized => unreachable!(),
        }
    }

    /// Scans this entry looking for keys in `range`.
    ///
    /// For each interior node that may have keys in `range`,
    /// [`ScanArgs::node_evaluator`] is invoked. This allows fine-grained
    /// control over skipping entire branches of a tree or aborting a scan
    /// early. If [`ScanEvaluation::ReadData`] is returned, the interior node's
    /// children will be scanned.
    ///
    /// Once a leaf node is encountered, each key is checked against `range` and
    /// [`ScanArgs::key_evaluator`] is invoked. If [`ScanEvaluation::ReadData`]
    /// is returned, the stored value for the entry will be loaded and
    /// [`ScanArgs::data_callback`] will be invoked.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(self, args, file, vault, cache))
    )]
    pub fn scan<
        'k,
        'keys,
        CallerError: Display + Debug,
        KeyRangeBounds,
        NodeEvaluator,
        KeyEvaluator,
        ScanDataCallback,
    >(
        &self,
        range: &'keys KeyRangeBounds,
        args: &mut ScanArgs<
            ArcBytes<'static>,
            Index,
            ReducedIndex,
            CallerError,
            NodeEvaluator,
            KeyEvaluator,
            ScanDataCallback,
        >,
        file: &mut dyn File,
        vault: Option<&dyn AnyVault>,
        cache: Option<&ChunkCache>,
        current_depth: usize,
    ) -> Result<bool, AbortError<CallerError>>
    where
        NodeEvaluator: FnMut(&ArcBytes<'static>, &ReducedIndex, usize) -> ScanEvaluation,
        KeyEvaluator: FnMut(&ArcBytes<'static>, &Index) -> ScanEvaluation,
        KeyRangeBounds: RangeBounds<&'keys [u8]> + Debug + ?Sized,
        ScanDataCallback: FnMut(
            ArcBytes<'static>,
            &Index,
            ArcBytes<'static>,
        ) -> Result<(), AbortError<CallerError>>,
    {
        match &self.node {
            BTreeNode::Leaf(children) => {
                for child in DirectionalSliceIterator::new(args.forwards, children) {
                    if range.contains(&child.key.as_slice()) {
                        match (args.key_evaluator)(&child.key, &child.index) {
                            ScanEvaluation::ReadData => {
                                if child.index.position() > 0 {
                                    let data = match read_chunk(
                                        child.index.position(),
                                        false,
                                        file,
                                        vault,
                                        cache,
                                    )? {
                                        CacheEntry::ArcBytes(contents) => contents,
                                        CacheEntry::Decoded(_) => unreachable!(),
                                    };
                                    (args.data_callback)(child.key.clone(), &child.index, data)?;
                                }
                            }
                            ScanEvaluation::Skip => {}
                            ScanEvaluation::Stop => return Ok(false),
                        };
                    }
                }
            }
            BTreeNode::Interior(children) => {
                for (index, child) in
                    DirectionalSliceIterator::new(args.forwards, children).enumerate()
                {
                    // The keys in this child range from the previous child's key (exclusive) to the entry's key (inclusive).
                    let start_bound = range.start_bound();
                    let end_bound = range.end_bound();
                    if args.forwards {
                        if index > 0 {
                            let previous_entry = &children[index - 1];

                            // One the previous entry's key is less than the end
                            // bound, we can break out of the loop.
                            match end_bound {
                                Bound::Included(key) => {
                                    if previous_entry.key > **key {
                                        break;
                                    }
                                }
                                Bound::Excluded(key) => {
                                    if &previous_entry.key >= key {
                                        break;
                                    }
                                }
                                Bound::Unbounded => {}
                            }
                        }
                    } else {
                        // TODO need to write the logic for breaking out when iterating backwards.
                    }

                    // Keys in this child could match as long as the start bound
                    // is less than the key for this child.
                    match start_bound {
                        Bound::Included(key) => {
                            if child.key < **key {
                                continue;
                            }
                        }
                        Bound::Excluded(key) => {
                            if &child.key <= key {
                                continue;
                            }
                        }
                        Bound::Unbounded => {}
                    }

                    let keep_scanning =
                        match (args.node_evaluator)(&child.key, &child.stats, current_depth) {
                            ScanEvaluation::Stop => false,
                            ScanEvaluation::ReadData => child.position.map_loaded_entry(
                                file,
                                vault,
                                cache,
                                Some(children.len()),
                                |entry, file| {
                                    entry.scan(range, args, file, vault, cache, current_depth + 1)
                                },
                            )?,
                            ScanEvaluation::Skip => true,
                        };
                    if !keep_scanning {
                        return Ok(false);
                    }
                }
            }
            BTreeNode::Uninitialized => unreachable!(),
        }
        Ok(true)
    }

    /// Read the values stored for one or more keys.
    ///
    /// `key_evaluator` is invoked once for each key found. If
    /// [`ScanEvaluation::ReadData`] is returned, the key's value will be loaded
    /// and `key_reader` will be invoked with the result.
    ///
    /// The `key_reader` function is not invoked immediately in an effort to
    /// optimize the order of reads from the disk.
    pub fn get_multiple<KeyEvaluator, KeyReader, Keys, Bytes>(
        &self,
        keys: &mut Keys,
        mut key_evaluator: KeyEvaluator,
        mut key_reader: KeyReader,
        file: &mut dyn File,
        vault: Option<&dyn AnyVault>,
        cache: Option<&ChunkCache>,
    ) -> Result<(), Error>
    where
        KeyEvaluator: for<'k> FnMut(&'k ArcBytes<'static>, &'k Index) -> ScanEvaluation,
        KeyReader: FnMut(ArcBytes<'static>, ArcBytes<'static>, Index) -> Result<(), Error>,
        Keys: Iterator<Item = Bytes>,
        Bytes: AsRef<[u8]>,
    {
        let mut positions_to_read = Vec::new();
        self.get(
            &mut KeyRange::new(keys),
            &mut |key, index| key_evaluator(key, index),
            &mut |key, index| {
                // Deleted keys are stored with a 0 position.
                if index.position() > 0 {
                    positions_to_read.push((key, index.clone()));
                }
                Ok(())
            },
            file,
            vault,
            cache,
        )?;

        // Sort by position on disk
        positions_to_read.sort_by(|a, b| a.1.position().cmp(&b.1.position()));

        for (key, index) in positions_to_read {
            if index.position() > 0 {
                match read_chunk(index.position(), false, file, vault, cache)? {
                    CacheEntry::ArcBytes(contents) => {
                        key_reader(key, contents, index)?;
                    }
                    CacheEntry::Decoded(_) => unreachable!(),
                };
            } else {
                key_reader(key, ArcBytes::default(), index)?;
            }
        }
        Ok(())
    }

    /// Read the values stored within a range of keys.
    ///
    /// `key_evaluator` is invoked once for each key found. If
    /// [`ScanEvaluation::ReadData`] is returned, the key's value will be loaded
    /// and `key_reader` will be invoked with the result.
    ///
    /// The `key_reader` function is not invoked immediately in an effort to
    /// optimize the order of reads from the disk.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(self, key_evaluator, keys, key_reader, file, vault, cache))
    )]
    fn get<KeyEvaluator, KeyReader, Keys, Bytes>(
        &self,
        keys: &mut KeyRange<Keys, Bytes>,
        key_evaluator: &mut KeyEvaluator,
        key_reader: &mut KeyReader,
        file: &mut dyn File,
        vault: Option<&dyn AnyVault>,
        cache: Option<&ChunkCache>,
    ) -> Result<bool, Error>
    where
        KeyEvaluator: FnMut(&ArcBytes<'static>, &Index) -> ScanEvaluation,
        KeyReader: FnMut(ArcBytes<'static>, &Index) -> Result<(), AbortError<Infallible>>,
        Keys: Iterator<Item = Bytes>,
        Bytes: AsRef<[u8]>,
    {
        match &self.node {
            BTreeNode::Leaf(children) => {
                let mut last_index = 0;
                let mut took_one_key = false;
                while let Some(key) = keys.current_key() {
                    match children[last_index..].binary_search_by(|child| (&*child.key).cmp(key)) {
                        Ok(matching) => {
                            took_one_key = true;
                            keys.next();
                            last_index += matching;
                            let entry = &children[last_index];
                            match key_evaluator(&entry.key, &entry.index) {
                                ScanEvaluation::ReadData => {
                                    key_reader(entry.key.clone(), &entry.index)?;
                                }
                                ScanEvaluation::Skip => {}
                                ScanEvaluation::Stop => return Ok(false),
                            }
                        }
                        Err(location) => {
                            last_index += location;
                            if last_index == children.len() && took_one_key {
                                // No longer matching within this tree
                                break;
                            }

                            // Key not found.
                            took_one_key = true;
                            keys.next();
                        }
                    }
                }
            }
            BTreeNode::Interior(children) => {
                let mut last_index = 0;
                while let Some(key) = keys.current_key() {
                    let containing_node_index = children[last_index..]
                        .binary_search_by(|child| (&*child.key).cmp(key))
                        .unwrap_or_else(|not_found| not_found);
                    last_index += containing_node_index;

                    // This isn't guaranteed to succeed because we add one. If
                    // the key being searched for isn't contained, it will be
                    // greater than any of the node's keys.
                    if let Some(child) = children.get(last_index) {
                        let keep_scanning = child.position.map_loaded_entry(
                            file,
                            vault,
                            cache,
                            Some(children.len()),
                            |entry, file| {
                                entry
                                    .get(keys, key_evaluator, key_reader, file, vault, cache)
                                    .map_err(AbortError::Nebari)
                            },
                        )?;
                        if !keep_scanning {
                            break;
                        }
                        // The leaf will consume all the keys that can match.
                        // Thus, we can skip it on the next iteration.
                        last_index += 1;
                    } else {
                        break;
                    }
                }
            }
            BTreeNode::Uninitialized => unreachable!(),
        }
        Ok(true)
    }

    /// Recursively copy all stored data from `file` to `writer`.
    ///
    /// Returns true if any data was copied.
    #[allow(clippy::too_many_arguments)]
    pub fn copy_data_to<Callback>(
        &mut self,
        include_nodes: NodeInclusion,
        file: &mut dyn File,
        copied_chunks: &mut HashMap<u64, u64>,
        writer: &mut PagedWriter<'_>,
        vault: Option<&dyn AnyVault>,
        scratch: &mut Vec<u8>,
        index_callback: &mut Callback,
    ) -> Result<bool, Error>
    where
        Callback: FnMut(
            &ArcBytes<'static>,
            &mut Index,
            &mut dyn File,
            &mut HashMap<u64, u64>,
            &mut PagedWriter<'_>,
            Option<&dyn AnyVault>,
        ) -> Result<bool, Error>,
    {
        let mut any_changes = false;
        match &mut self.node {
            BTreeNode::Leaf(children) => {
                for child in children {
                    any_changes =
                        child.copy_data_to(file, copied_chunks, writer, vault, index_callback)?
                            || any_changes;
                }
            }
            BTreeNode::Interior(children) => {
                for child in children {
                    any_changes = child.copy_data_to(
                        include_nodes.next(),
                        file,
                        copied_chunks,
                        writer,
                        vault,
                        scratch,
                        index_callback,
                    )? || any_changes;
                }
            }
            BTreeNode::Uninitialized => unreachable!(),
        }
        // Regardless of if data was copied, data locations have been updated,
        // so the node is considered dirty.
        self.dirty |= true;
        Ok(any_changes)
    }
}

/// Determines whether a data copy should include nodes or just data.
#[derive(Clone, Debug, Copy, Eq, PartialEq)]
pub enum NodeInclusion {
    /// Excludes nodes from this copy.
    Exclude,
    /// Excludes the current node, but includes all subsequent nodes.
    IncludeNext,
    /// Includes all nodes.
    Include,
}

impl NodeInclusion {
    pub(crate) const fn next(self) -> Self {
        match self {
            Self::Exclude => Self::Exclude,
            Self::IncludeNext | Self::Include => Self::Include,
        }
    }

    pub(crate) const fn should_include(self) -> bool {
        matches!(self, Self::Include)
    }
}

impl<
        Index: Clone + BinarySerialization + Debug + 'static,
        ReducedIndex: Clone + BinarySerialization + Debug + 'static,
    > BinarySerialization for BTreeEntry<Index, ReducedIndex>
{
    fn serialize_to(
        &mut self,
        writer: &mut Vec<u8>,
        paged_writer: &mut PagedWriter<'_>,
    ) -> Result<usize, Error> {
        self.dirty = false;
        let mut bytes_written = 0;
        // The next byte determines the node type.
        match &mut self.node {
            BTreeNode::Leaf(leafs) => {
                assert_children_order!(leafs);
                writer.write_u8(1)?;
                bytes_written += 1;
                for leaf in leafs {
                    bytes_written += leaf.serialize_to(writer, paged_writer)?;
                }
            }
            BTreeNode::Interior(interiors) => {
                assert_children_order!(interiors);
                writer.write_u8(0)?;
                bytes_written += 1;
                for interior in interiors {
                    bytes_written += interior.serialize_to(writer, paged_writer)?;
                }
            }
            BTreeNode::Uninitialized => unreachable!(),
        }

        Ok(bytes_written)
    }

    fn deserialize_from(
        reader: &mut ArcBytes<'_>,
        current_order: Option<usize>,
    ) -> Result<Self, Error> {
        let node_header = reader.read_u8()?;
        match node_header {
            0 => {
                // Interior
                let mut nodes = Vec::new();
                if let Some(current_order) = current_order {
                    nodes.reserve(current_order);
                }
                while !reader.is_empty() {
                    nodes.push(Interior::deserialize_from(reader, current_order)?);
                }
                Ok(Self {
                    node: BTreeNode::Interior(nodes),
                    dirty: false,
                })
            }
            1 => {
                // Leaf
                let mut nodes = Vec::new();
                if let Some(current_order) = current_order {
                    nodes.reserve(current_order);
                }
                while !reader.is_empty() {
                    nodes.push(KeyEntry::deserialize_from(reader, current_order)?);
                }
                Ok(Self {
                    node: BTreeNode::Leaf(nodes),
                    dirty: false,
                })
            }
            _ => Err(Error::data_integrity("invalid node header")),
        }
    }
}

/// An operation to perform on a key.
#[derive(Debug)]
pub enum KeyOperation<T> {
    /// Do not alter the key.
    Skip,
    /// Set the key to the new value.
    Set(T),
    /// Remove the key.
    Remove,
}

struct DirectionalSliceIterator<'a, I> {
    forwards: bool,
    index: usize,
    contents: &'a [I],
}

impl<'a, I> DirectionalSliceIterator<'a, I> {
    pub const fn new(forwards: bool, contents: &'a [I]) -> Self {
        Self {
            forwards,
            contents,
            index: if forwards { 0 } else { contents.len() },
        }
    }
}

impl<'a, I> Iterator for DirectionalSliceIterator<'a, I> {
    type Item = &'a I;

    fn next(&mut self) -> Option<Self::Item> {
        if self.forwards && self.index < self.contents.len() {
            let element = &self.contents[self.index];
            self.index += 1;
            Some(element)
        } else if !self.forwards && self.index > 0 {
            self.index -= 1;
            Some(&self.contents[self.index])
        } else {
            None
        }
    }
}

/// Arguments for a tree scan operation.
pub struct ScanArgs<
    Value,
    Index,
    ReducedIndex,
    CallerError: Display + Debug,
    NodeEvaluator,
    KeyEvaluator,
    DataCallback,
> where
    NodeEvaluator: FnMut(&ArcBytes<'static>, &ReducedIndex, usize) -> ScanEvaluation,
    KeyEvaluator: FnMut(&ArcBytes<'static>, &Index) -> ScanEvaluation,
    DataCallback: FnMut(ArcBytes<'static>, &Index, Value) -> Result<(), AbortError<CallerError>>,
{
    /// Controls the order of the scan. If true, the scan starts at the lowest
    /// ordered key and scans forward. If false, the scan starts at the highest
    /// ordered key and scans in reverse.
    pub forwards: bool,
    /// A callback that is invoked for each interior node being considered in
    /// the scan operation.
    pub node_evaluator: NodeEvaluator,
    /// A callback that is invoked for each matching key found during the scan
    /// operation.
    pub key_evaluator: KeyEvaluator,
    /// A callback that is invoked once data has been loaded for a key during
    /// the scan operation.
    pub data_callback: DataCallback,
    _phantom: PhantomData<(Value, Index, ReducedIndex, CallerError)>,
}

impl<
        Value,
        Index,
        ReducedIndex,
        CallerError: Display + Debug,
        NodeEvaluator,
        KeyEvaluator,
        DataCallback,
    > ScanArgs<Value, Index, ReducedIndex, CallerError, NodeEvaluator, KeyEvaluator, DataCallback>
where
    NodeEvaluator: FnMut(&ArcBytes<'static>, &ReducedIndex, usize) -> ScanEvaluation,
    KeyEvaluator: FnMut(&ArcBytes<'static>, &Index) -> ScanEvaluation,
    DataCallback: FnMut(ArcBytes<'static>, &Index, Value) -> Result<(), AbortError<CallerError>>,
{
    /// Returns a new instance of the scan arguments.
    pub fn new(
        forwards: bool,
        node_evaluator: NodeEvaluator,
        key_evaluator: KeyEvaluator,
        data_callback: DataCallback,
    ) -> Self {
        Self {
            forwards,
            node_evaluator,
            key_evaluator,
            data_callback,
            _phantom: PhantomData,
        }
    }
}
