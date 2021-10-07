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
    versioned::ChangeResult,
    KeyRange, PagedWriter,
};
use crate::{
    error::Error, io::ManagedFile, tree::KeyEvaluation, AbortError, Buffer, ChunkCache, ErrorKind,
    Vault,
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
pub trait Reducer<Index> {
    /// Returns the number of keys that this index and its children contain.
    fn key_count(&self) -> u64;

    /// Reduces one or more indexes into a single reduced index.
    fn reduce(indexes: &[&Index]) -> Self;

    /// Reduces one or more previously-reduced indexes into a single reduced index.
    fn rereduce(reductions: &[&Self]) -> Self;
}

impl<Index> Reducer<Index> for () {
    fn key_count(&self) -> u64 {
        0
    }

    fn reduce(_indexes: &[&Index]) -> Self {}

    fn rereduce(_reductions: &[&Self]) -> Self {}
}

pub struct ModificationContext<IndexedType, File, Index, Context, Indexer, Loader>
where
    Indexer: Fn(
        &Buffer<'_>,
        Option<&IndexedType>,
        Option<&Index>,
        &mut Context,
        &mut PagedWriter<'_, File>,
    ) -> Result<KeyOperation<Index>, Error>,
    Loader: Fn(&Index, &mut PagedWriter<'_, File>) -> Result<Option<IndexedType>, Error>,
{
    pub current_order: usize,
    pub minimum_children: usize,
    pub indexer: Indexer,
    pub loader: Loader,
    pub _phantom: PhantomData<(IndexedType, File, Index, Context)>,
}

impl<Index, ReducedIndex> BTreeEntry<Index, ReducedIndex>
where
    Index: Clone + BinarySerialization + Debug + 'static,
    ReducedIndex: Clone + Reducer<Index> + BinarySerialization + Debug + 'static,
{
    pub(crate) fn modify<File, IndexedType, Context, Indexer, Loader>(
        &mut self,
        modification: &mut Modification<'_, IndexedType>,
        context: &ModificationContext<IndexedType, File, Index, Context, Indexer, Loader>,
        max_key: Option<&Buffer<'_>>,
        changes: &mut Context,
        writer: &mut PagedWriter<'_, File>,
    ) -> Result<ChangeResult<Index, ReducedIndex>, Error>
    where
        File: ManagedFile,
        Indexer: Fn(
            &Buffer<'_>,
            Option<&IndexedType>,
            Option<&Index>,
            &mut Context,
            &mut PagedWriter<'_, File>,
        ) -> Result<KeyOperation<Index>, Error>,
        Loader: Fn(&Index, &mut PagedWriter<'_, File>) -> Result<Option<IndexedType>, Error>,
    {
        match &mut self.node {
            BTreeNode::Leaf(children) => {
                if Self::modify_leaf(children, modification, context, max_key, changes, writer)? {
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
                match Self::modify_interior(
                    children,
                    modification,
                    context,
                    max_key,
                    changes,
                    writer,
                )? {
                    ChangeResult::Changed => {
                        self.dirty = true;
                        Ok(Self::clean_up_interior(children, context.current_order))
                    }
                    other => Ok(other),
                }
            }
            BTreeNode::Uninitialized => unreachable!(),
        }
    }

    fn clean_up_leaf(
        children: &mut Vec<KeyEntry<Index>>,
        current_order: usize,
        minimum_children: usize,
    ) -> ChangeResult<Index, ReducedIndex> {
        let child_count = children.len();

        if child_count >= current_order {
            // We need to split this leaf into two leafs, moving a new interior node using the middle element.
            let midpoint = children.len() / 2;
            let (_, upper_half) = children.split_at(midpoint);
            let upper_half = BTreeNode::Leaf(upper_half.to_vec());
            children.truncate(midpoint);

            ChangeResult::Split(Self::from(upper_half))
        } else if child_count == 0 {
            ChangeResult::Remove
        } else if child_count < minimum_children {
            ChangeResult::Absorb(std::mem::take(children))
        } else {
            ChangeResult::Changed
        }
    }

    fn clean_up_interior(
        children: &mut Vec<Interior<Index, ReducedIndex>>,
        current_order: usize,
    ) -> ChangeResult<Index, ReducedIndex> {
        let child_count = children.len();

        if child_count >= current_order {
            let midpoint = children.len() / 2;
            let (_, upper_half) = children.split_at(midpoint);

            // TODO this re-clones the upper-half children, but splitting a vec
            // without causing multiple copies of data seems
            // impossible without unsafe.
            let upper_half = upper_half.to_vec();
            debug_assert_eq!(midpoint + upper_half.len(), children.len());
            children.truncate(midpoint);

            ChangeResult::Split(Self::from(BTreeNode::Interior(upper_half)))
        } else if child_count == 0 {
            ChangeResult::Remove
        } else {
            ChangeResult::Changed
        }
    }

    #[allow(clippy::too_many_lines)] // TODO refactor, too many lines
    fn modify_leaf<File, IndexedType, Context, Indexer, Loader>(
        children: &mut Vec<KeyEntry<Index>>,
        modification: &mut Modification<'_, IndexedType>,
        context: &ModificationContext<IndexedType, File, Index, Context, Indexer, Loader>,
        max_key: Option<&Buffer<'_>>,
        changes: &mut Context,
        writer: &mut PagedWriter<'_, File>,
    ) -> Result<bool, Error>
    where
        File: ManagedFile,
        Indexer: Fn(
            &Buffer<'_>,
            Option<&IndexedType>,
            Option<&Index>,
            &mut Context,
            &mut PagedWriter<'_, File>,
        ) -> Result<KeyOperation<Index>, Error>,
        Loader: Fn(&Index, &mut PagedWriter<'_, File>) -> Result<Option<IndexedType>, Error>,
    {
        let mut last_index = 0;
        let mut any_changes = false;
        let max_len = children.len().max(context.current_order);
        while !modification.keys.is_empty() && children.len() <= max_len {
            let key = modification.keys.last().unwrap();
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
                            changes,
                            writer,
                        )?,
                        Operation::SetEach(values) => (context.indexer)(
                            &key,
                            Some(&values.pop().ok_or_else(|| {
                                ErrorKind::message("need the same number of keys as values")
                            })?),
                            Some(&children[last_index].index),
                            changes,
                            writer,
                        )?,
                        Operation::Remove => (context.indexer)(
                            &key,
                            None,
                            Some(&children[last_index].index),
                            changes,
                            writer,
                        )?,
                        Operation::CompareSwap(callback) => {
                            let current_index = &children[last_index].index;
                            let existing_value = (context.loader)(current_index, writer)?;
                            match callback(&key, existing_value) {
                                KeyOperation::Skip => KeyOperation::Skip,
                                KeyOperation::Set(new_value) => (context.indexer)(
                                    &key,
                                    Some(&new_value),
                                    Some(current_index),
                                    changes,
                                    writer,
                                )?,
                                KeyOperation::Remove => (context.indexer)(
                                    &key,
                                    None,
                                    Some(current_index),
                                    changes,
                                    writer,
                                )?,
                            }
                        }
                    };

                    match index {
                        KeyOperation::Skip => {}
                        KeyOperation::Set(index) => {
                            children[last_index] = KeyEntry {
                                key: key.to_owned(),
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
                    if max_key.map(|max_key| key > max_key).unwrap_or_default() {
                        break;
                    }
                    let key = modification.keys.pop().unwrap();
                    let operation = match &mut modification.operation {
                        Operation::Set(new_value) => {
                            (context.indexer)(&key, Some(new_value), None, changes, writer)?
                        }
                        Operation::SetEach(new_values) => (context.indexer)(
                            &key,
                            Some(&new_values.pop().ok_or_else(|| {
                                ErrorKind::message("need the same number of keys as values")
                            })?),
                            None,
                            changes,
                            writer,
                        )?,
                        Operation::Remove => {
                            // The key doesn't exist, so a remove is a no-op.
                            KeyOperation::Remove
                        }
                        Operation::CompareSwap(callback) => match callback(&key, None) {
                            KeyOperation::Skip => KeyOperation::Skip,
                            KeyOperation::Set(new_value) => {
                                (context.indexer)(&key, Some(&new_value), None, changes, writer)?
                            }
                            KeyOperation::Remove => {
                                (context.indexer)(&key, None, None, changes, writer)?
                            }
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
                                    key: key.to_owned(),
                                    index,
                                },
                            );
                            any_changes = true;
                        }
                        KeyOperation::Skip | KeyOperation::Remove => {}
                    }
                }
            }
            debug_assert!(children.windows(2).all(|w| w[0].key < w[1].key),);
        }
        Ok(any_changes)
    }

    fn modify_interior<File, IndexedType, Context, Indexer, Loader>(
        children: &mut Vec<Interior<Index, ReducedIndex>>,
        modification: &mut Modification<'_, IndexedType>,
        context: &ModificationContext<IndexedType, File, Index, Context, Indexer, Loader>,
        max_key: Option<&Buffer<'_>>,
        changes: &mut Context,
        writer: &mut PagedWriter<'_, File>,
    ) -> Result<ChangeResult<Index, ReducedIndex>, Error>
    where
        File: ManagedFile,
        Indexer: Fn(
            &Buffer<'_>,
            Option<&IndexedType>,
            Option<&Index>,
            &mut Context,
            &mut PagedWriter<'_, File>,
        ) -> Result<KeyOperation<Index>, Error>,
        Loader: Fn(&Index, &mut PagedWriter<'_, File>) -> Result<Option<IndexedType>, Error>,
    {
        let mut last_index = 0;
        let mut any_changes = false;
        while let Some(key) = modification.keys.last().cloned() {
            if max_key.map(|max_key| &key > max_key).unwrap_or_default() {
                break;
            }
            let containing_node_index = children[last_index..]
                .binary_search_by(|child| child.key.cmp(&key))
                .map_or_else(
                    |not_found| {
                        if not_found + last_index == children.len() {
                            // If we can't find a key less than what would fit
                            // within our children, this key will become the new key
                            // of the last child.
                            not_found - 1
                        } else {
                            not_found
                        }
                    },
                    |found| found,
                );

            last_index += containing_node_index;
            let child = &mut children[last_index];
            child.position.load(
                writer.file,
                false,
                writer.vault,
                writer.cache,
                context.current_order,
            )?;
            let child_entry = child.position.get_mut().unwrap();
            match Self::process_interior_change_result(
                child_entry.modify(
                    modification,
                    context,
                    Some(&key.max(child.key.clone())),
                    changes,
                    writer,
                )?,
                last_index,
                children,
                context,
                writer,
            )? {
                ChangeResult::Unchanged => {}
                ChangeResult::Split(_) => unreachable!(),
                ChangeResult::Absorb(leaves) => {
                    return Ok(ChangeResult::Absorb(leaves));
                }
                ChangeResult::Remove => {
                    any_changes = true;
                }
                ChangeResult::Changed => any_changes = true,
            }
            debug_assert!(children.windows(2).all(|w| w[0].key < w[1].key));
        }
        Ok(if any_changes {
            ChangeResult::Changed
        } else {
            ChangeResult::Unchanged
        })
    }

    fn process_interior_change_result<File, IndexedType, Context, Indexer, Loader>(
        result: ChangeResult<Index, ReducedIndex>,
        child_index: usize,
        children: &mut Vec<Interior<Index, ReducedIndex>>,
        context: &ModificationContext<IndexedType, File, Index, Context, Indexer, Loader>,
        writer: &mut PagedWriter<'_, File>,
    ) -> Result<ChangeResult<Index, ReducedIndex>, Error>
    where
        File: ManagedFile,
        Indexer: Fn(
            &Buffer<'_>,
            Option<&IndexedType>,
            Option<&Index>,
            &mut Context,
            &mut PagedWriter<'_, File>,
        ) -> Result<KeyOperation<Index>, Error>,
        Loader: Fn(&Index, &mut PagedWriter<'_, File>) -> Result<Option<IndexedType>, Error>,
    {
        match result {
            ChangeResult::Unchanged => Ok(ChangeResult::Unchanged),
            ChangeResult::Changed => {
                let child = &mut children[child_index];
                let child_entry = child.position.get_mut().unwrap();
                child.key = child_entry.max_key().clone();
                child.stats = child_entry.stats();
                Ok(ChangeResult::Changed)
            }
            ChangeResult::Split(upper) => {
                let child = &mut children[child_index];
                let child_entry = child.position.get_mut().unwrap();
                child.key = child_entry.max_key().clone();
                child.stats = child_entry.stats();

                if children.capacity() < children.len() + 1
                    && context.current_order > children.len()
                {
                    children.reserve(context.current_order - children.len());
                }
                children.insert(child_index + 1, Interior::from(upper));
                Ok(ChangeResult::Changed)
            }
            ChangeResult::Absorb(leaves) => {
                children.remove(child_index);
                let (insert_on_top, sponge_index) = if child_index > 0 {
                    (true, child_index - 1)
                } else {
                    (false, child_index)
                };

                if let Some(sponge) = children.get_mut(sponge_index) {
                    sponge.position.load(
                        writer.file,
                        false,
                        writer.vault,
                        writer.cache,
                        context.current_order,
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
                        ChangeResult::Absorb(_)
                        | ChangeResult::Split(_)
                        | ChangeResult::Unchanged => unreachable!(),
                        ChangeResult::Remove | ChangeResult::Changed => Ok(ChangeResult::Remove),
                    }
                } else {
                    Ok(ChangeResult::Absorb(leaves))
                }
            }
            ChangeResult::Remove => {
                children.remove(child_index);

                Ok(ChangeResult::Changed)
            }
        }
    }

    fn absorb<File: ManagedFile>(
        &mut self,
        leaves: Vec<KeyEntry<Index>>,
        insert_at_top: bool,
        current_order: usize,
        minimum_children: usize,
        writer: &mut PagedWriter<'_, File>,
    ) -> Result<ChangeResult<Index, ReducedIndex>, Error> {
        self.dirty = true;
        match &mut self.node {
            BTreeNode::Leaf(existing_children) => {
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
            BTreeNode::Interior(existing_children) => {
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
                    current_order,
                )?;
                let sponge = sponge.position.get_mut().unwrap();
                sponge.absorb(
                    leaves,
                    insert_at_top,
                    current_order,
                    minimum_children,
                    writer,
                )
            }
            BTreeNode::Uninitialized => unreachable!(),
        }
    }

    pub(crate) fn split_root(&mut self, upper: Self) {
        let mut lower = Self::from(BTreeNode::Uninitialized);
        std::mem::swap(self, &mut lower);
        self.node = BTreeNode::Interior(vec![Interior::from(lower), Interior::from(upper)]);
    }

    /// Returns the collected statistics for this node.
    #[must_use]
    pub fn stats(&self) -> ReducedIndex {
        match &self.node {
            BTreeNode::Leaf(children) => {
                ReducedIndex::reduce(&children.iter().map(|c| &c.index).collect::<Vec<_>>())
            }
            BTreeNode::Interior(children) => {
                ReducedIndex::rereduce(&children.iter().map(|c| &c.stats).collect::<Vec<_>>())
            }
            BTreeNode::Uninitialized => unreachable!(),
        }
    }

    /// Returns the highest-ordered key contained in this node.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn max_key(&self) -> &Buffer<'static> {
        match &self.node {
            BTreeNode::Leaf(children) => &children.last().unwrap().key,
            BTreeNode::Interior(children) => &children.last().unwrap().key,
            BTreeNode::Uninitialized => unreachable!(),
        }
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(self, args, file, vault, cache))
    )]
    pub(crate) fn scan<
        'k,
        CallerError: Display + Debug,
        File: ManagedFile,
        KeyRangeBounds,
        KeyEvaluator,
        KeyReader,
    >(
        &self,
        range: &KeyRangeBounds,
        args: &mut ScanArgs<&Index, CallerError, KeyEvaluator, KeyReader>,
        file: &mut File,
        vault: Option<&dyn Vault>,
        cache: Option<&ChunkCache>,
    ) -> Result<bool, AbortError<CallerError>>
    where
        KeyEvaluator: FnMut(&Buffer<'static>) -> KeyEvaluation,
        KeyReader: FnMut(Buffer<'static>, &Index) -> Result<(), AbortError<CallerError>>,
        KeyRangeBounds: RangeBounds<Buffer<'k>> + Debug,
    {
        match &self.node {
            BTreeNode::Leaf(children) => {
                for child in DirectionalSliceIterator::new(args.forwards, children) {
                    if range.contains(&child.key) {
                        match (args.key_evaluator)(&child.key) {
                            KeyEvaluation::ReadData => {
                                (args.key_reader)(child.key.clone(), &child.index)?;
                            }
                            KeyEvaluation::Skip => {}
                            KeyEvaluation::Stop => return Ok(false),
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
                                    if previous_entry.key > *key {
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
                            if child.key < *key {
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

                    let keep_scanning = child.position.map_loaded_entry(
                        file,
                        vault,
                        cache,
                        children.len(),
                        |entry, file| entry.scan(range, args, file, vault, cache),
                    )?;
                    if !keep_scanning {
                        return Ok(false);
                    }
                }
            }
            BTreeNode::Uninitialized => unreachable!(),
        }
        Ok(true)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(self, key_evaluator, key_reader, file, vault, cache))
    )]
    pub(crate) fn get<File: ManagedFile, KeyEvaluator, KeyReader>(
        &self,
        keys: &mut KeyRange<'_>,
        key_evaluator: &mut KeyEvaluator,
        key_reader: &mut KeyReader,
        file: &mut File,
        vault: Option<&dyn Vault>,
        cache: Option<&ChunkCache>,
    ) -> Result<bool, Error>
    where
        KeyEvaluator: FnMut(&Buffer<'static>) -> KeyEvaluation,
        KeyReader: FnMut(Buffer<'static>, &Index) -> Result<(), AbortError<Infallible>>,
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
                            match key_evaluator(&entry.key) {
                                KeyEvaluation::ReadData => {
                                    key_reader(entry.key.clone(), &entry.index)
                                        .map_err(AbortError::infallible)?;
                                }
                                KeyEvaluation::Skip => {}
                                KeyEvaluation::Stop => return Ok(false),
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
                        let keep_scanning = child
                            .position
                            .map_loaded_entry(file, vault, cache, children.len(), |entry, file| {
                                entry
                                    .get(keys, key_evaluator, key_reader, file, vault, cache)
                                    .map_err(AbortError::Nebari)
                            })
                            .map_err(AbortError::infallible)?;
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn copy_data_to<File, Callback>(
        &mut self,
        include_nodes: NodeInclusion,
        file: &mut File,
        copied_chunks: &mut HashMap<u64, u64>,
        writer: &mut PagedWriter<'_, File>,
        vault: Option<&dyn Vault>,
        scratch: &mut Vec<u8>,
        index_callback: &mut Callback,
    ) -> Result<bool, Error>
    where
        File: ManagedFile,
        Callback: FnMut(
            &Buffer<'static>,
            &mut Index,
            &mut File,
            &mut HashMap<u64, u64>,
            &mut PagedWriter<'_, File>,
            Option<&dyn Vault>,
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

#[derive(Clone, Debug, Copy, Eq, PartialEq)]
pub enum NodeInclusion {
    Exclude,
    IncludeNext,
    Include,
}

impl NodeInclusion {
    pub const fn next(self) -> Self {
        match self {
            Self::Exclude => Self::Exclude,
            Self::IncludeNext | Self::Include => Self::Include,
        }
    }

    pub const fn should_include(self) -> bool {
        matches!(self, Self::Include)
    }
}

impl<
        Index: Clone + BinarySerialization + Debug + 'static,
        ReducedIndex: Reducer<Index> + Clone + BinarySerialization + Debug + 'static,
    > BinarySerialization for BTreeEntry<Index, ReducedIndex>
{
    fn serialize_to<File: ManagedFile>(
        &mut self,
        writer: &mut Vec<u8>,
        paged_writer: &mut PagedWriter<'_, File>,
    ) -> Result<usize, Error> {
        let mut bytes_written = 0;
        // The next byte determines the node type.
        match &mut self.node {
            BTreeNode::Leaf(leafs) => {
                debug_assert!(leafs.windows(2).all(|w| w[0].key < w[1].key));
                writer.write_u8(1)?;
                bytes_written += 1;
                for leaf in leafs {
                    bytes_written += leaf.serialize_to(writer, paged_writer)?;
                }
            }
            BTreeNode::Interior(interiors) => {
                debug_assert!(interiors.windows(2).all(|w| w[0].key < w[1].key));
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

    fn deserialize_from(reader: &mut Buffer<'_>, current_order: usize) -> Result<Self, Error> {
        let node_header = reader.read_u8()?;
        match node_header {
            0 => {
                // Interior
                let mut nodes = Vec::new();
                nodes.reserve(current_order);
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
                nodes.reserve(current_order);
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

pub struct ScanArgs<Index, CallerError: Display + Debug, KeyEvaluator, KeyReader>
where
    KeyEvaluator: FnMut(&Buffer<'static>) -> KeyEvaluation,
    KeyReader: FnMut(Buffer<'static>, Index) -> Result<(), AbortError<CallerError>>,
{
    pub forwards: bool,
    pub key_evaluator: KeyEvaluator,
    pub key_reader: KeyReader,
    _phantom: PhantomData<(Index, CallerError)>,
}

impl<Index, CallerError: Display + Debug, KeyEvaluator, KeyReader>
    ScanArgs<Index, CallerError, KeyEvaluator, KeyReader>
where
    KeyEvaluator: FnMut(&Buffer<'static>) -> KeyEvaluation,
    KeyReader: FnMut(Buffer<'static>, Index) -> Result<(), AbortError<CallerError>>,
{
    pub fn new(forwards: bool, key_evaluator: KeyEvaluator, key_reader: KeyReader) -> Self {
        Self {
            forwards,
            key_evaluator,
            key_reader,
            _phantom: PhantomData,
        }
    }
}
