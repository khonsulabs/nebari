use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    ops::RangeBounds,
};

use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};
use sediment::format::GrainId;

use super::{
    btree::BTreeEntry,
    by_id::{ByIdStats, UnversionedByIdIndex},
    modify::Modification,
    serialization::BinarySerialization,
    PagedWriter, ScanEvaluation,
};
use crate::{
    chunk_cache::CacheEntry,
    error::{Error, InternalError},
    storage::BlobStorage,
    transaction::TransactionId,
    tree::{
        btree::{Indexer, KeyOperation, ModificationContext, NodeInclusion, ScanArgs},
        by_id::ByIdIndexer,
        dynamic_order, BTreeNode, ChangeResult, ModificationResult, PageHeader, Root,
    },
    vault::AnyVault,
    AbortError, ArcBytes, ChunkCache, ErrorKind,
};

/// An unversioned tree with no additional indexed data.
pub type Unversioned = UnversionedTreeRoot<()>;

/// A versioned B-Tree root. This tree root internally uses two btrees, one to
/// keep track of all writes using a unique "sequence" ID, and one that keeps
/// track of all key-value pairs.
#[derive(Clone, Debug)]
pub struct UnversionedTreeRoot<Index>
where
    Index: Clone + super::EmbeddedIndex<ArcBytes<'static>> + Debug + 'static,
{
    /// The transaction ID of the tree root. If this transaction ID isn't
    /// present in the transaction log, this root should not be trusted.
    pub transaction_id: Option<TransactionId>,
    /// The by-id B-Tree.
    pub by_id_root:
        BTreeEntry<UnversionedByIdIndex<Index, ArcBytes<'static>>, ByIdStats<Index::Reduced>>,

    reducer: <Self as Root>::Reducer,
}

impl<Index> Default for UnversionedTreeRoot<Index>
where
    Index: Clone + super::EmbeddedIndex<ArcBytes<'static>> + Debug + 'static,
    <Self as Root>::Reducer: Default,
{
    fn default() -> Self {
        Self {
            transaction_id: None,
            by_id_root: BTreeEntry::default(),
            reducer: <<Self as Root>::Reducer as Default>::default(),
        }
    }
}

impl<Index> UnversionedTreeRoot<Index>
where
    Index: Clone + super::EmbeddedIndex<ArcBytes<'static>> + Debug + 'static,
{
    fn modify_id_root(
        &mut self,
        mut modification: Modification<
            '_,
            ArcBytes<'static>,
            UnversionedByIdIndex<Index, ArcBytes<'static>>,
        >,
        writer: &mut PagedWriter<'_, '_>,
        max_order: Option<usize>,
    ) -> Result<Vec<ModificationResult<UnversionedByIdIndex<Index, ArcBytes<'static>>>>, Error>
    {
        modification.prepare()?;

        let total_keys =
            self.by_id_root.stats(self.reducer()).total_keys() + modification.keys.len() as u64;
        let by_id_order = dynamic_order(total_keys, max_order);
        let minimum_children = by_id_order / 2 - 1;
        let minimum_children =
            minimum_children.min(usize::try_from(total_keys).unwrap_or(usize::MAX));

        let reducer = self.reducer.clone();

        let mut results = Vec::with_capacity(modification.keys.len());

        while !modification.keys.is_empty() {
            match self.by_id_root.modify(
                &mut modification,
                &mut ModificationContext::new(
                    by_id_order,
                    minimum_children,
                    |key: &ArcBytes<'_>,
                     value: Option<&ArcBytes<'static>>,
                     _existing_index,
                     writer: &mut PagedWriter<'_, '_>| {
                        if let Some(value) = value {
                            let position = writer.write_chunk_cached(value.clone())?;
                            // write_chunk errors if it can't fit within a u32
                            #[allow(clippy::cast_possible_truncation)]
                            let value_length = value.len() as u32;
                            let new_index = UnversionedByIdIndex::new(
                                value_length,
                                Some(position),
                                reducer.0.index(key, Some(value)),
                            );
                            results.push(ModificationResult {
                                key: key.to_owned(),
                                index: Some(new_index.clone()),
                            });
                            Ok(KeyOperation::Set(new_index))
                        } else {
                            results.push(ModificationResult {
                                key: key.to_owned(),
                                index: None,
                            });
                            Ok(KeyOperation::Remove)
                        }
                    },
                    |index, writer| {
                        if let Some(position) = index.position {
                            match writer.read_chunk(position)? {
                                CacheEntry::ArcBytes(buffer) => Ok(Some(buffer.clone())),
                                CacheEntry::Decoded(_) => unreachable!(),
                            }
                        } else {
                            Ok(None)
                        }
                    },
                    self.reducer().clone(),
                ),
                None,
                writer,
            )? {
                ChangeResult::Absorb | ChangeResult::Changed | ChangeResult::Unchanged => {}
                ChangeResult::Remove => {
                    self.by_id_root.node = BTreeNode::Leaf(vec![]);
                    self.by_id_root.dirty = true;
                }
                ChangeResult::Split => {
                    self.by_id_root.split_root(&self.reducer().clone());
                }
            }
        }

        Ok(results)
    }
}

impl<EmbeddedIndex> Root for UnversionedTreeRoot<EmbeddedIndex>
where
    EmbeddedIndex: Clone + super::EmbeddedIndex<ArcBytes<'static>> + 'static,
{
    const HEADER: PageHeader = PageHeader::UnversionedHeader;
    type Value = ArcBytes<'static>;
    type Index = UnversionedByIdIndex<EmbeddedIndex, Self::Value>;
    type ReducedIndex = ByIdStats<EmbeddedIndex::Reduced>;
    type Reducer = ByIdIndexer<EmbeddedIndex::Indexer>;

    fn default_with(reducer: Self::Reducer) -> Self {
        Self {
            transaction_id: None,
            by_id_root: BTreeEntry::default(),
            reducer,
        }
    }

    fn reducer(&self) -> &Self::Reducer {
        &self.reducer
    }

    fn count(&self) -> u64 {
        self.by_id_root.stats(self.reducer()).alive_keys
    }

    fn dirty(&self) -> bool {
        self.by_id_root.dirty
    }

    fn initialized(&self) -> bool {
        self.transaction_id.is_some()
    }

    fn initialize_default(&mut self) {
        self.transaction_id = Some(TransactionId(0));
    }

    fn serialize(
        &mut self,
        paged_writer: &mut PagedWriter<'_, '_>,
        output: &mut Vec<u8>,
    ) -> Result<(), Error> {
        output.write_u64::<BigEndian>(
            self.transaction_id
                .expect("serializing an uninitialized root")
                .0,
        )?;
        // Reserve space for by_id size.
        output.write_u32::<BigEndian>(0)?;

        let by_id_size = self.by_id_root.serialize_to(output, paged_writer)?;
        let by_id_size = u32::try_from(by_id_size)
            .ok()
            .ok_or(ErrorKind::Internal(InternalError::HeaderTooLarge))?;
        BigEndian::write_u32(&mut output[8..12], by_id_size);

        Ok(())
    }

    fn deserialize(mut bytes: ArcBytes<'_>, reducer: Self::Reducer) -> Result<Self, Error> {
        let transaction_id = Some(TransactionId(bytes.read_u64::<BigEndian>()?));
        let by_id_size = bytes.read_u32::<BigEndian>()? as usize;
        if by_id_size != bytes.len() {
            return Err(Error::data_integrity(format!(
                "Header reported index size {}, but data has {} remaining",
                by_id_size,
                bytes.len()
            )));
        };

        let mut by_id_bytes = bytes.read_bytes(by_id_size)?.to_owned();

        let by_id_root = BTreeEntry::deserialize_from(&mut by_id_bytes, None)?;

        Ok(Self {
            transaction_id,
            by_id_root,
            reducer,
        })
    }

    fn transaction_id(&self) -> TransactionId {
        self.transaction_id.unwrap_or_default()
    }

    fn modify(
        &mut self,
        modification: Modification<'_, ArcBytes<'static>, Self::Index>,
        writer: &mut PagedWriter<'_, '_>,
        max_order: Option<usize>,
    ) -> Result<Vec<ModificationResult<Self::Index>>, Error> {
        let transaction_id = modification.persistence_mode.transaction_id();

        let results = self.modify_id_root(modification, writer, max_order)?;

        // Only update the transaction id if a new one was specified.
        if let Some(transaction_id) = transaction_id {
            self.transaction_id = Some(transaction_id);
        }

        Ok(results)
    }

    fn get_multiple<'keys, KeyEvaluator, KeyReader, Keys>(
        &self,
        keys: &mut Keys,
        key_evaluator: &mut KeyEvaluator,
        key_reader: &mut KeyReader,
        file: &mut dyn BlobStorage,
        vault: Option<&dyn AnyVault>,
        cache: Option<&ChunkCache>,
    ) -> Result<(), Error>
    where
        KeyEvaluator: FnMut(&ArcBytes<'static>, &Self::Index) -> ScanEvaluation,
        KeyReader: FnMut(ArcBytes<'static>, ArcBytes<'static>, Self::Index) -> Result<(), Error>,
        Keys: Iterator<Item = &'keys [u8]>,
    {
        self.by_id_root
            .get_multiple(keys, key_evaluator, key_reader, file, vault, cache)
    }

    fn scan<
        'keys,
        CallerError: Display + Debug,
        NodeEvaluator,
        KeyRangeBounds,
        KeyEvaluator,
        DataCallback,
    >(
        &self,
        range: &'keys KeyRangeBounds,
        mut args: ScanArgs<
            Self::Value,
            Self::Index,
            Self::ReducedIndex,
            CallerError,
            NodeEvaluator,
            KeyEvaluator,
            DataCallback,
        >,
        file: &mut dyn BlobStorage,
        vault: Option<&dyn AnyVault>,
        cache: Option<&ChunkCache>,
    ) -> Result<bool, AbortError<CallerError>>
    where
        NodeEvaluator: FnMut(&ArcBytes<'static>, &Self::ReducedIndex, usize) -> ScanEvaluation,
        KeyEvaluator: FnMut(&ArcBytes<'static>, &Self::Index) -> ScanEvaluation,
        KeyRangeBounds: RangeBounds<&'keys [u8]> + Debug + ?Sized,
        DataCallback: FnMut(
            ArcBytes<'static>,
            &Self::Index,
            ArcBytes<'static>,
        ) -> Result<(), AbortError<CallerError>>,
    {
        self.by_id_root
            .scan(range, &mut args, file, vault, cache, 0)
    }

    fn copy_data_to(
        &mut self,
        include_nodes: bool,
        file: &mut dyn BlobStorage,
        copied_chunks: &mut HashMap<GrainId, GrainId>,
        writer: &mut PagedWriter<'_, '_>,
        vault: Option<&dyn AnyVault>,
    ) -> Result<(), Error> {
        let mut scratch = Vec::new();
        self.by_id_root.copy_data_to(
            if include_nodes {
                NodeInclusion::IncludeNext
            } else {
                NodeInclusion::Exclude
            },
            file,
            copied_chunks,
            writer,
            vault,
            &mut scratch,
            &mut |_key,
                  index: &mut UnversionedByIdIndex<EmbeddedIndex, ArcBytes<'static>>,
                  from_file,
                  copied_chunks,
                  to_file,
                  vault| {
                let new_position = if let Some(position) = index.position {
                    Some(to_file.copy_chunk_from(position, from_file, copied_chunks, vault)?)
                } else {
                    None
                };

                if new_position == index.position {
                    // Data is already in the new file
                    Ok(false)
                } else {
                    index.position = new_position;
                    Ok(true)
                }
            },
        )?;

        Ok(())
    }
}
