use std::{
    array::TryFromSliceError,
    collections::HashMap,
    fmt::{Debug, Display},
    ops::RangeBounds,
};

use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};
use sediment::format::GrainId;

use super::{
    btree::BTreeEntry,
    by_id::{ByIdStats, VersionedByIdIndex},
    by_sequence::{BySequenceIndex, BySequenceStats},
    modify::Modification,
    serialization::BinarySerialization,
    ChangeResult, PagedWriter, ScanEvaluation,
};
use crate::{
    chunk_cache::CacheEntry,
    error::{Error, InternalError},
    storage::BlobStorage,
    transaction::TransactionId,
    tree::{
        btree::{Indexer, KeyOperation, ModificationContext, NodeInclusion, ScanArgs},
        by_id::ByIdIndexer,
        by_sequence::{BySequenceReducer, SequenceId},
        dynamic_order,
        key_entry::KeyEntry,
        modify::Operation,
        BTreeNode, Interior, ModificationResult, PageHeader, PersistenceMode, Reducer, Root,
    },
    vault::AnyVault,
    AbortError, ArcBytes, ChunkCache, ErrorKind,
};

/// An versioned tree with no additional indexed data.
pub type Versioned = VersionedTreeRoot<()>;

/// A versioned B-Tree root. This tree root internally uses two btrees, one to
/// keep track of all writes using a unique "sequence" ID, and one that keeps
/// track of all key-value pairs.
#[derive(Clone, Debug)]
pub struct VersionedTreeRoot<EmbeddedIndex>
where
    EmbeddedIndex: super::EmbeddedIndex<ArcBytes<'static>>,
{
    /// The transaction ID of the tree root. If this transaction ID isn't
    /// present in the transaction log, this root should not be trusted.
    pub transaction_id: TransactionId,
    /// The last sequence ID inside of this root.
    pub sequence: SequenceId,
    /// The by-sequence B-Tree.
    pub by_sequence_root: BTreeEntry<BySequenceIndex<EmbeddedIndex>, BySequenceStats>,
    /// The by-id B-Tree.
    pub by_id_root: BTreeEntry<
        VersionedByIdIndex<EmbeddedIndex, ArcBytes<'static>>,
        ByIdStats<EmbeddedIndex::Reduced>,
    >,

    reducer: ByIdIndexer<EmbeddedIndex::Indexer>,
}
impl<EmbeddedIndex> Default for VersionedTreeRoot<EmbeddedIndex>
where
    EmbeddedIndex: super::EmbeddedIndex<ArcBytes<'static>> + Clone + Debug + 'static,
    EmbeddedIndex::Indexer: Default,
{
    fn default() -> Self {
        Self {
            transaction_id: TransactionId(0),
            sequence: SequenceId(0),
            by_sequence_root: BTreeEntry::default(),
            by_id_root: BTreeEntry::default(),
            reducer: ByIdIndexer(<EmbeddedIndex::Indexer as Default>::default()),
        }
    }
}

#[derive(Debug)]
pub enum Children<Index, ReducedIndex> {
    Leaves(Vec<KeyEntry<Index>>),
    Interiors(Vec<Interior<Index, ReducedIndex>>),
}

impl<EmbeddedIndex> VersionedTreeRoot<EmbeddedIndex>
where
    EmbeddedIndex: super::EmbeddedIndex<ArcBytes<'static>> + Clone + Debug + 'static,
    ByIdIndexer<EmbeddedIndex::Indexer>: Reducer<
        VersionedByIdIndex<EmbeddedIndex, ArcBytes<'static>>,
        ByIdStats<EmbeddedIndex::Reduced>,
    >,
{
    fn modify_sequence_root(
        &mut self,
        mut modification: Modification<
            '_,
            BySequenceIndex<EmbeddedIndex>,
            BySequenceIndex<EmbeddedIndex>,
        >,
        writer: &mut PagedWriter<'_, '_>,
        max_order: Option<usize>,
    ) -> Result<(), Error> {
        // Reverse so that pop is efficient.
        modification.prepare()?;

        let total_sequence_records = self
            .by_sequence_root
            .stats(&BySequenceReducer)
            .total_sequences
            + modification.keys.len() as u64;
        let by_sequence_order = dynamic_order(total_sequence_records, max_order);

        let by_sequence_minimum_children = by_sequence_order / 2 - 1;
        let by_sequence_minimum_children = by_sequence_minimum_children
            .min(usize::try_from(total_sequence_records).unwrap_or(usize::MAX));

        while !modification.keys.is_empty() {
            match self.by_sequence_root.modify(
                &mut modification,
                &mut ModificationContext::new(
                    by_sequence_order,
                    by_sequence_minimum_children,
                    |_key: &ArcBytes<'_>,
                     value: Option<&BySequenceIndex<EmbeddedIndex>>,
                     _existing_index: Option<&BySequenceIndex<EmbeddedIndex>>,
                     _writer: &mut PagedWriter<'_, '_>| {
                        Ok(KeyOperation::Set(value.unwrap().clone()))
                    },
                    |_index: &BySequenceIndex<EmbeddedIndex>, _writer: &mut PagedWriter<'_, '_>| {
                        Ok(None)
                    },
                    BySequenceReducer,
                ),
                None,
                writer,
            )? {
                ChangeResult::Absorb
                | ChangeResult::Remove
                | ChangeResult::Unchanged
                | ChangeResult::Changed => {}
                ChangeResult::Split => {
                    self.by_sequence_root.split_root(&BySequenceReducer);
                }
            }
        }
        Ok(())
    }

    fn modify_id_root(
        &mut self,
        mut modification: Modification<
            '_,
            ArcBytes<'static>,
            VersionedByIdIndex<EmbeddedIndex, ArcBytes<'static>>,
        >,
        changes: &mut EntryChanges<EmbeddedIndex>,
        writer: &mut PagedWriter<'_, '_>,
        max_order: Option<usize>,
    ) -> Result<Vec<ModificationResult<VersionedByIdIndex<EmbeddedIndex, ArcBytes<'static>>>>, Error>
    {
        modification.prepare()?;

        let total_id_records =
            self.by_id_root.stats(self.reducer()).total_keys() + modification.keys.len() as u64;
        let by_id_order = dynamic_order(total_id_records, max_order);

        let by_id_minimum_children = by_id_order / 2 - 1;
        let by_id_minimum_children =
            by_id_minimum_children.min(usize::try_from(total_id_records).unwrap_or(usize::MAX));

        let mut results = Vec::with_capacity(modification.keys.len());

        while !modification.keys.is_empty() {
            let reducer = self.reducer.clone();
            match self.by_id_root.modify(
                &mut modification,
                &mut ModificationContext::new(
                    by_id_order,
                    by_id_minimum_children,
                    |key: &ArcBytes<'_>,
                     value: Option<&ArcBytes<'static>>,
                     existing_index: Option<
                        &VersionedByIdIndex<EmbeddedIndex, ArcBytes<'static>>,
                    >,
                     writer: &mut PagedWriter<'_, '_>| {
                        let (position, value_size) = if let Some(value) = value {
                            let new_position = writer.write_chunk_cached(value.clone())?;
                            // write_chunk errors if it can't fit within a u32
                            #[allow(clippy::cast_possible_truncation)]
                            let value_length = value.len() as u32;
                            (Some(new_position), value_length)
                        } else {
                            (None, 0)
                        };
                        let embedded = reducer.0.index(key, value);
                        changes.current_sequence = changes
                            .current_sequence
                            .next_sequence()
                            .expect("sequence rollover prevented");
                        let key = key.to_owned();
                        changes.changes.push(EntryChange {
                            key_sequence: KeySequence {
                                key: key.clone(),
                                sequence: changes.current_sequence,
                                last_sequence: existing_index.map(|idx| idx.sequence_id),
                                embedded: Some(embedded.clone()),
                            },
                            value_position: position,
                            value_size,
                        });
                        let new_index = VersionedByIdIndex::new(
                            changes.current_sequence,
                            value_size,
                            position,
                            embedded,
                        );
                        results.push(ModificationResult {
                            key,
                            index: Some(new_index.clone()),
                        });
                        Ok(KeyOperation::Set(new_index))
                    },
                    |index, writer| {
                        if let Some(position) = index.position {
                            match writer.read_chunk(position) {
                                Ok(CacheEntry::ArcBytes(buffer)) => Ok(Some(buffer)),
                                Ok(CacheEntry::Decoded(_)) => unreachable!(),
                                Err(err) => Err(err),
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

        self.sequence = changes.current_sequence;

        Ok(results)
    }
}

impl<EmbeddedIndex> Root for VersionedTreeRoot<EmbeddedIndex>
where
    EmbeddedIndex: super::EmbeddedIndex<ArcBytes<'static>> + Clone + Debug + 'static,
{
    const HEADER: PageHeader = PageHeader::VersionedHeader;
    type Value = ArcBytes<'static>;
    type Index = VersionedByIdIndex<EmbeddedIndex, Self::Value>;
    type ReducedIndex = ByIdStats<EmbeddedIndex::Reduced>;
    type Reducer = ByIdIndexer<EmbeddedIndex::Indexer>;

    fn default_with(reducer: Self::Reducer) -> Self {
        Self {
            transaction_id: TransactionId(0),
            sequence: SequenceId(0),
            by_sequence_root: BTreeEntry::default(),
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
        self.by_id_root.dirty || self.by_sequence_root.dirty
    }

    fn initialized(&self) -> bool {
        self.sequence.valid()
    }

    fn initialize_default(&mut self) {
        self.sequence = SequenceId(1);
    }

    fn serialize(
        &mut self,
        paged_writer: &mut PagedWriter<'_, '_>,
        output: &mut Vec<u8>,
    ) -> Result<(), Error> {
        output.reserve(4096);
        output.write_u64::<BigEndian>(self.transaction_id.0)?;
        output.write_u64::<BigEndian>(self.sequence.0)?;
        // Reserve space for by_sequence and by_id sizes (2xu32).
        output.write_u64::<BigEndian>(0)?;

        let by_sequence_size = self.by_sequence_root.serialize_to(output, paged_writer)?;

        let by_id_size = self.by_id_root.serialize_to(output, paged_writer)?;

        let by_sequence_size = u32::try_from(by_sequence_size)
            .ok()
            .ok_or(ErrorKind::Internal(InternalError::HeaderTooLarge))?;
        BigEndian::write_u32(&mut output[16..20], by_sequence_size);
        let by_id_size = u32::try_from(by_id_size)
            .ok()
            .ok_or(ErrorKind::Internal(InternalError::HeaderTooLarge))?;
        BigEndian::write_u32(&mut output[20..24], by_id_size);

        Ok(())
    }

    fn deserialize(mut bytes: ArcBytes<'_>, reducer: Self::Reducer) -> Result<Self, Error> {
        let transaction_id = TransactionId(bytes.read_u64::<BigEndian>()?);
        let sequence = SequenceId(bytes.read_u64::<BigEndian>()?);
        let by_sequence_size = bytes.read_u32::<BigEndian>()? as usize;
        let by_id_size = bytes.read_u32::<BigEndian>()? as usize;
        if by_sequence_size + by_id_size != bytes.len() {
            return Err(Error::data_integrity(format!(
                "Header reported index sizes {} and {}, but data has {} remaining",
                by_sequence_size,
                by_id_size,
                bytes.len()
            )));
        }

        let mut by_sequence_bytes = bytes.read_bytes(by_sequence_size)?.to_owned();
        let mut by_id_bytes = bytes.read_bytes(by_id_size)?.to_owned();

        let by_sequence_root = BTreeEntry::deserialize_from(&mut by_sequence_bytes, None)?;
        let by_id_root = BTreeEntry::deserialize_from(&mut by_id_bytes, None)?;

        Ok(Self {
            transaction_id,
            sequence,
            by_sequence_root,
            by_id_root,
            reducer,
        })
    }

    fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    fn modify(
        &mut self,
        modification: Modification<'_, ArcBytes<'static>, Self::Index>,
        writer: &mut PagedWriter<'_, '_>,
        max_order: Option<usize>,
    ) -> Result<Vec<ModificationResult<Self::Index>>, Error> {
        let persistence_mode = modification.persistence_mode;

        // Insert into both trees
        let mut changes = EntryChanges {
            current_sequence: self.sequence,
            changes: Vec::with_capacity(modification.keys.len()),
        };
        let results = self.modify_id_root(modification, &mut changes, writer, max_order)?;

        // Convert the changes into a modification request for the id root.
        let mut values = Vec::with_capacity(changes.changes.len());
        let keys = changes
            .changes
            .into_iter()
            .map(|change| {
                values.push(BySequenceIndex {
                    key: change.key_sequence.key,
                    last_sequence: change.key_sequence.last_sequence,
                    value_length: change.value_size,
                    position: change.value_position,
                    embedded: change.key_sequence.embedded,
                });
                ArcBytes::from(change.key_sequence.sequence.0.to_be_bytes())
            })
            .collect();
        let sequence_modifications = Modification {
            persistence_mode,
            keys,
            operation: Operation::SetEach(values),
        };

        self.modify_sequence_root(sequence_modifications, writer, max_order)?;

        // Only update the transaction id if a new one was specified.
        if let Some(transaction_id) = persistence_mode.transaction_id() {
            self.transaction_id = transaction_id;
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
        ScanDataCallback,
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
            ScanDataCallback,
        >,
        file: &mut dyn BlobStorage,
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
        // Copy all of the data using the ID root.
        let mut sequence_indexes = Vec::with_capacity(
            usize::try_from(self.by_id_root.stats(self.reducer()).alive_keys).unwrap_or(usize::MAX),
        );
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
            &mut |key,
                  index: &mut VersionedByIdIndex<EmbeddedIndex, ArcBytes<'static>>,
                  from_file,
                  copied_chunks,
                  to_file,
                  vault| {
                let new_position = if let Some(position) = index.position {
                    Some(to_file.copy_chunk_from(position, from_file, copied_chunks, vault)?)
                } else {
                    None
                };

                sequence_indexes.push((
                    key.clone(),
                    BySequenceIndex {
                        key: key.clone(),
                        last_sequence: None,
                        value_length: index.value_length,
                        position: new_position,
                        embedded: Some(index.embedded.clone()),
                    },
                ));

                index.position = new_position;
                Ok(true)
            },
        )?;

        // Replace our by_sequence index with a new truncated one.
        self.by_sequence_root = BTreeEntry::default();

        sequence_indexes.sort_by(|a, b| a.0.cmp(&b.0));
        let by_sequence_order = dynamic_order(sequence_indexes.len() as u64, None);
        let mut keys = Vec::with_capacity(sequence_indexes.len());
        let mut indexes = Vec::with_capacity(sequence_indexes.len());
        for (id, index) in sequence_indexes {
            keys.push(id);
            indexes.push(index);
        }

        let mut modification = Modification {
            persistence_mode: PersistenceMode::Transactional(self.transaction_id),
            keys,
            operation: Operation::SetEach(indexes),
        };

        let minimum_children = by_sequence_order / 2 - 1;
        let minimum_children = minimum_children.min(modification.keys.len());

        // This modification copies the `sequence_indexes` into the sequence root.
        self.by_sequence_root.modify(
            &mut modification,
            &mut ModificationContext::new(
                by_sequence_order,
                minimum_children,
                |_key: &ArcBytes<'_>,
                               value: Option<&BySequenceIndex<EmbeddedIndex>>,
                               _existing_index: Option<&BySequenceIndex<EmbeddedIndex>>,
                               _writer: &mut PagedWriter<'_, '_>| {
                    Ok(KeyOperation::Set(value.unwrap().clone()))
                },
                |_index: &BySequenceIndex<EmbeddedIndex>, _writer: &mut PagedWriter<'_, '_>| unreachable!(),
                BySequenceReducer,
            ),
            None,
            writer,
        )?;

        Ok(())
    }
}

pub struct EntryChanges<Embedded> {
    pub current_sequence: SequenceId,
    pub changes: Vec<EntryChange<Embedded>>,
}

impl<Embedded> Default for EntryChanges<Embedded> {
    fn default() -> Self {
        Self {
            current_sequence: SequenceId::default(),
            changes: Vec::default(),
        }
    }
}

pub struct EntryChange<Embedded> {
    pub key_sequence: KeySequence<Embedded>,
    pub value_position: Option<GrainId>,
    pub value_size: u32,
}

/// A stored revision of a key.
#[derive(Debug)]
pub struct KeySequence<Embedded> {
    /// The key that this entry was written for.
    pub key: ArcBytes<'static>,
    /// The unique sequence id.
    pub sequence: SequenceId,
    /// The previous sequence id for this key, if any.
    pub last_sequence: Option<SequenceId>,
    /// The embedded index stored for this sequence.
    pub embedded: Option<Embedded>,
}

impl<'a> TryFrom<&'a [u8]> for SequenceId {
    type Error = TryFromSliceError;

    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        value.try_into().map(u64::from_be_bytes).map(Self)
    }
}

/// A stored entry in a versioned tree.
#[derive(Debug)]
pub struct SequenceEntry<Embedded> {
    /// The stored index for this sequence id.
    pub index: BySequenceIndex<Embedded>,
    /// The value stored for this sequence id, if still present.
    pub value: Option<ArcBytes<'static>>,
}

/// A stored index in a versioned tree.
#[derive(Debug)]
pub struct SequenceIndex<Embedded> {
    /// The unique sequence id.
    pub sequence: SequenceId,
    /// The stored index for this sequence id.
    pub index: BySequenceIndex<Embedded>,
}
