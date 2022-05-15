use std::marker::PhantomData;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use super::{btree::Reducer, BinarySerialization, PagedWriter};
use crate::{
    error::Error,
    tree::{by_sequence::SequenceId, key_entry::PositionIndex},
    ArcBytes,
};

/// The index stored within [`VersionedTreeRoot::by_id_root`](crate::tree::VersionedTreeRoot::by_id_root).
#[derive(Clone, Debug)]
pub struct VersionedByIdIndex<EmbeddedIndex: super::EmbeddedIndex<Value>, Value> {
    /// The unique sequence id generated when writing the value to the file.
    pub sequence_id: SequenceId,
    /// The size of the value stored on disk.
    pub value_length: u32,
    /// The position of the value on disk.
    pub position: u64,
    /// The embedded index.
    pub embedded: EmbeddedIndex,

    _value: PhantomData<Value>,
}

impl<EmbeddedIndex, Value> VersionedByIdIndex<EmbeddedIndex, Value>
where
    EmbeddedIndex: super::EmbeddedIndex<Value>,
{
    /// Retruns a new index instance.
    pub fn new(
        sequence_id: SequenceId,
        value_length: u32,
        position: u64,
        embedded: EmbeddedIndex,
    ) -> Self {
        Self {
            sequence_id,
            value_length,
            position,
            embedded,
            _value: PhantomData,
        }
    }
}

impl<EmbeddedIndex, Value> BinarySerialization for VersionedByIdIndex<EmbeddedIndex, Value>
where
    EmbeddedIndex: super::EmbeddedIndex<Value>,
    Value: Send + Sync,
{
    fn serialize_to(
        &mut self,
        writer: &mut Vec<u8>,
        _paged_writer: &mut PagedWriter<'_>,
    ) -> Result<usize, Error> {
        writer.write_u64::<BigEndian>(self.sequence_id.0)?;
        writer.write_u32::<BigEndian>(self.value_length)?;
        writer.write_u64::<BigEndian>(self.position)?;
        Ok(20 + self.embedded.serialize_to(writer)?)
    }

    fn deserialize_from(
        reader: &mut ArcBytes<'_>,
        _current_order: Option<usize>,
    ) -> Result<Self, Error> {
        let sequence_id = SequenceId(reader.read_u64::<BigEndian>()?);
        let value_length = reader.read_u32::<BigEndian>()?;
        let position = reader.read_u64::<BigEndian>()?;
        Ok(Self::new(
            sequence_id,
            value_length,
            position,
            EmbeddedIndex::deserialize_from(reader)?,
        ))
    }
}

impl<EmbeddedIndex, Value> PositionIndex for VersionedByIdIndex<EmbeddedIndex, Value>
where
    EmbeddedIndex: super::EmbeddedIndex<Value>,
{
    fn position(&self) -> u64 {
        self.position
    }
}

/// The index stored within [`UnversionedTreeRoot::by_id_root`](crate::tree::UnversionedTreeRoot::by_id_root).
#[derive(Clone, Debug)]
pub struct UnversionedByIdIndex<EmbeddedIndex: super::EmbeddedIndex<Value>, Value> {
    /// The size of the value stored on disk.
    pub value_length: u32,
    /// The position of the value on disk.
    pub position: u64,
    /// The embedded index.
    pub embedded: EmbeddedIndex,

    _value: PhantomData<Value>,
}

impl<EmbeddedIndex, Value> UnversionedByIdIndex<EmbeddedIndex, Value>
where
    EmbeddedIndex: super::EmbeddedIndex<Value>,
{
    /// Retruns a new index instance.
    pub fn new(value_length: u32, position: u64, embedded: EmbeddedIndex) -> Self {
        Self {
            value_length,
            position,
            embedded,
            _value: PhantomData,
        }
    }
}

impl<EmbeddedIndex, Value> BinarySerialization for UnversionedByIdIndex<EmbeddedIndex, Value>
where
    EmbeddedIndex: super::EmbeddedIndex<Value>,
    Value: Send + Sync,
{
    fn serialize_to(
        &mut self,
        writer: &mut Vec<u8>,
        _paged_writer: &mut PagedWriter<'_>,
    ) -> Result<usize, Error> {
        writer.write_u32::<BigEndian>(self.value_length)?;
        writer.write_u64::<BigEndian>(self.position)?;
        Ok(12 + self.embedded.serialize_to(writer)?)
    }

    fn deserialize_from(
        reader: &mut ArcBytes<'_>,
        _current_order: Option<usize>,
    ) -> Result<Self, Error> {
        let value_length = reader.read_u32::<BigEndian>()?;
        let position = reader.read_u64::<BigEndian>()?;
        Ok(Self::new(
            value_length,
            position,
            EmbeddedIndex::deserialize_from(reader)?,
        ))
    }
}

impl<EmbeddedIndex, Value> PositionIndex for UnversionedByIdIndex<EmbeddedIndex, Value>
where
    EmbeddedIndex: super::EmbeddedIndex<Value>,
{
    fn position(&self) -> u64 {
        self.position
    }
}

/// The reduced index of both [`VersionedByIdIndex`] and [`UnversionedByIdIndex`]
#[derive(Clone, Debug)]
pub struct ByIdStats<EmbeddedStats> {
    /// The number of keys that have values stored within them.
    pub alive_keys: u64,
    /// The number of keys that no longer have values stored within them.
    pub deleted_keys: u64,
    /// The total number of bytes stored on disk associated with currently-alive values.
    pub total_indexed_bytes: u64,
    /// The embedded statistics.
    pub embedded: EmbeddedStats,
}

impl<EmbeddedStats> ByIdStats<EmbeddedStats> {
    /// Returns the total number of keys regardless of whether data is stored within them.
    #[must_use]
    pub const fn total_keys(&self) -> u64 {
        self.alive_keys + self.deleted_keys
    }
}

impl<EmbeddedStats> BinarySerialization for ByIdStats<EmbeddedStats>
where
    EmbeddedStats: super::Serializable,
{
    fn serialize_to(
        &mut self,
        writer: &mut Vec<u8>,
        _paged_writer: &mut PagedWriter<'_>,
    ) -> Result<usize, Error> {
        writer.write_u64::<BigEndian>(self.alive_keys)?;
        writer.write_u64::<BigEndian>(self.deleted_keys)?;
        writer.write_u64::<BigEndian>(self.total_indexed_bytes)?;
        Ok(24 + self.embedded.serialize_to(writer)?)
    }

    fn deserialize_from(
        reader: &mut ArcBytes<'_>,
        _current_order: Option<usize>,
    ) -> Result<Self, Error> {
        let alive_keys = reader.read_u64::<BigEndian>()?;
        let deleted_keys = reader.read_u64::<BigEndian>()?;
        let total_indexed_bytes = reader.read_u64::<BigEndian>()?;
        Ok(Self {
            alive_keys,
            deleted_keys,
            total_indexed_bytes,
            embedded: EmbeddedStats::deserialize_from(reader)?,
        })
    }
}

/// Indexes and Reduces [`VersionedByIdIndex`] and [`UnversionedByIdIndex`].
/// Contains an [`EmbeddedIndex`][super::EmbeddedIndex].
#[derive(Clone, Default, Debug)]
pub struct ByIdIndexer<EmbeddedIndexer>(pub EmbeddedIndexer);

impl<EmbeddedIndexer, EmbeddedIndex, EmbeddedStats, Value>
    Reducer<VersionedByIdIndex<EmbeddedIndex, Value>, ByIdStats<EmbeddedStats>>
    for ByIdIndexer<EmbeddedIndexer>
where
    EmbeddedIndexer: Reducer<EmbeddedIndex, EmbeddedStats>,
    EmbeddedIndex: super::EmbeddedIndex<Value, Indexer = EmbeddedIndexer, Reduced = EmbeddedStats>,
    Value: Send + Sync + 'static,
{
    fn reduce<'a, Indexes, IndexesIter>(&self, indexes: Indexes) -> ByIdStats<EmbeddedStats>
    where
        EmbeddedIndex: 'a,
        Indexes: IntoIterator<
                Item = &'a VersionedByIdIndex<EmbeddedIndex, Value>,
                IntoIter = IndexesIter,
            > + ExactSizeIterator,
        IndexesIter: Iterator<Item = &'a VersionedByIdIndex<EmbeddedIndex, Value>>
            + ExactSizeIterator
            + Clone,
    {
        self.reduce(indexes)
    }

    fn rereduce<
        'a,
        ReducedIndexes: IntoIterator<Item = &'a ByIdStats<EmbeddedStats>, IntoIter = ReducedIndexesIter>
            + ExactSizeIterator,
        ReducedIndexesIter: Iterator<Item = &'a ByIdStats<EmbeddedStats>> + ExactSizeIterator + Clone,
    >(
        &self,
        values: ReducedIndexes,
    ) -> ByIdStats<EmbeddedStats>
    where
        Self: 'a,
        EmbeddedStats: 'a,
    {
        self.rereduce(values)
    }
}

impl<EmbeddedIndexer, EmbeddedIndex, EmbeddedStats, Value>
    Reducer<UnversionedByIdIndex<EmbeddedIndex, Value>, ByIdStats<EmbeddedStats>>
    for ByIdIndexer<EmbeddedIndexer>
where
    EmbeddedIndexer: Reducer<EmbeddedIndex, EmbeddedStats>,
    EmbeddedIndex: super::EmbeddedIndex<Value, Indexer = EmbeddedIndexer, Reduced = EmbeddedStats>,
    Value: 'static,
{
    fn reduce<'a, Indexes, IndexesIter>(&self, indexes: Indexes) -> ByIdStats<EmbeddedStats>
    where
        EmbeddedIndex: 'a,
        Indexes: IntoIterator<
                Item = &'a UnversionedByIdIndex<EmbeddedIndex, Value>,
                IntoIter = IndexesIter,
            > + ExactSizeIterator,
        IndexesIter: Iterator<Item = &'a UnversionedByIdIndex<EmbeddedIndex, Value>>
            + ExactSizeIterator
            + Clone,
    {
        self.reduce(indexes)
    }

    fn rereduce<'a, ReducedIndexes, ReducedIndexesIter>(
        &self,
        values: ReducedIndexes,
    ) -> ByIdStats<EmbeddedStats>
    where
        Self: 'a,
        EmbeddedStats: 'a,
        ReducedIndexes: IntoIterator<Item = &'a ByIdStats<EmbeddedStats>, IntoIter = ReducedIndexesIter>
            + ExactSizeIterator,
        ReducedIndexesIter:
            Iterator<Item = &'a ByIdStats<EmbeddedStats>> + ExactSizeIterator + Clone,
    {
        self.rereduce(values)
    }
}

impl<EmbeddedIndexer> ByIdIndexer<EmbeddedIndexer> {
    fn reduce<'a, EmbeddedIndex, EmbeddedStats, Id, Indexes, IndexesIter, Value>(
        &self,
        values: Indexes,
    ) -> ByIdStats<EmbeddedStats>
    where
        Id: IdIndex<EmbeddedIndex> + 'a,
        EmbeddedIndex:
            super::EmbeddedIndex<Value, Indexer = EmbeddedIndexer, Reduced = EmbeddedStats> + 'a,
        Indexes: IntoIterator<Item = &'a Id, IntoIter = IndexesIter> + ExactSizeIterator,
        IndexesIter: Iterator<Item = &'a Id> + ExactSizeIterator + Clone,
        EmbeddedIndexer: Reducer<EmbeddedIndex, EmbeddedStats>,
    {
        let values = values.into_iter();
        let (alive_keys, deleted_keys, total_indexed_bytes) = values
            .clone()
            .map(|index| {
                if index.position() > 0 {
                    // Alive key
                    (1, 0, u64::from(index.value_size()))
                } else {
                    // Deleted
                    (0, 1, 0)
                }
            })
            .reduce(
                |(total_alive, total_deleted, total_size), (alive, deleted, size)| {
                    (
                        total_alive + alive,
                        total_deleted + deleted,
                        total_size + size,
                    )
                },
            )
            .unwrap_or_default();
        ByIdStats {
            alive_keys,
            deleted_keys,
            total_indexed_bytes,
            embedded: self.0.reduce(values.map(IdIndex::embedded)),
        }
    }

    fn rereduce<'a, ReducedIndexes, ReducedIndexesIter, EmbeddedStats, EmbeddedIndex>(
        &self,
        values: ReducedIndexes,
    ) -> ByIdStats<EmbeddedStats>
    where
        EmbeddedStats: 'a,
        ReducedIndexes: IntoIterator<Item = &'a ByIdStats<EmbeddedStats>, IntoIter = ReducedIndexesIter>
            + ExactSizeIterator,
        ReducedIndexesIter:
            Iterator<Item = &'a ByIdStats<EmbeddedStats>> + ExactSizeIterator + Clone,
        EmbeddedIndexer: Reducer<EmbeddedIndex, EmbeddedStats>,
    {
        let values = values.into_iter();
        ByIdStats {
            alive_keys: values.clone().map(|v| v.alive_keys).sum(),
            deleted_keys: values.clone().map(|v| v.deleted_keys).sum(),
            total_indexed_bytes: values.clone().map(|v| v.total_indexed_bytes).sum(),
            // TODO change this to an iterator
            embedded: self.0.rereduce(values.map(|v| &v.embedded)),
        }
    }
}

pub trait IdIndex<EmbeddedIndex> {
    fn value_size(&self) -> u32;
    fn position(&self) -> u64;
    fn embedded(&self) -> &EmbeddedIndex;
}

impl<EmbeddedIndex, Value> IdIndex<EmbeddedIndex> for UnversionedByIdIndex<EmbeddedIndex, Value>
where
    EmbeddedIndex: super::EmbeddedIndex<Value>,
{
    fn value_size(&self) -> u32 {
        self.value_length
    }

    fn position(&self) -> u64 {
        self.position
    }

    fn embedded(&self) -> &EmbeddedIndex {
        &self.embedded
    }
}

impl<EmbeddedIndex, Value> IdIndex<EmbeddedIndex> for VersionedByIdIndex<EmbeddedIndex, Value>
where
    EmbeddedIndex: super::EmbeddedIndex<Value>,
{
    fn value_size(&self) -> u32 {
        self.value_length
    }

    fn position(&self) -> u64 {
        self.position
    }

    fn embedded(&self) -> &EmbeddedIndex {
        &self.embedded
    }
}
