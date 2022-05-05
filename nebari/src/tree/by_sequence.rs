use std::fmt::Display;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::{
    error::Error,
    tree::{btree_entry::Reducer, key_entry::ValueIndex, BinarySerialization, PagedWriter},
    ArcBytes, ErrorKind,
};

/// A unique ID of a single modification to a key in a versioned tree file.
#[derive(Default, Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct SequenceId(pub u64);

impl From<u64> for SequenceId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl From<SequenceId> for u64 {
    fn from(id: SequenceId) -> Self {
        id.0
    }
}

impl SequenceId {
    #[must_use]
    pub(crate) const fn valid(self) -> bool {
        self.0 > 0
    }

    /// Returns the nexxt sequence id after `self`.
    pub fn next_sequence(&self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

impl Display for SequenceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// The index stored within [`VersionedTreeRoot::by_sequence_root`](crate::tree::VersionedTreeRoot::by_sequence_root).
#[derive(Clone, Debug)]
pub struct BySequenceIndex {
    /// The key associated with this sequence id.
    pub key: ArcBytes<'static>,
    /// The previous sequence of this key.
    pub last_sequence: Option<SequenceId>,
    /// The size of the value stored on disk.
    pub value_length: u32,
    /// The position of the value on disk.
    pub position: u64,
}

impl BinarySerialization for BySequenceIndex {
    fn serialize_to(
        &mut self,
        writer: &mut Vec<u8>,
        _paged_writer: &mut PagedWriter<'_>,
    ) -> Result<usize, Error> {
        let mut bytes_written = 0;
        writer.write_u32::<BigEndian>(self.value_length)?;
        bytes_written += 4;
        writer.write_u64::<BigEndian>(self.position)?;
        bytes_written += 8;
        writer.write_u64::<BigEndian>(self.last_sequence.unwrap_or(SequenceId(0)).0)?;
        bytes_written += 8;

        let key_length = u16::try_from(self.key.len()).map_err(|_| ErrorKind::KeyTooLarge)?;
        writer.write_u16::<BigEndian>(key_length)?;
        bytes_written += 2;
        writer.extend_from_slice(&self.key);
        bytes_written += key_length as usize;
        Ok(bytes_written)
    }

    fn deserialize_from(
        reader: &mut ArcBytes<'_>,
        _current_order: Option<usize>,
    ) -> Result<Self, Error> {
        let value_length = reader.read_u32::<BigEndian>()?;
        let position = reader.read_u64::<BigEndian>()?;
        let last_sequence = SequenceId(reader.read_u64::<BigEndian>()?);
        let key_length = reader.read_u16::<BigEndian>()? as usize;
        if key_length > reader.len() {
            return Err(Error::data_integrity(format!(
                "key length {} found but only {} bytes remaining",
                key_length,
                reader.len()
            )));
        }
        let key = reader.read_bytes(key_length)?.into_owned();

        Ok(Self {
            key,
            last_sequence: if last_sequence.valid() {
                Some(last_sequence)
            } else {
                None
            },
            value_length,
            position,
        })
    }
}

impl ValueIndex for BySequenceIndex {
    fn position(&self) -> u64 {
        self.position
    }
}

/// The reduced index of [`BySequenceIndex`].
#[derive(Clone, Debug)]
pub struct BySequenceStats {
    /// The total number of sequence entries.
    pub total_sequences: u64,
}

impl BinarySerialization for BySequenceStats {
    fn serialize_to(
        &mut self,
        writer: &mut Vec<u8>,
        _paged_writer: &mut PagedWriter<'_>,
    ) -> Result<usize, Error> {
        writer.write_u64::<BigEndian>(self.total_sequences)?;
        Ok(8)
    }

    fn deserialize_from(
        reader: &mut ArcBytes<'_>,
        _current_order: Option<usize>,
    ) -> Result<Self, Error> {
        let number_of_records = reader.read_u64::<BigEndian>()?;
        Ok(Self {
            total_sequences: number_of_records,
        })
    }
}

#[derive(Clone, Default, Debug)]
pub struct BySequenceReducer;

impl Reducer<BySequenceIndex, BySequenceStats> for BySequenceReducer {
    fn reduce<'a, Indexes, IndexesIter>(&self, indexes: Indexes) -> BySequenceStats
    where
        BySequenceIndex: 'a,
        Indexes:
            IntoIterator<Item = &'a BySequenceIndex, IntoIter = IndexesIter> + ExactSizeIterator,
        IndexesIter: Iterator<Item = &'a BySequenceIndex> + ExactSizeIterator + Clone,
    {
        BySequenceStats {
            total_sequences: indexes.len() as u64,
        }
    }

    fn rereduce<'a, ReducedIndexes, ReducedIndexesIter>(
        &self,
        values: ReducedIndexes,
    ) -> BySequenceStats
    where
        Self: 'a,
        ReducedIndexes: IntoIterator<Item = &'a BySequenceStats, IntoIter = ReducedIndexesIter>
            + ExactSizeIterator,
        ReducedIndexesIter: Iterator<Item = &'a BySequenceStats> + ExactSizeIterator,
    {
        BySequenceStats {
            total_sequences: values.into_iter().map(|v| v.total_sequences).sum(),
        }
    }
}
