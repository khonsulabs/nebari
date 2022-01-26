use std::convert::TryFrom;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use super::{btree_entry::Reducer, BinarySerialization, PagedWriter};
use crate::{error::Error, io::ManagedFile, tree::key_entry::ValueIndex, ArcBytes, ErrorKind};

/// The index stored within [`VersionedTreeRoot::by_sequence_root`](crate::tree::VersionedTreeRoot::by_sequence_root).
#[derive(Clone, Debug)]
pub struct BySequenceIndex {
    /// The key associated with this sequence id.
    pub key: ArcBytes<'static>,
    /// The previous sequence of this key.
    pub last_sequence: Option<u64>,
    /// The size of the value stored on disk.
    pub value_length: u32,
    /// The position of the value on disk.
    pub position: u64,
}

impl BinarySerialization for BySequenceIndex {
    fn serialize_to<File: ManagedFile>(
        &mut self,
        writer: &mut Vec<u8>,
        _paged_writer: &mut PagedWriter<'_, File>,
    ) -> Result<usize, Error> {
        let mut bytes_written = 0;
        writer.write_u32::<BigEndian>(self.value_length)?;
        bytes_written += 4;
        writer.write_u64::<BigEndian>(self.position)?;
        bytes_written += 8;
        writer.write_u64::<BigEndian>(self.last_sequence.unwrap_or(0))?;
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
        let last_sequence = reader.read_u64::<BigEndian>()?;
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
            last_sequence: if last_sequence > 0 {
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
    fn serialize_to<File: ManagedFile>(
        &mut self,
        writer: &mut Vec<u8>,
        _paged_writer: &mut PagedWriter<'_, File>,
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

impl<'a> Reducer<BySequenceIndex> for BySequenceStats {
    fn reduce(values: &[&BySequenceIndex]) -> Self {
        Self {
            total_sequences: values.len() as u64,
        }
    }

    fn rereduce(values: &[&Self]) -> Self {
        Self {
            total_sequences: values.iter().map(|v| v.total_sequences).sum(),
        }
    }
}
