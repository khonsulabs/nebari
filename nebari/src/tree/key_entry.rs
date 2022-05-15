use std::collections::HashMap;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use super::{serialization::BinarySerialization, PagedWriter};
use crate::{error::Error, io::File, vault::AnyVault, ArcBytes, ErrorKind};

/// An entry for a key. Stores a single index value for a single key.
#[derive(Debug, Clone)]
pub struct KeyEntry<Index> {
    /// The key of this entry.
    pub key: ArcBytes<'static>,
    /// The index value of this entry.
    pub index: Index,
}

/// An index that serializes a value to the file.
pub trait PositionIndex {
    /// The position on-disk of the stored value.
    fn position(&self) -> u64;
}

impl<Index: PositionIndex + BinarySerialization> KeyEntry<Index> {
    pub(crate) fn copy_data_to<Callback>(
        &mut self,
        file: &mut dyn File,
        copied_chunks: &mut HashMap<u64, u64>,
        writer: &mut PagedWriter<'_>,
        vault: Option<&dyn AnyVault>,
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
        index_callback(
            &self.key,
            &mut self.index,
            file,
            copied_chunks,
            writer,
            vault,
        )
    }
}

impl<Index: BinarySerialization> BinarySerialization for KeyEntry<Index> {
    fn serialize_to(
        &mut self,
        writer: &mut Vec<u8>,
        paged_writer: &mut PagedWriter<'_>,
    ) -> Result<usize, Error> {
        let mut bytes_written = 0;
        // Write the key
        let key_len = u16::try_from(self.key.len()).map_err(|_| ErrorKind::KeyTooLarge)?;
        writer.write_u16::<BigEndian>(key_len)?;
        writer.extend_from_slice(&self.key);
        bytes_written += 2 + key_len as usize;

        // Write the value
        bytes_written += self.index.serialize_to(writer, paged_writer)?;
        Ok(bytes_written)
    }

    fn deserialize_from(
        reader: &mut ArcBytes<'_>,
        current_order: Option<usize>,
    ) -> Result<Self, Error> {
        let key_len = reader.read_u16::<BigEndian>()? as usize;
        if key_len > reader.len() {
            return Err(Error::data_integrity(format!(
                "key length {} found but only {} bytes remaining",
                key_len,
                reader.len()
            )));
        }
        let key = reader.read_bytes(key_len)?.into_owned();

        let value = Index::deserialize_from(reader, current_order)?;

        Ok(Self { key, index: value })
    }
}
