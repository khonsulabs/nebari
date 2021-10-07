use super::PagedWriter;
use crate::{error::Error, io::ManagedFile, Buffer};

pub trait BinarySerialization: Send + Sync + Sized {
    fn serialize_to<File: ManagedFile>(
        &mut self,
        writer: &mut Vec<u8>,
        paged_writer: &mut PagedWriter<'_, File>,
    ) -> Result<usize, Error>;
    fn deserialize_from(reader: &mut Buffer<'_>, current_order: usize) -> Result<Self, Error>;
}

impl BinarySerialization for () {
    fn serialize_to<File: ManagedFile>(
        &mut self,
        _writer: &mut Vec<u8>,
        _paged_writer: &mut PagedWriter<'_, File>,
    ) -> Result<usize, Error> {
        Ok(0)
    }

    fn deserialize_from(_reader: &mut Buffer<'_>, _current_order: usize) -> Result<Self, Error> {
        Ok(())
    }
}
