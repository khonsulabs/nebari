use super::PagedWriter;
use crate::{error::Error, ArcBytes};

pub trait BinarySerialization: Send + Sync + Sized {
    fn serialize_to(
        &mut self,
        writer: &mut Vec<u8>,
        paged_writer: &mut PagedWriter<'_>,
    ) -> Result<usize, Error>;
    fn deserialize_from(
        reader: &mut ArcBytes<'_>,
        current_order: Option<usize>,
    ) -> Result<Self, Error>;
}

impl BinarySerialization for () {
    fn serialize_to(
        &mut self,
        _writer: &mut Vec<u8>,
        _paged_writer: &mut PagedWriter<'_>,
    ) -> Result<usize, Error> {
        Ok(0)
    }

    fn deserialize_from(
        _reader: &mut ArcBytes<'_>,
        _current_order: Option<usize>,
    ) -> Result<Self, Error> {
        Ok(())
    }
}
