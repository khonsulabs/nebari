pub mod sediment;

use std::fmt::Debug;

use ::sediment::{
    format::{BatchId, GrainId},
    io::paths::PathId,
};
use arc_bytes::ArcBytes;

use crate::Error;

pub trait BlobStorage: Debug + Send + Sync {
    fn unique_id(&self) -> PathId;
    fn read_header(&mut self) -> Result<ArcBytes<'static>, Error>;
    fn write_header(&mut self, data: &[u8]) -> Result<(), Error>;
    fn write_header_async(&mut self, data: Vec<u8>) -> Result<(), Error>;

    fn read(&mut self, id: GrainId) -> Result<ArcBytes<'static>, Error>;
    fn free(&mut self, id: GrainId) -> Result<(), Error>;
    fn write(&mut self, data: &[u8]) -> Result<GrainId, Error>;
    fn write_async(&mut self, data: Vec<u8>) -> Result<GrainId, Error>;
    fn sync(&mut self) -> Result<BatchId, Error>;
}
