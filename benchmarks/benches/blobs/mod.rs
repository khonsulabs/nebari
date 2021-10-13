use std::fmt::Display;

use ::nebari::Buffer;
use criterion::{Criterion, Throughput};
use nanorand::{Pcg64, Rng};
use ubyte::ToByteUnit;

use crate::{BenchConfig, SimpleBench, UnversionedBenchmark, VersionedBenchmark};
mod nebari;
#[cfg(feature = "persy")]
mod persy;
#[cfg(feature = "sled")]
mod sled;
#[cfg(feature = "sqlite")]
mod sqlite;

pub fn benches(c: &mut Criterion) {
    inserts(c);
}

pub fn inserts(c: &mut Criterion) {
    let mut group = c.benchmark_group("blobs-insert");

    for blob_size in [1024, 1024 * 1024, 1024 * 1024 * 64] {
        let config = InsertConfig { blob_size };

        nebari::InsertBlobs::<VersionedBenchmark>::run(&mut group, &config);
        nebari::InsertBlobs::<UnversionedBenchmark>::run(&mut group, &config);
        #[cfg(feature = "sled")]
        sled::InsertBlobs::run(&mut group, &config);
        #[cfg(feature = "sqlite")]
        sqlite::InsertBlobs::run(&mut group, &config);
        #[cfg(feature = "persy")]
        persy::InsertBlobs::run(&mut group, &config);
        // #[cfg(feature = "couchdb")]
        // couchdb::InsertLogs::run(&mut group, &config);
    }
}

#[derive(Clone)]
pub struct InsertConfig {
    pub blob_size: usize,
}

#[derive(Clone)]
pub struct Blob(u64, Buffer<'static>);

pub struct BlobGenerator {
    last_id: u64,
    rng: Pcg64,
    bytes: Vec<u8>,
}

impl Iterator for BlobGenerator {
    type Item = Blob;

    fn next(&mut self) -> Option<Self::Item> {
        self.last_id += 1;
        // // sqlite has some impressive block-level deduplication. This approach
        // // to "randomizing" data tricks sqlite to not be able to figure out that
        // // there's any repeat data. Simpler approaches failed to trick sqlite
        // // into writing the full blobs.
        // let number_of_mutations = self.bytes.len() / 16;
        // let offset = self.rng.generate_range(0..16);
        // for i in 0..number_of_mutations {
        //     self.bytes[16 * i + offset] = self.rng.generate();
        // }
        for i in 0..self.bytes.len() {
            self.bytes[i] = self.rng.generate();
        }
        Some(Blob(self.last_id, Buffer::from(self.bytes.clone())))
    }
}

impl BenchConfig for InsertConfig {
    type GroupState = ();
    type State = BlobGenerator;
    type Batch = Blob;

    fn initialize_group(&self) -> Self::GroupState {}

    fn initialize(&self, _group_state: &Self::GroupState) -> Self::State {
        let mut bytes = Vec::with_capacity(self.blob_size);
        let mut rng = Pcg64::new_seed(1);
        for _ in 0..self.blob_size {
            bytes.push(rng.generate());
        }
        BlobGenerator {
            last_id: 0,
            rng,
            bytes,
        }
    }

    fn throughput(&self) -> Throughput {
        Throughput::Bytes(self.blob_size as u64)
    }
}

impl Display for InsertConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.blob_size.bytes())
    }
}
