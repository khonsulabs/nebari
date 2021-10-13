use std::time::{Duration, Instant};

use persy::{ByteVec, Config, Persy, ValueMode};

use super::InsertConfig;
use crate::{blobs::BlobGenerator, BenchConfig, SimpleBench};

pub struct InsertBlobs {
    // _tempfile: NamedTempFile,
    db: persy::Persy,
    blob: BlobGenerator,
}

impl SimpleBench for InsertBlobs {
    type GroupState = ();
    type Config = InsertConfig;
    const BACKEND: &'static str = "persy";

    fn initialize_group(
        _config: &Self::Config,
        _group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Self::GroupState {
    }

    fn initialize(
        _group_state: &Self::GroupState,
        config: &Self::Config,
        config_group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Result<Self, anyhow::Error> {
        let _ = std::fs::remove_file("/tmp/persy");
        let db = Persy::open_or_create_with("/tmp/persy", Config::default(), |persy| {
            let mut tx = persy.begin()?;
            tx.create_index::<u64, ByteVec>("index", ValueMode::Replace)?;
            let prepared = tx.prepare()?;
            prepared.commit()?;
            Ok(())
        })
        .unwrap();

        Ok(Self {
            // _tempfile: tempfile,
            db,
            blob: config.initialize(config_group_state),
        })
    }

    fn execute_measured(
        &mut self,
        _config: &Self::Config,
        iters: u64,
    ) -> Result<Duration, anyhow::Error> {
        let mut total_duration = Duration::default();
        for _ in 0..iters {
            let blob = self.blob.next().unwrap();
            let start = Instant::now();
            let mut tx = self.db.begin()?;

            tx.put("index", blob.0, ByteVec::new(blob.1.to_vec()))
                .unwrap();

            let prepared = tx.prepare().unwrap();
            prepared.commit().unwrap();
            total_duration += Instant::now() - start;
        }
        Ok(total_duration)
    }
}
