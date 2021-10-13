use std::time::{Duration, Instant};

use persy::{ByteVec, Config, Persy, ValueMode};

use super::{InsertConfig, LogEntry, LogEntryBatchGenerator, ReadConfig, ReadState};
use crate::{
    logs::{ScanConfig, ScanState},
    BenchConfig, SimpleBench,
};

pub struct InsertLogs {
    // _tempfile: NamedTempFile,
    db: persy::Persy,
    state: LogEntryBatchGenerator,
}

impl SimpleBench for InsertLogs {
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
            tx.create_index::<i64, ByteVec>("index", ValueMode::Replace)?;
            let prepared = tx.prepare()?;
            prepared.commit()?;
            Ok(())
        })
        .unwrap();

        Ok(Self {
            // _tempfile: tempfile,
            db,
            state: config.initialize(config_group_state),
        })
    }

    fn execute_measured(
        &mut self,
        _config: &Self::Config,
        iters: u64,
    ) -> Result<Duration, anyhow::Error> {
        let mut total_duration = Duration::default();
        for _ in 0..iters {
            let batch = self.state.next().unwrap();
            let start = Instant::now();
            let mut tx = self.db.begin()?;

            for entry in &batch {
                tx.put("index", entry.id, ByteVec::new(pot::to_vec(&entry)?))
                    .unwrap();
            }
            let prepared = tx.prepare().unwrap();
            prepared.commit().unwrap();
            total_duration += Instant::now() - start;
        }
        Ok(total_duration)
    }
}

pub struct ReadLogs {
    db: Persy,
    state: ReadState,
}

impl SimpleBench for ReadLogs {
    type GroupState = ();
    type Config = ReadConfig;
    const BACKEND: &'static str = "persy";

    fn initialize_group(
        config: &Self::Config,
        _group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Self::GroupState {
        let _ = std::fs::remove_file("/tmp/persy");
        let db = Persy::open_or_create_with("/tmp/persy", Config::default(), |persy| {
            let mut tx = persy.begin()?;
            tx.create_index::<i64, ByteVec>("index", ValueMode::Replace)?;
            let prepared = tx.prepare()?;
            prepared.commit()?;
            Ok(())
        })
        .unwrap();

        config.for_each_database_chunk(100_000, |chunk| {
            let mut tx = db.begin().unwrap();
            for entry in chunk {
                tx.put(
                    "index",
                    entry.id,
                    ByteVec::new(pot::to_vec(&entry).unwrap()),
                )
                .unwrap();
            }
            let prepared = tx.prepare().unwrap();
            prepared.commit().unwrap();
        });
    }

    fn initialize(
        _group_state: &Self::GroupState,
        config: &Self::Config,
        config_group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Result<Self, anyhow::Error> {
        let db = Persy::open("/tmp/persy", Config::default()).unwrap();
        let state = config.initialize(config_group_state);
        Ok(Self { db, state })
    }

    fn execute_measured(
        &mut self,
        config: &Self::Config,
        iters: u64,
    ) -> Result<Duration, anyhow::Error> {
        // To be fair, we're only evaluating that content equals when it's a single get
        let mut total_duration = Duration::default();
        for _ in 0..iters {
            if config.get_count == 1 {
                let entry = self.state.next().unwrap();
                let start = Instant::now();
                let bytes: ByteVec = self
                    .db
                    .get("index", &entry.id)?
                    .next()
                    .expect("value not found");
                let decoded = pot::from_slice::<LogEntry>(&bytes)?;
                assert_eq!(&decoded, &entry);
                total_duration += Instant::now() - start;
            } else {
                for _ in 0..config.get_count {
                    let entry = self.state.next().unwrap();
                    let start = Instant::now();
                    self.db
                        .get::<_, ByteVec>("index", &entry.id)?
                        .next()
                        .expect("value not found");
                    total_duration += Instant::now() - start;
                }
            }
        }
        Ok(total_duration)
    }
}

pub struct ScanLogs {
    db: Persy,
    state: ScanState,
}

impl SimpleBench for ScanLogs {
    type GroupState = ();
    type Config = ScanConfig;
    const BACKEND: &'static str = "persy";

    fn initialize_group(
        config: &Self::Config,
        _group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Self::GroupState {
        let _ = std::fs::remove_file("/tmp/persy");
        let db = Persy::open_or_create_with("/tmp/persy", Config::default(), |persy| {
            let mut tx = persy.begin()?;
            tx.create_index::<i64, ByteVec>("index", ValueMode::Replace)?;
            let prepared = tx.prepare()?;
            prepared.commit()?;
            Ok(())
        })
        .unwrap();

        config.for_each_database_chunk(100_000, |chunk| {
            let mut tx = db.begin().unwrap();
            for entry in chunk {
                tx.put(
                    "index",
                    entry.id,
                    ByteVec::new(pot::to_vec(&entry).unwrap()),
                )
                .unwrap();
            }
            let prepared = tx.prepare().unwrap();
            prepared.commit().unwrap();
        });
    }

    fn initialize(
        _group_state: &Self::GroupState,
        config: &Self::Config,
        config_group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Result<Self, anyhow::Error> {
        let db = Persy::open("/tmp/persy", Config::default()).unwrap();
        let state = config.initialize(config_group_state);
        Ok(Self { db, state })
    }

    fn execute_measured(
        &mut self,
        config: &Self::Config,
        iters: u64,
    ) -> Result<Duration, anyhow::Error> {
        let mut total_duration = Duration::default();
        for _ in 0..iters {
            let range = self.state.next().unwrap();
            let start = Instant::now();
            let results = self.db.range::<i64, ByteVec, _>("index", range)?;
            assert_eq!(results.count(), config.element_count);
            total_duration += Instant::now() - start;
        }
        Ok(total_duration)
    }
}
