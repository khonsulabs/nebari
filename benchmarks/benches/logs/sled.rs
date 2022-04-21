use std::{
    io::ErrorKind,
    time::{Duration, Instant},
};

use _sled::{transaction::ConflictableTransactionError, Db, IVec};
use tempfile::TempDir;

use super::{InsertConfig, LogEntry, LogEntryBatchGenerator, ReadConfig, ReadState};
use crate::{
    logs::{ScanConfig, ScanState},
    BenchConfig, SimpleBench,
};

pub struct InsertLogs {
    _tempfile: TempDir,
    db: Db,
    state: LogEntryBatchGenerator,
}

impl SimpleBench for InsertLogs {
    type GroupState = ();
    type Config = InsertConfig;
    const BACKEND: &'static str = "sled";

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
        let tempfile = TempDir::new()?;
        let db = _sled::open(tempfile.path())?;

        Ok(Self {
            _tempfile: tempfile,
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
            self.db.transaction(|db| {
                for entry in &batch {
                    db.insert(
                        &entry.id.to_be_bytes(),
                        pot::to_vec(&entry).map_err(ConflictableTransactionError::Abort)?,
                    )?;
                }
                db.flush();
                Ok(())
            })?;
            total_duration += Instant::now() - start;
        }
        Ok(total_duration)
    }
}

pub struct ReadLogs {
    db: Db,
    state: ReadState,
}

impl SimpleBench for ReadLogs {
    type GroupState = TempDir;
    type Config = ReadConfig;
    const BACKEND: &'static str = "sled";

    fn initialize_group(
        config: &Self::Config,
        _group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Self::GroupState {
        let tempfile = TempDir::new().unwrap();
        let db = _sled::Config::default()
            .cache_capacity(2_000 * 160_384)
            .path(tempfile.path())
            .open()
            .unwrap();

        config.for_each_database_chunk(1_000_000, |chunk| {
            for entry in chunk {
                db.insert(&entry.id.to_be_bytes(), pot::to_vec(&entry).unwrap())
                    .unwrap();
            }
        });
        db.flush().unwrap();
        tempfile
    }

    fn initialize(
        group_state: &Self::GroupState,
        config: &Self::Config,
        config_group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Result<Self, anyhow::Error> {
        let db = loop {
            match _sled::Config::default()
                .cache_capacity(2_000 * 160_384)
                .path(group_state.path())
                .open()
            {
                Ok(db) => break db,
                Err(_sled::Error::Io(err)) => {
                    if err.kind() == ErrorKind::WouldBlock {
                        // Sled occasionally returns a blocking error, but the only way this could happen is if background threads haven't cleaned up fully.
                        eprintln!("Sled returned a would block error.");
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }
                Err(other) => return Err(anyhow::anyhow!(other)),
            }
        };
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
                let bytes = self
                    .db
                    .get(&entry.id.to_be_bytes())?
                    .expect("value not found");
                let decoded = pot::from_slice::<LogEntry>(&bytes)?;
                assert_eq!(&decoded, &entry);
                total_duration += Instant::now() - start;
            } else {
                let entries = (0..config.get_count)
                    .filter_map(|_| self.state.next())
                    .collect::<Vec<_>>();

                let start = Instant::now();
                for _ in entries {
                    let entry = self.state.next().unwrap();
                    self.db
                        .get(&entry.id.to_be_bytes())?
                        .expect("value not found");
                }
                total_duration += Instant::now() - start;
            }
        }
        Ok(total_duration)
    }
}

pub struct ScanLogs {
    db: Db,
    state: ScanState,
}

impl SimpleBench for ScanLogs {
    type GroupState = TempDir;
    type Config = ScanConfig;
    const BACKEND: &'static str = "sled";

    fn initialize_group(
        config: &Self::Config,
        _group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Self::GroupState {
        let tempfile = TempDir::new().unwrap();
        let db = _sled::Config::default()
            .cache_capacity(2_000 * 160_384)
            .path(tempfile.path())
            .open()
            .unwrap();

        config.for_each_database_chunk(1_000_000, |chunk| {
            for entry in chunk {
                db.insert(&entry.id.to_be_bytes(), pot::to_vec(&entry).unwrap())
                    .unwrap();
            }
        });
        db.flush().unwrap();
        tempfile
    }

    fn initialize(
        group_state: &Self::GroupState,
        config: &Self::Config,
        config_group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Result<Self, anyhow::Error> {
        let db = loop {
            match _sled::Config::default()
                .cache_capacity(2_000 * 160_384)
                .path(group_state.path())
                .open()
            {
                Ok(db) => break db,
                Err(_sled::Error::Io(err)) => {
                    if err.kind() == ErrorKind::WouldBlock {
                        // Sled occasionally returns a blocking error, but the only way this could happen is if background threads haven't cleaned up fully.
                        eprintln!("Sled returned a would block error.");
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }
                Err(other) => return Err(anyhow::anyhow!(other)),
            }
        };
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
            let range = IVec::from(range.start().to_be_bytes().to_vec())
                ..=IVec::from(range.end().to_be_bytes().to_vec());
            let entries = self
                .db
                .range(range)
                .collect::<Result<Vec<(IVec, IVec)>, _sled::Error>>()?;
            assert_eq!(entries.len(), config.element_count);
            total_duration += Instant::now() - start;
        }
        Ok(total_duration)
    }
}
