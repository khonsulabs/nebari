use std::{io::ErrorKind, time::Duration};

use _sled::{transaction::ConflictableTransactionError, Db};
use tempfile::TempDir;

use super::{InsertConfig, LogEntry, LogEntryBatchGenerator, ReadConfig, ReadState};
use crate::{BenchConfig, SimpleBench};

pub struct InsertLogs {
    _tempfile: TempDir,
    db: Db,
    state: LogEntryBatchGenerator,
}

impl SimpleBench for InsertLogs {
    type GroupState = ();
    type Config = InsertConfig;
    const BACKEND: &'static str = "Sled";

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

    fn execute_measured(&mut self, _config: &Self::Config) -> Result<(), anyhow::Error> {
        let batch = self.state.next().unwrap();
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
        Ok(())
    }
}

pub struct ReadLogs {
    db: Db,
    state: ReadState,
}

impl SimpleBench for ReadLogs {
    type GroupState = TempDir;
    type Config = ReadConfig;
    const BACKEND: &'static str = "Sled";

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

    fn execute_measured(&mut self, config: &Self::Config) -> Result<(), anyhow::Error> {
        // To be fair, we're only evaluating that content equals when it's a single get
        if config.get_count == 1 {
            let entry = self.state.next().unwrap();
            let bytes = self
                .db
                .get(&entry.id.to_be_bytes())?
                .expect("value not found");
            let decoded = pot::from_slice::<LogEntry>(&bytes)?;
            assert_eq!(&decoded, &entry);
        } else {
            for _ in 0..config.get_count {
                let entry = self.state.next().unwrap();
                self.db
                    .get(&entry.id.to_be_bytes())?
                    .expect("value not found");
            }
        }
        Ok(())
    }
}
