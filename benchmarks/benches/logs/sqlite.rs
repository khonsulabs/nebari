use std::time::{Duration, Instant};

use rusqlite::{params, Connection};
use tempfile::NamedTempFile;

use super::{InsertConfig, LogEntry, LogEntryBatchGenerator, ReadConfig, ReadState};
use crate::{
    logs::{ScanConfig, ScanState},
    BenchConfig, SimpleBench,
};

pub struct InsertLogs {
    sqlite: Connection,
    _tempfile: NamedTempFile,
    state: LogEntryBatchGenerator,
}

impl SimpleBench for InsertLogs {
    type GroupState = ();
    type Config = InsertConfig;
    const BACKEND: &'static str = "sqlite";

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
        // For fair testing, this needs to use ACID-compliant settings that a
        // user would use in production. While a WAL might be used in
        // production, it alters more than just insert performance. A more
        // complete benchmark which includes both inserts and queries would be
        // better to compare roots against sqlite's WAL performance.
        let tempfile = NamedTempFile::new_in(".")?;
        let sqlite = Connection::open(tempfile.path())?;
        // Sets synchronous to NORMAL, which "should" be safe and provides
        // better performance. See:
        // https://www.sqlite.org/pragma.html#pragma_synchronous
        sqlite.pragma_update(None, "synchronous", &"NORMAL")?;
        // Mac OS does not implement fsync() with the same guarantees as Linux.
        // From Apple's guide "Reducing Disk Writes"
        // (https://developer.apple.com/documentation/xcode/reducing-disk-writes#Minimize-Explicit-Storage-Synchronization):
        //
        // > Only use F_FULLFSYNC when your app requires a strong expectation of
        // > data persistence. Note that F_FULLFSYNC represents a best-effort
        // > guarantee that iOS writes data to the disk, but data can still be
        // > lost in the case of sudden power loss.
        //
        // Rust's implementation of `File::sync_data` calls `fcntl` with
        // `F_FULLFSYNC` on Mac OS, which means Nebari is performing the
        // strongest guarantees that Apple provides that bits are fully
        // persisted to disk before reporting a succesful result. SQLite does
        // not enable this by default, and instead opts for better performance
        // over a best-attempt at ACID-compliance.
        #[cfg(target_os = "mac_os")]
        sqlite.pragma_update(None, "fullfsync", &"true")?;
        sqlite.execute(
            "create table logs (id integer primary key, timestamp integer, message text)",
            [],
        )?;
        Ok(Self {
            sqlite,
            state: config.initialize(config_group_state),
            _tempfile: tempfile,
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
            self.sqlite.execute("begin transaction;", [])?;

            let mut prepared = self
                .sqlite
                .prepare("insert into logs (id, timestamp, message) values (?, ?, ?)")?;
            for log in batch {
                // sqlite doesn't support u64, so we're going to cast to i64
                prepared.execute(params![log.id as i64, log.timestamp as i64, log.message])?;
            }

            self.sqlite.execute("commit transaction;", [])?;
            total_duration += Instant::now() - start;
        }
        Ok(total_duration)
    }
}

pub struct ReadLogs {
    sqlite: Connection,
    state: ReadState,
}

impl SimpleBench for ReadLogs {
    type GroupState = NamedTempFile;
    type Config = ReadConfig;
    const BACKEND: &'static str = "sqlite";

    fn initialize_group(
        config: &Self::Config,
        _group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Self::GroupState {
        let tempfile = NamedTempFile::new_in(".").unwrap();
        let sqlite = Connection::open(tempfile.path()).unwrap();
        sqlite
            .execute(
                "create table logs (id integer primary key, timestamp integer, message text)",
                [],
            )
            .unwrap();

        let mut prepared = sqlite
            .prepare("insert into logs (id, timestamp, message) values (?, ?, ?)")
            .unwrap();
        config.for_each_database_chunk(1_000_000, |chunk| {
            sqlite.execute("begin transaction;", []).unwrap();
            for log in chunk {
                // sqlite doesn't support u64, so we're going to cast to i64
                prepared
                    .execute(params![log.id as i64, log.timestamp as i64, log.message])
                    .unwrap();
            }

            sqlite.execute("commit transaction;", []).unwrap();
        });
        tempfile
    }

    fn initialize(
        group_state: &Self::GroupState,
        config: &Self::Config,
        config_group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Result<Self, anyhow::Error> {
        let sqlite = Connection::open(group_state.path())?;
        Ok(Self {
            sqlite,
            state: config.initialize(config_group_state),
        })
    }

    fn execute_measured(
        &mut self,
        config: &Self::Config,
        iters: u64,
    ) -> Result<Duration, anyhow::Error> {
        let mut total_duration = Duration::default();
        for _ in 0..iters {
            if config.get_count == 1 {
                let entry = self.state.next().unwrap();
                let start = Instant::now();
                let fetched = self.sqlite.query_row(
                    "SELECT id, timestamp, message FROM logs WHERE id = ?",
                    [entry.id],
                    |row| {
                        Ok(LogEntry {
                            id: row.get(0)?,
                            timestamp: row.get::<_, i64>(1)? as u64,
                            message: row.get(2)?,
                        })
                    },
                )?;
                assert_eq!(&fetched, &entry);
                total_duration += Instant::now() - start;
            } else {
                let entries = (0..config.get_count)
                    .map(|_| (self.state.next().unwrap().id as i64).to_string())
                    .collect::<Vec<_>>();
                let start = Instant::now();

                let mut prepared = self.sqlite.prepare(&format!(
                    "SELECT id, timestamp, message FROM logs WHERE id IN ({})",
                    entries.join(",")
                ))?;
                let rows = prepared.query([])?.mapped(|row| {
                    Ok(LogEntry {
                        id: row.get(0)?,
                        timestamp: row.get::<_, i64>(1)? as u64,
                        message: row.get(2)?,
                    })
                });
                assert_eq!(rows.count(), config.get_count);
                total_duration += Instant::now() - start;
            }
        }
        Ok(total_duration)
    }
}

pub struct ScanLogs {
    sqlite: Connection,
    state: ScanState,
}

impl SimpleBench for ScanLogs {
    type GroupState = NamedTempFile;
    type Config = ScanConfig;
    const BACKEND: &'static str = "sqlite";

    fn initialize_group(
        config: &Self::Config,
        _group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Self::GroupState {
        let tempfile = NamedTempFile::new_in(".").unwrap();
        let sqlite = Connection::open(tempfile.path()).unwrap();
        sqlite
            .execute(
                "create table logs (id integer primary key, timestamp integer, message text)",
                [],
            )
            .unwrap();

        let mut prepared = sqlite
            .prepare("insert into logs (id, timestamp, message) values (?, ?, ?)")
            .unwrap();
        config.for_each_database_chunk(1_000_000, |chunk| {
            sqlite.execute("begin transaction;", []).unwrap();
            for log in chunk {
                // sqlite doesn't support u64, so we're going to cast to i64
                prepared
                    .execute(params![log.id as i64, log.timestamp as i64, log.message])
                    .unwrap();
            }

            sqlite.execute("commit transaction;", []).unwrap();
        });
        tempfile
    }

    fn initialize(
        group_state: &Self::GroupState,
        config: &Self::Config,
        config_group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Result<Self, anyhow::Error> {
        let sqlite = Connection::open(group_state.path())?;
        Ok(Self {
            sqlite,
            state: config.initialize(config_group_state),
        })
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
            let mut prepared = self
                .sqlite
                .prepare("SELECT id, timestamp, message FROM logs WHERE id >= ? AND id <= ?")?;
            let rows = prepared.query([range.start(), range.end()])?.mapped(|row| {
                Ok(LogEntry {
                    id: row.get(0)?,
                    timestamp: row.get::<_, i64>(1)? as u64,
                    message: row.get(2)?,
                })
            });
            assert_eq!(rows.count(), config.element_count);
            total_duration += Instant::now() - start;
        }
        Ok(total_duration)
    }
}
