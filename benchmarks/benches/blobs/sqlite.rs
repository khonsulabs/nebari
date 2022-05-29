use std::time::{Duration, Instant};

use rusqlite::{params, Connection};
use tempfile::NamedTempFile;

use super::InsertConfig;
use crate::{blobs::BlobGenerator, BenchConfig, SimpleBench};

pub struct InsertBlobs {
    sqlite: Connection,
    _tempfile: NamedTempFile,
    blob: BlobGenerator,
}

impl SimpleBench for InsertBlobs {
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
        // Sets the journal to what seems to be the most optimal, safe setting
        // for @ecton. See:
        // https://www.sqlite.org/pragma.html#pragma_journal_mode
        sqlite.pragma_update(None, "journal_mode", &"TRUNCATE")?;
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
        sqlite.execute("create table blobs (id integer primary key, data blob)", [])?;
        Ok(Self {
            sqlite,
            blob: config.initialize(config_group_state),
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
            let blob = self.blob.next().unwrap();
            let start = Instant::now();
            self.sqlite.execute(
                "insert into blobs (id, data) values (?, ?)",
                params![blob.0 as i64, blob.1.as_slice()],
            )?;
            total_duration += Instant::now() - start;
        }
        Ok(total_duration)
    }
}
