use std::{collections::HashSet, fmt::Display, iter::Take, ops::RangeInclusive};

use criterion::{Criterion, Throughput};
use nanorand::{Pcg64, Rng};
use serde::{Deserialize, Serialize};

use crate::{
    logs::nebari::{UnversionedBenchmark, VersionedBenchmark},
    BenchConfig, SimpleBench,
};

#[cfg(feature = "couchdb")]
mod couchdb;
mod nebari;
#[cfg(feature = "persy")]
mod persy;
#[cfg(feature = "sled")]
mod sled;
#[cfg(feature = "sqlite")]
mod sqlite;

pub fn benches(c: &mut Criterion) {
    scans(c);
    inserts(c);
    gets(c);
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub id: i64,
    pub timestamp: u64,
    pub message: String,
}

const ALPHABET: &str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

impl LogEntry {
    pub fn generate(id: i64, rng: &mut Pcg64) -> LogEntry {
        LogEntry {
            id,
            timestamp: rng.generate(),
            message: (0..rng.generate_range(50..1024))
                .map(|_| {
                    let idx = rng.generate_range(0..ALPHABET.len());
                    &ALPHABET[idx..idx + 1]
                })
                .collect(),
        }
    }

    pub fn generate_batch(
        id_base: i64,
        count: usize,
        sequential: bool,
        rng: &mut Pcg64,
        for_insert: bool,
    ) -> Vec<LogEntry> {
        let mut batch = Vec::new();
        for id in 0..count {
            batch.push(LogEntry {
                id: if sequential {
                    id_base + id as i64
                } else {
                    rng.generate_range(0..)
                },
                timestamp: rng.generate(),
                message: (0..rng.generate_range(50..1024))
                    .map(|_| {
                        let idx = rng.generate_range(0..ALPHABET.len());
                        &ALPHABET[idx..idx + 1]
                    })
                    .collect(),
            })
        }
        if for_insert {
            // roots requires that keys be sorted on insert.
            batch.sort_by(|a, b| a.id.cmp(&b.id));
        }
        batch
    }
}

pub struct LogEntryBatchGenerator {
    rng: Pcg64,
    base: i64,
    sequential_ids: bool,
    entries_per_transaction: usize,
}

impl Iterator for LogEntryBatchGenerator {
    type Item = Vec<LogEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = LogEntry::generate_batch(
            self.base,
            self.entries_per_transaction,
            self.sequential_ids,
            &mut self.rng,
            true,
        );
        self.base += self.entries_per_transaction as i64;
        Some(batch)
    }
}

pub struct LogEntryGenerator {
    rng: Pcg64,
    base: i64,
    sequential_ids: bool,
}

impl Iterator for LogEntryGenerator {
    type Item = LogEntry;

    fn next(&mut self) -> Option<Self::Item> {
        let id = if self.sequential_ids {
            self.base += 1;
            self.base
        } else {
            self.rng.generate_range(0..)
        };
        let batch = LogEntry::generate(id, &mut self.rng);
        Some(batch)
    }
}

pub fn inserts(c: &mut Criterion) {
    for sequential_ids in [true, false] {
        let mut group = c.benchmark_group(format!(
            "logs-insert-{}",
            if sequential_ids { "seq" } else { "rdm" }
        ));

        for entries_per_transaction in [1, 100, 1_000, 10_000] {
            let config = InsertConfig {
                sequential_ids,
                entries_per_transaction,
            };

            nebari::InsertLogs::<VersionedBenchmark>::run(&mut group, &config);
            nebari::InsertLogs::<UnversionedBenchmark>::run(&mut group, &config);
            #[cfg(feature = "sled")]
            sled::InsertLogs::run(&mut group, &config);
            #[cfg(feature = "sqlite")]
            sqlite::InsertLogs::run(&mut group, &config);
            #[cfg(feature = "persy")]
            persy::InsertLogs::run(&mut group, &config);
            #[cfg(feature = "couchdb")]
            couchdb::InsertLogs::run(&mut group, &config);
        }
    }
}

#[derive(Clone)]
pub struct InsertConfig {
    pub sequential_ids: bool,
    pub entries_per_transaction: usize,
}

impl BenchConfig for InsertConfig {
    type GroupState = ();
    type State = LogEntryBatchGenerator;
    type Batch = Vec<LogEntry>;

    fn initialize_group(&self) -> Self::GroupState {}

    fn initialize(&self, _group_state: &Self::GroupState) -> Self::State {
        LogEntryBatchGenerator {
            base: 0,
            rng: Pcg64::new_seed(1),
            sequential_ids: self.sequential_ids,
            entries_per_transaction: self.entries_per_transaction,
        }
    }

    fn throughput(&self) -> Throughput {
        Throughput::Elements(self.entries_per_transaction as u64)
    }
}

impl Display for InsertConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} r/tx", self.entries_per_transaction,)
    }
}

pub fn gets(c: &mut Criterion) {
    // Handle the single gets
    for database_size in [Dataset::Small, Dataset::Medium, Dataset::Large] {
        for sequential_ids in [true, false] {
            let mut group = c.benchmark_group(format!(
                "logs-get-{}-{}",
                database_size,
                if sequential_ids { "seq" } else { "rdm" }
            ));
            for mode in [GetMode::Single, GetMode::Multiple] {
                for get_count in mode.get_counts(database_size.size()) {
                    let config = ReadConfig::new(sequential_ids, database_size, get_count);

                    nebari::ReadLogs::<VersionedBenchmark>::run(&mut group, &config);
                    nebari::ReadLogs::<UnversionedBenchmark>::run(&mut group, &config);

                    #[cfg(feature = "sled")]
                    sled::ReadLogs::run(&mut group, &config);
                    #[cfg(feature = "sqlite")]
                    sqlite::ReadLogs::run(&mut group, &config);
                    #[cfg(feature = "persy")]
                    persy::ReadLogs::run(&mut group, &config);
                    #[cfg(feature = "couchdb")]
                    couchdb::ReadLogs::run(&mut group, &config);
                }
            }
        }
    }
}

pub struct ReadConfig {
    sequential_ids: bool,
    database_size: Dataset,
    get_count: usize,
}

impl ReadConfig {
    fn new(sequential_ids: bool, database_size: Dataset, get_count: usize) -> Self {
        Self {
            sequential_ids,
            database_size,
            get_count,
        }
    }
}

#[derive(Clone)]
pub struct ReadState {
    samples: Vec<LogEntry>,
    offset: usize,
}

impl ReadConfig {
    pub fn database_generator(&self) -> Take<LogEntryGenerator> {
        LogEntryGenerator {
            rng: Pcg64::new_seed(1),
            base: 0,
            sequential_ids: self.sequential_ids,
        }
        .take(self.database_size.size())
    }

    pub fn for_each_database_chunk<F: FnMut(&[LogEntry])>(
        &self,
        chunk_size: usize,
        mut callback: F,
    ) {
        let mut database_generator = self.database_generator();
        let mut chunk = Vec::new();
        loop {
            chunk.clear();
            while chunk.len() < chunk_size {
                match database_generator.next() {
                    Some(entry) => {
                        chunk.push(entry);
                    }
                    None => break,
                }
            }
            if chunk.is_empty() {
                break;
            }
            chunk.sort_by(|a, b| a.id.cmp(&b.id));
            callback(&chunk);
        }
    }
}

impl BenchConfig for ReadConfig {
    type GroupState = ReadState;
    type State = ReadState;
    type Batch = LogEntry;

    fn initialize_group(&self) -> Self::GroupState {
        // This is wasteful... but it's not being measured by the benchmark.
        let database = self.database_generator();
        let samples_to_collect = self.database_size.size() / 10;
        let skip_between = self.database_size.size() / samples_to_collect;
        let mut samples = Vec::new();
        for (index, entry) in database.enumerate() {
            if index % skip_between == 0 {
                samples.push(entry);
            }
        }
        Pcg64::new_seed(1).shuffle(&mut samples);
        ReadState { samples, offset: 0 }
    }

    fn initialize(&self, group_state: &Self::GroupState) -> Self::State {
        group_state.clone()
    }

    fn throughput(&self) -> Throughput {
        Throughput::Elements(self.get_count as u64)
    }
}

impl Iterator for ReadState {
    type Item = LogEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset == self.samples.len() {
            self.offset = 0;
        }
        let entry = self.samples[self.offset].clone();
        self.offset += 1;
        Some(entry)
    }
}

impl Display for ReadConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} row{}",
            self.get_count,
            if self.get_count > 1 { "s" } else { "" },
        )
    }
}

enum GetMode {
    Single,
    Multiple,
}

impl GetMode {
    fn get_counts(&self, database_size: usize) -> Vec<usize> {
        match self {
            GetMode::Single => vec![1],
            GetMode::Multiple => vec![(database_size as f64).sqrt() as usize, database_size / 16],
        }
    }
}

#[derive(Copy, Clone)]
enum Dataset {
    Small,
    Medium,
    Large,
}

impl Display for Dataset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Dataset::Small => f.write_str("1k"),
            Dataset::Medium => f.write_str("10k"),
            Dataset::Large => f.write_str("1m"),
        }
    }
}

impl Dataset {
    fn size(&self) -> usize {
        match self {
            Dataset::Small => 1024,
            Dataset::Medium => 10_000,
            Dataset::Large => 1_000_000,
        }
    }
}

pub fn scans(c: &mut Criterion) {
    // Handle the single gets
    for database_size in [Dataset::Small, Dataset::Medium, Dataset::Large] {
        for sequential_ids in [true, false] {
            let mut group = c.benchmark_group(format!(
                "logs-scan-{}-{}",
                database_size,
                if sequential_ids { "seq" } else { "rdm" }
            ));
            for element_count in GetMode::Multiple.get_counts(database_size.size()) {
                let config = ScanConfig::new(sequential_ids, database_size, element_count);

                nebari::ScanLogs::<VersionedBenchmark>::run(&mut group, &config);
                nebari::ScanLogs::<UnversionedBenchmark>::run(&mut group, &config);

                #[cfg(feature = "sled")]
                sled::ScanLogs::run(&mut group, &config);
                #[cfg(feature = "sqlite")]
                sqlite::ScanLogs::run(&mut group, &config);
                #[cfg(feature = "persy")]
                persy::ScanLogs::run(&mut group, &config);
                // #[cfg(feature = "couchdb")]
                // couchdb::ReadLogs::run(&mut group, &config);
            }
        }
    }
}

pub struct ScanConfig {
    sequential_ids: bool,
    database_size: Dataset,
    element_count: usize,
}

impl ScanConfig {
    fn new(sequential_ids: bool, database_size: Dataset, element_count: usize) -> Self {
        Self {
            sequential_ids,
            database_size,
            element_count,
        }
    }
}

#[derive(Clone)]
pub struct ScanState {
    all_ids: Vec<i64>,
    random: Pcg64,
    element_count: usize,
}

impl ScanConfig {
    pub fn database_generator(&self) -> Take<LogEntryGenerator> {
        LogEntryGenerator {
            rng: Pcg64::new_seed(1),
            base: 0,
            sequential_ids: self.sequential_ids,
        }
        .take(self.database_size.size())
    }

    pub fn for_each_database_chunk<F: FnMut(&[LogEntry])>(
        &self,
        chunk_size: usize,
        mut callback: F,
    ) {
        let mut database_generator = self.database_generator();
        let mut chunk = Vec::new();
        loop {
            chunk.clear();
            while chunk.len() < chunk_size {
                match database_generator.next() {
                    Some(entry) => {
                        chunk.push(entry);
                    }
                    None => break,
                }
            }
            if chunk.is_empty() {
                break;
            }
            chunk.sort_by(|a, b| a.id.cmp(&b.id));
            callback(&chunk);
        }
    }
}

impl BenchConfig for ScanConfig {
    type GroupState = ScanState;
    type State = ScanState;
    type Batch = RangeInclusive<i64>;

    fn initialize_group(&self) -> Self::GroupState {
        // This is wasteful... but it's not being measured by the benchmark.
        let database = self.database_generator();
        let mut all_ids = HashSet::new();
        for entry in database {
            all_ids.insert(entry.id);
        }
        let mut all_ids = all_ids.into_iter().collect::<Vec<_>>();
        all_ids.sort_unstable();
        ScanState {
            all_ids,
            random: Pcg64::new_seed(1),
            element_count: self.element_count,
        }
    }

    fn initialize(&self, group_state: &Self::GroupState) -> Self::State {
        group_state.clone()
    }

    fn throughput(&self) -> Throughput {
        Throughput::Elements(self.element_count as u64)
    }
}

impl Iterator for ScanState {
    type Item = RangeInclusive<i64>;

    fn next(&mut self) -> Option<Self::Item> {
        let max_index = self.all_ids.len() - self.element_count;
        let start_index = self.random.generate_range(..max_index);
        Some(self.all_ids[start_index]..=self.all_ids[start_index + self.element_count - 1])
    }
}

impl Display for ScanConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} elements", self.element_count,)
    }
}
