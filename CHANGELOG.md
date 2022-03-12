# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v0.5.0

### Breaking Changes

- `KeyEvaluation` has been renamed to `ScanEvaluation`.
- All ``scan()` functions have been updated with the `node_evaluator`callback
now returns a`ScanEvaluation` instead of a `bool`. To preserve existing
behavior, return`ScanEvaluation::ReadData`instead of true and
`ScanEvaluation::Stop` instead of false.

### Added

- `TreeFile::reduce()`, `Tree::reduce()`, `TransactionTree::reduce()` have been
  added as a way to return aggregated information stored within the nodes. A
  practical use case is the ability to retrieve the number of alive/deleted keys
  over a given range, but this functionality extends to embedded indexes through
  the existing `Reducer` trait.

## v0.4.0

### Breaking Changes

- `get_multiple` has been changed to accept an Iterator over borrowed byte slices.
- `ExecutingTransaction::tree` now returns a `LockedTransactionTree`, which
  holds a shared reference to the transaction now. Previously `tree()` required
  an exclusive reference to the transaction, preventing consumers of Nebari from
  using multiple threads to process more complicated transactions.

  This API is paired by a new addition: `ExecutingTransaction::unlocked_tree`.
  This API returns an `UnlockedTransactionTree` which can be sent across thread
  boundaries safely. It offers a `lock()` function to return a
  `LockedTransactionTree` when the tread is ready to operate on the tree.
- `TransactionManager::push` has been made private. This is a result of the
  previous breaking change. `TransactionManager::new_transaction()` is a new
  function that returns a `ManagedTransaction`. `ManagedTransaction::commit()`
  is the new way to commit a transaction in a transaction manager.

### Fixed

- `TransactionManager` now enforces that transaction log entries are written
  sequentially. The ACID-compliance of Nebari was never broken when
  non-sequential log entries are written, but scanning the log file could fail
  to retrieve items as the scanning algorithm expects the file to be ordered
  sequentially.

### Added

- `ThreadPool::new(usize)` allows creating a thread pool with a maximum number
  of threads set. `ThreadPool::default()` continues to use `num_cpus::get` to
  configure this value automatically.

## v0.3.2

### Fixed

- Fixed potential infinite loop when scanning for a transaction ID that does not
  exist.
- Reading associated transaction log data now works when the data is larger than
  the page size. Previously, the data returned included the extra bytes that the
  transaction log inserts at page boundaries.

## v0.3.1

### Changed

- `BorrowedRange` now exposes its fields as public. Without this, there was no
  way to implement `BorrowByteRange` outside of this crate.
- This crate now explicitly states its minimum supported Rust version (MSRV).
  The MSRV did not change as part of this update. It previously was not
  documented.

## v0.3.0

### Breaking Changes

- `ManagedFile` has had its metadata functions moved to a new trait `File` which
  `ManagedFile` must be an implementor of. This allows `dyn File` to be used
  internally. As a result, `PagedWriter` no longer takes a file type generic
  parameter.
- `ManagedFile` has had its functions `open_for_read` and `open_for_append` have
  been moved to a new trait, `ManagedFileOpener`.
- `FileManager::replace_with` now takes the replacement file itself instead of
  the file's Path.
- `compare_and_swap` has had the `old` parameter loosened to `&[u8]`, avoiding
  an extra allocation.
- `TreeFile::push()` has been renamed `TreeFile::set()` and now accepts any type
  that can convert to `ArcBytes<'static>.

### Added

- `AnyFileManager` has been added to make it easy to select between memory or
  standard files at runtime.
- `Tree::first[_key]()`, `TransactionTree::first[_key]()`, and
  `TreeFile::first[_key]()` have been added, pairing the functionality provided
  by `last()` and `last_key()`.

### Fixed

- Memory files now can be compacted.

## v0.2.2

### Fixed

- Fixed a hypothetical locking deadlock if transactions for trees passed into
  `State::new_transaction` or `Roots::new_transaction` in a consistent order.

## v0.2.1

### Fixed

- Removing a key in a versioned tree would cause subsequent `scan()` operations
  to fail if the key evaluator requested reading data from key that has no
  current data. A safeguard has been put in place to ensure that even if
  `KeyEvaluation::ReadData` is returned on an index that contains no position it
  will skip the operation rather than attempting to read data from the start of
  the file.

  Updating the crate should restore access to any "broken" files.

## v0.2.0

### Breaking Changes

- `tree::State::read()` now returns an `Arc` containing the state, rather than a
  read guard. This change has no noticable impact on microbenchmarks, but yields
  more fair writing performance under heavy-read conditions -- something the
  current microbenchmarks don't test but in-development Commerce Benchmark for
  BonsaiDb unvieled.
- `Buffer` has been renamed to `ArcBytes`. This type has been extracted into its
  own crate, allowing it to be used in `bonsaidb::core`. The new crate is
  [available here](https://github.com/khonsulabs/arc-bytes).
- `Root::scan`, `Tree::scan`, `Tree::get_range`, `TransactionTree::scan`, and
  `TransactionTree::get_range` now take types that implement
  `RangeBounds<&[u8]>`. `BorrowByteRange` is a trait that can be used to help
  borrow ranges of owned data.

### Added

- `nebari::tree::U64Range` has been exposed. This type makes it easier to work
  with ranges of u64s.

## v0.1.1

### Added

- `Tree::replace` has been added, which calls through to `TransactionTree::replace`.
- `Tree::modify` and `TransactionTree::modify` have been added, which execute a
  lower-level modification on the underlying tree.

## v0.1.0

Initial release.
