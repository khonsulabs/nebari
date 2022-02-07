# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Breaking Changes

- `ManagedFile` has had its metadata functions moved to a new trait `File` which
  `ManagedFile` must be an implementor of. This allows `dyn File` to be used
  internally. As a result, `PagedWriter` no longer takes a file type generic
  parameter.
- `ManagedFile` has had its functions `open_for_read` and `open_for_append` have
  been moved to a new trait, `ManagedFileOpener`.
- `FileManager::replace_with` now takes the replacement file itself instead of
  the file's Path.

### Added

- `AnyFileManager` has been added to make it easy to select between memory or
  standard files at runtime.

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
