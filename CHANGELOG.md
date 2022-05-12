# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Breaking Changes

- Transaction IDs and Sequence IDs are now returned as new types,
  `TransactionId` and `SequenceId` instead of `u64`.
- The `Root` trait now has a new associated type, `Reducer`. The `Reducer`
  trait's methods now take `&self`. These changes allow dynamic reducers to be
  used -- for example, in BonsaiDb, the View indexing system needs to call a
  specific View's `reduce()` function. Without the ability to have `&self`
  passed to the `Reducer`, there was no way to provide any state information to
  the Reducer.

  There are several changes related to this, including:

  - `Root` no longer requires `Default`. Instead, a new function
    `Root::default_with` is used to instantiate newly created tree states.
  - `Root::tree()` requires that `Reducer` implements `Default`, but
    `Root::tree_with_reducer()` can be used to create a `TreeRoot` with a
    specific `Reducer` instance.
  - `Root::deserialize` is provided an additional parameter: the reducer.
- The `reduce()` function now returns an `Option::<ReducedIndex>::None` when no
  keys match. Previously, `rereduce()` was called with an empty array to produce
  a default value. However, this new change allows detecting the difference
  between no matches and a match.
- `StdFile::flush()` is now implemented using
`std::File::flush()`. To synchronize all data and metadata, use
`File::synchronize()` instead.
- `TransactionLog::current_transaction_id` has been removed, and
  `TransactionLog::state` now returns a reference to the current state. This
  function previously was returning `State::next_transaction_id()`, which made
  the name a little weird. To preserve existing behavior, use
  `log.state().next_transaction_id()`.
- `EmbeddedIndex::index` has been moved to a new trait, `Indexer<T>`.
  `EmbeddedIndex::Reducer` has been renamed to `EmbeddedIndex::Indexer`, and now
  requires both `Reducer` and `Indexer` to be implemented.

  This change also adds `&self` to the `index()` function's signature, allowing
  the embedded indexer to have a configuration/state.
- `CompareSwap`'s function now takes an additional parameter:
  `Option<&Root::Index>`. This parameter allows inspecting the currently stored
  index during the swap function.
- `modify()` now returns a `Vec<ModificationResult<Root::Index>>`, which
  contains a list of keys that were modified and their new indexes (if the key
  still exists after the modification). Note that for a Versioned tree,
  modifications will always produce new indexes as removed keys are still
  stored.
- `set()` now returns `Root::Index`, which is the newly stored index for the
  key.
- `replace()` now returns a `(Option<ArcBytes>, Root::Index)>`, which is the
  previously stored value and the new index for this key.
- `remove()` now returns both the key and index.
  
### Fixed

- When using `Roots::delete_tree()` on a tree that had previously been opened,
  an edge case was fixed that could cause a subsequent write operation to return
  an `InternalCommunication` error.
- When using `Roots::delete_tree()`, subsequent read operations will now return
  an appropriate None/empty result instead of returning  a file not found error.
- `log::State::current_transaction_id()` now behaves as documented. Previously,
  it was returning the last transaction ID that the log file had allocated, but
  the transaction ID returned may not have been committed. Now the ID returned
  is guaranteed to be the last ID written to the log.

### Changed

- `TreeFile` functions that previously took an `Option<TransactionId>` now take
  an `Into<PersistenceMode>` generic parameter. `PersistenceMode` allows
  controlling whether a write is transactional (fully synchronized + tagged),
  fully synchronized (ACID), or flushed (susceptible to data loss if the kernel
  doesn't flush it before a crash/power loss).

  `Option::<TransactionId>::None` converts to `PersistenceMode::Sync`. This
  means that existing code will continue to compile and operate as it was
  originally written without using the new `PersistenceMode` enum. However, if
  you wish to remove the overhead of synchronizing, passing
  `PersistenceMode::Flush` will only ensure all application-level caches are
  flushed before confirming the write is successful.
- `BySequenceIndex` now has a generic parameter and a new field: the embedded
  index. When a versioned tree is written, the current embedded index is copied
  to the `BySequenceIndex`, allowing for historical retrieval of index values.
  
  For example, BonsaiDb is using the embedded index to store the document's
  hash. This change allows for historical lookups to retrieve the document hash
  without fetching the value from disk.

  When retrieving a `BySequenceIndex` that was stored prior to this change, the
  embedded index will be None. For all indexes written after this change, the
  embedded index will be present. If you are only working with files written
  after this change, it is safe to unwrap the embedded index.

### Added

- `TreeFile`, `Tree`, and `TransactionTree` now have additional methods that
  allow fetching indexes or values and indexes:

  - `get_index()` - single-key index retrieval
  - `get_with_index()` - single-key value + index retrieval
  - `get_multiple_indexes()` multi-key index retrieval
  - `get_multiple_with_indexes()` multi-key value + index retrieval
  - `get_range_indexes()` ranged index retrieval
  - `get_range_with_indexes()` ranged value + index retrieval
- `Tree` and `TransactionTree` now both have `current_sequence_id()` and
  `scan_sequences()` functions. These functions serve the same purpose as those
  already existing on `TreeFile`.
- `TreeFile`, `Tree`, and `TransactionTree` now have additional methods that
  allow fetching versioned tree's information by `SequenceId``:

  - `get_multiple_by_sequence` - retrieve one or more values by their sequence
    id. This retrieves the value at the time of the sequence id.
  - `get_multiple_indexes_by_sequence` - retrieve one or more indexes by their
    sequence id.
  - `get_multiple_with_indexes_by_sequence` - retrieve one or more values and
    indexes by their sequence id.

  When a sequence is found in any of these functions, a result is returned.
  Because an index will still be present for deleted keys, all retrievals of
  values via this method will return an Option. This allows callers to
  distinguish between `SequenceId` being not found and the value being deleted.

## v0.5.3

### Fixed

- File operations are now fully persisted to disk to the best ability provided
  by each operating system. @justinj discovered that no `fsync()` operations
  were happening, and reported the finding. Nebari's TreeFile was using
  `File::flush()` instead of `File::sync_data()/sync_all()`. This means that it
  would be possible for an OS-level buffer to not be flushed to disk before
  Nebari reported a successful write.

  Interestingly, this change has no noticable impact on performance on Linux.
  However, on Mac OS, `File::sync_data()` performs a `fcntl` with `F_FULLFSYNC`,
  which has a significant impact on performance. This is the correct behavior,
  however, as without this level of guarantee, sudden power loss could result in
  data loss.

  Many people argue that using `F_BARRIERFSYNC` is sufficient for most people,
  but Apple's [own documentation][apple-reducing-disk-writes] states this is
  necessary:

  > Only use F_FULLFSYNC when your app requires a strong expectation of data
  > persistence. Note that F_FULLFSYNC represents a best-effort guarantee that
  > iOS writes data to the disk, but data can still be lost in the case of
  > sudden power loss.

  For now, the stance of Nebari's authors is that `F_FULLFSYNC` is the proper
  way to implement true ACID-compliance.

[apple-reducing-disk-writes]:
    https://developer.apple.com/documentation/xcode/reducing-disk-writes#Minimize-Explicit-Storage-Synchronization

## v0.5.2

### Fixed

- Another edge case similar to the one found in v0.5.1 was discovered through
  newly implemented fuzzer-based testing. When a node is fully absorbed to the
  bottom of the next, in some cases, the modification iterator would not back up
  to reconsider the node. When inserting a new key in this situation, if the new
  key was greater than the lowest key in the next node, the tree would get out
  of order.

  The exact circumstances of this bug are similarly as rare as described in
  v0.5.1's entry.

### Added

- Feature `paranoid` enables extra sanity checks. This feature flag was added
  for purposes of fuzzing. It enables extra sanity checks in release builds that
  are always present in debug builds. These sanity checks are useful in catching
  bugs, but they represent that a database would be corrupted if the state was
  persisted to disk.

  These checks slow down modifications to the database significantly.

## v0.5.1

### Fixed

- `modify()` operations on larger trees (> 50k entries) that performed multiple
  modification operations could trigger a debug_assert in debug builds, or
  worse, yield incorrect databases in release builds.

  The offending situations occur with edge cases surrounding "absorbing" nodes
  to rebalance trees as entries are deleted. This particular edge case only
  arose when the absorb phase moved entries in both directions and performed
  subsequent operations before the next save to disk occurred.

  This bug should only have been able to be experienced if you were using large
  `modify()` operations that did many deletions as well as insertions, and even
  then, only in certain circumstances.

## v0.5.0

### Breaking Changes

- `KeyEvaluation` has been renamed to `ScanEvaluation`.
- All `scan()` functions have been updated with the `node_evaluator` callback
now returns a`ScanEvaluation` instead of a `bool`. To preserve existing
behavior, return`ScanEvaluation::ReadData`instead of true and
`ScanEvaluation::Stop` instead of false.

  The new functionality unlocked with this change is that scan operations can
  now be directed as to whether to skip navigating into an interior node. The
  new `reduce()` function uses this ability to skip scanning nodes when an
  already reduced value is available on a node.

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
