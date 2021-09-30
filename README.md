# nebari

![nebari forbids unsafe code](https://img.shields.io/badge/unsafe-forbid-success)
![nebari is considered experimental and unsupported](https://img.shields.io/badge/status-experimental-blueviolet)
[![crate version](https://img.shields.io/crates/v/nebari.svg)](https://crates.io/crates/nebari)
[![Live Build Status](https://img.shields.io/github/workflow/status/khonsulabs/nebari/Tests/main)](https://github.com/khonsulabs/nebari/actions?query=workflow:Tests)
[![HTML Coverage Report for `main` branch](https://khonsulabs.github.io/nebari/coverage/badge.svg)](https://nebari.bonsaidb.io/coverage/)
[![Documentation for `main` branch](https://img.shields.io/badge/docs-main-informational)](https://nebari.bonsaidb.io/main/nebari/)

> nebari - noun - the surface roots that flare out from the base of a bonsai tree

**Warning:** This crate is early in development. The format of the file is not
considered stable yet. Do not use in production.

This crate provides the `Roots` type, which is the transactional storage layer
for [`BonsaiDb`](https://dev.bonsaidb.io/). It is loosely inspired by
[`Couchstore`](https://github.com/couchbase/couchstore).

## Features

Nebari exposes multiple levels of functionality. The lowest level functionality is the [`TreeFile`](https://nebari.bonsaidb.io/main/nebari/tree/struct.TreeFile.html). A `TreeFile` is a key-value store that uses an append-only file format for its implementation.

Using `TreeFile`s and a transaction log, [`Roots`](https://nebari.bonsaidb.io/main/nebari/struct.Roots.html) enables ACID-compliant, multi-tree transactions.

Each tree supports:

- **Key-value storage**: Keys can be any arbitrary byte sequence up to 65,535
  bytes long. For efficiency, keys should be kept to smaller lengths. Values can be up to 4 gigabytes (2^32 bytes) in size.
- **Flexible query options**: Fetch records one key at a time, multiple keys at once, or ranges of keys.
- **Powerful multi-key operations**: Internally, all functions that alter the
  data in a tree use
  [`TreeFile::modify()`](https://nebari.bonsaidb.io/main/nebari/tree/struct.TreeFile.html#method.modify)
  which allows operating on one or more keys and performing [various
  operations](https://nebari.bonsaidb.io/main/nebari/tree/enum.Operation.html).
- **Pluggable low-level modifications**: The [`Vault`
  trait](https://nebari.bonsaidb.io/main/nebari/trait.Vault.html) allows you to
  bring your own encryption, compression, or other functionality to this format.
  Each independently-addressible chunk of data that is written to the file
  passes through the vault.
- **Optional full revision history**. If you don't want to lose old revisions of data, you can use a [`VersionedTreeRoot`](https://nebari.bonsaidb.io/main/nebari/tree/struct.VersionedTreeRoot.html) to store information that [will allow querying old revisions](https://github.com/khonsulabs/nebari/issues/10). Or, if you want to avoid the extra IO, use the [`UnversionedTreeRoot`](https://nebari.bonsaidb.io/main/nebari/tree/struct.UnversionedTreeRoot.html) which only stores the information needed to retrieve the latest data in the file.
- **[ACID](https://en.wikipedia.org/wiki/ACID)-compliance**:
  - **Atomicity**: Every operation on a `TreeFile` is done atomically.
    [`Operation::CompareSwap`](https://nebari.bonsaidb.io/main/nebari/tree/enum.Operation.html#variant.CompareSwap)
    can be used to perform atomic operations that require evaluating the currently stored value.
  - **Consistency**: Atomic locking operations are used when publishing a new
    transaction state. This ensures that readers can never operate on a partially
    updated state.
  - **Isolation**: Currently, each tree can only be accessed exclusively within
    a transaction. This means that if two transactions request the same tree,
    one will execute and complete before the second is allowed access to the
    tree. This strategy could be modified in the future to allow for more
    flexibility.
  - **Durability**: The append-only file format is designed to only allow
    reading data that has been fully flushed to disk. Any writes that were
    interrupted will be truncated from the end of the file.

    Transaction IDs are recorded in the tree headers. When restoring from disk,
    the transaction IDs are verified with the transaction log. Because of the
    append-only format, if we encounter a transaction that wasn't recorded, we
    can continue scanning the file to recover the previous state. We do this
    until we find a successfluly commited transaction.

    This process is much simpler than most database implementations due to the
    simple guarantees that append-only formats provide.

### Why use an append-only file format?

[@ecton](https://github.com/ecton) wasn't a database engineer before starting this project, and depending on your viewpoint may still not be considered a database engineer. Implementing ACID-compliance is not something that should be attempted lightly.

Creating ACID-compliance with append-only formats is much easier to achieve, however, as long as you can guarantee two things:

- When opening a previously existing file, can you identify where the last valid write occurred?
- When writing the file, do not report that a transaction has succeeded until the file is fully flushed to disk.

The B-Tree implementation in Nebari is designed to offer those exact guarantees.

The major downside of append-only formats is that deleted data isn't cleaned up
until a maintenance process occurs: compaction. This process rewrites the files
contents, skipping over entries that are no longer alive. This process can
happen without blocking the file from being operated on, but it does
introduce IO overhead during the operation.

Nebari [will provide](https://github.com/khonsulabs/nebari/issues/3) the APIs
necessary to perform compaction, but may delegate scheduling and automation to
users of the library and `BonsaiDb`.

## Open-source Licenses

This project, like all projects from [Khonsu Labs](https://khonsulabs.com/), are
open-source. This repository is available under the [MIT License](./LICENSE-MIT)
or the [Apache License 2.0](./LICENSE-APACHE).
