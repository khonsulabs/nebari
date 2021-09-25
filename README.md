# nebari

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

## Features of `nebari`

The main highlights are:

### Append-only file format

The files stored are written in such a way that makes it easy to confirm what
data is valid and what data wasn't fully written to disk before a crash or power
outage occurred. This high concurrency during read operations, as they never are
blocked waiting for a writer to finish.

The major downside of append-only formats is that deleted data isn't cleaned up
until a maintenance process occurs: compaction. This process rewrites the files
contents, skipping over entries that are no longer alive. This process can
happen without blocking the file from being operated on, but it does
introduce IO overhead during the operation.

Nebari will provide the APIs necessary to perform compaction, but due to the IO
overhead introduced by the operation, the implementation strategy is left to
`BonsaiDb` to allow for flexible customization and scheduling.

### Multi-tree ACID-compliant Storage

A transaction log is maintained such that if any failures occur, the database
can be recovered in a consistent state. All writers that were told their
transactions succeeded will still find their data after an unexpected power
event or crash.

### Pluggable encryption support

This crate defines a [`Vault`
trait](https://nebari.bonsaidb.io/main/nebari/trait.Vault.html) that allows
virtually any encryption backend without Nebari needing to know any details
about it.
