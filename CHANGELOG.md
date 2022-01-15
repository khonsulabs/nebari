# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Breaking Changes

- `tree::State::read()` now returns an `Arc` containing the state, rather than a
  read guard. This change has no noticable impact on microbenchmarks, but yields
  more fair writing performance under heavy-read conditions -- something the
  current microbenchmarks don't test but in-development Commerce Benchmark for
  BonsaiDb unvieled.

## v0.1.1

### Added

- `Tree::replace` has been added, which calls through to `TransactionTree::replace`.
- `Tree::modify` and `TransactionTree::modify` have been added, which execute a
  lower-level modification on the underlying tree.

## v0.1.0

Initial release.
