# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- `Tree::replace` has been added, which calls through to `TransactionTree::replace`.
- `Tree::modify` and `TransactionTree::modify` have been added, which execute a
  lower-level modification on the underlying tree.

## v0.1.0

Initial release.
