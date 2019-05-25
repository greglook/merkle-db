MerkleDB
========

[![CircleCI](https://circleci.com/gh/greglook/merkle-db/tree/master.svg?style=shield&circle-token=e55e37284e553afef82aa79235cdfe201bd87b6f)](https://circleci.com/gh/greglook/merkle-db/tree/master)
[![codecov](https://codecov.io/gh/greglook/merkle-db/branch/master/graph/badge.svg)](https://codecov.io/gh/greglook/merkle-db)
[![core docs](https://img.shields.io/badge/doc-core-blue.svg)](https://greglook.github.io/merkle-db/codox/core/)
[![spark docs](https://img.shields.io/badge/doc-spark-blue.svg)](https://greglook.github.io/merkle-db/codox/spark/)
[![tools docs](https://img.shields.io/badge/doc-tools-blue.svg)](https://greglook.github.io/merkle-db/codox/tools/)

MerkleDB is a Clojure library for storing and accessing large data sets in a
hybrid column-oriented tree of content-adressable data blocks.

**This project is usable, but should be considered alpha quality.** For more
details, see the [design doc](doc/design.md), proposed [client
interface](doc/api.md), and sample [usage patterns](doc/usage.md).


## Installation

Library releases are published on Clojars. To use the latest version with
Leiningen, add the following dependency to your project definition:

[![Clojars Project](http://clojars.org/merkle-db/merkle-db/latest-version.svg)](http://clojars.org/merkle-db/merkle-db)

This will pull in the omnibus package, which in turn depends on each subproject
of the same version. You may instead depend on the subprojects directly if you
wish to omit some functionality, such as Spark integration.


## Concepts

The high-level semantics of this library are similar to a traditional key-value
data store:

- A _database_ is a collection of _tables_, along with some user metadata.
- Tables are collections of _records_, which are identified uniquely within the
  table by an id key.
- Each record is an associative collection of _fields_, mapping field names to
  values.
- Values may have any type that the underlying serialization format supports.
  There is no guarantee that all the values for a given field have the same
  type.


## Goals

The primary design goals of MerkleDB are:

- Flexible schema-free key-value storage.
- High-parallelism reads and writes to optimize for bulk-processing, where a
  job computes over most or all of the records in the table, but possibly only
  needs access to a subset of the fields in each record.

Secondary goals include:

- Efficient storage utilization via deduplication and structural sharing.
- Light-weight versioning and copy-on-write to support immutable reads.
- Building on storage and synchronization abstractions to support hosted service
  backends.

Non-goals:

- High-frequency, highly concurrent writes. Initial versions will have simple
  database-wide locking for updates.
- Access control. In this library, all authentication and authorization is
  deferred to the storage layers backing the block store and ref manager.


## License

This is free and unencumbered software released into the public domain.
See the UNLICENSE file for more information.
