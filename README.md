MerkleDB
========

[![API codox](https://img.shields.io/badge/doc-API-blue.svg)](https://greglook.github.io/merkle-db/api/)
[![marginalia docs](https://img.shields.io/badge/doc-marginalia-blue.svg)](https://greglook.github.io/merkle-db/marginalia/uberdoc.html)

MerkleDB is a Clojure library for storing and accessing large data sets in a
hybrid column-oriented tree of content-adressable data blocks.

**Right now this project is still a work in progress.** For details, see the
[design doc](doc/design.md), proposed [client interface](doc/api.md), and sample
[usage patterns](doc/usage.md).


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
