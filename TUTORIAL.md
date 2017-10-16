A Quick Introduction to MerkleDB
================================

Hello there! This document will walk you through a brief introduction to the
MerkleDB library and API. To get started, you'll need a repl with the
`merkle-db.*` namespaces available - running `lein repl` in this library will
work just fine.


## Storage Requirements

Before we can build databases, we need to configure the storage mechanism for
both the record data and the reference tracking metadata. MerkleDB is built on
top of the MerkleDAG library, which uses separate abstractions for the two.

Graph nodes are stored as immutable values in a block store. By default, we
could use an in-memory store that would not be persistent, but to make things a
little more real we'll use a filesystem-backed block store.

In order to handle the block encoding, we'll use the standard set of type
extensions that come with MerkleDB.

```clojure
=> (require
     '[blocks.store.file :refer [file-block-store]]
     '[merkledag.core :as mdag]
     '[merkle-db.graph :as graph])

=> (def graph-store
     (mdag/init-store
       :store (file-block-store "var/db/blocks")
       :types graph/codec-types))
```

Next we need to set up storage for the reference metadata. Fortunately, there's
also a simple file-based implementation that is suitable for local testing:

```clojure
=> (require
     '[merkledag.ref.file :refer [file-ref-tracker]])

=> (def ref-tracker
     (file-ref-tracker "var/db/refs.tsv"))
```


## Connections

```clojure
=> (require '[merkle-db.connection :as conn])

=> (def conn (conn/connect graph-store ref-tracker))
```


## Databases

- creating databases
  - what a database represents

- deleting databases

- modifying a database
  - local (in-memory) modifications
  - committing changes
  - database ref versioning


## Tables

- creating tables

- inserting data

- reading data

- removing data

- flushing state


## FAQ

- cloning a database?
