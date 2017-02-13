MerkleDB Design Doc
===================

The high-level semantics of this library are similar to a traditional key-value
data store:

- A _database_ is a collection of _tables_, along with some user metadata.
- Tables are collections of _records_, which are identified uniquely within the
  table by a primary key.
- Each record is an associative collection of _fields_, mapping string field
  names to values.
- Values may have any type that the underlying serialization format supports.
  There is no guarantee that all the values for a given field have the same
  type.

Databases are immutable data structures built as a _Merkle Tree_ of data nodes.
Each node is a [content-adressed block](https://github.com/greglook/blocks), so
a database ultimately resolves to the multihash id of the root block. As a
result, "committing" an updated database version means updating a mutable
reference to the root block's hash. Then readers need access to the underlying
block store, and the library does the rest locally.

Modifications to the database return a new root value, representing the updated
tree. Any unchanged data shares the same blocks as the previous version,
allowing for space usage to generally scale with modification size.


## Goals

**TODO:** Establish usage patterns more clearly

The primary design goals of MerkleDB are:

- Read optimized (be more specific)
- Bulk-processing oriented, where a job processes most or all of the records in
  the table, but possibly only needs access to a subset of the fields in each
  record.
- Concurrent access by multiple readers, regardless of writers.

Secondary goals include:

- Efficient storage utilization via deduplication and structural sharing.
- "Time travel" support by tracking database versions across time.

Non goals:

- User management. In this library, all authentication and authorization is
  deferred to the storage layers backing the block store.


## Storage Structure

- A database is represented by a _db root_ block, which contains db-wide
  information and maps string table names to _table roots_.
- Primary keys are just bytes, allowing for pluggable key serialization formats.
- **TODO:** exact algorithm for ordering keys. Should this be pluggable too?
- **TODO:** pluggable serialization backends via MultiStream/Codec?
- Tables may define _families_ of fields which are often accessed together, as a
  storage optimization for queries.
- The records in a table are stored in _segments_, which are the leaves of a
  _data tree_ of index blocks, sorted by primary key. The index is a variant of
  a [B+ tree](https://en.wikipedia.org/wiki/B%2B_tree).
- The data tree blocks contain a count of the records under them, so the index is
  also an [order statistic tree](https://en.wikipedia.org/wiki/Order_statistic_tree).
  A similar metric for the linked block sizes allows for quick data sizing as
  well.
- Tables always contain at least one _base_ data tree, which is used to store
  the data from any fields not grouped into a family. Additionally, _all_ record
  keys will be present in the base tree, even if there is no field data present.
  This makes sure that the full sequence of keys can be enumerated with only the
  base tree.
- Each family of fields will be written as additional separate data trees in the
  table.
- Reads will only load data from the trees containing the fields they need.
  Multiple trees will be returned as an ordered merge.
- Should tables support a "patch" block linked from the table root? This would
  hold complete records sorted by pk which should be preferred to any found in the
  main table body. This is similar to Clojure's PersistentVector 'tails' and
  allow for amortizing table updates across a few versions.
- Bound leaf segments based on size, rather than record count?


## Library API

The library should support the following interactions:

### Connection Operations

A _connection_ is a long-lived handle to the backing data store and db root
tracker. It can be used to open databases for reading and writing.

```clojure
; Create a new connection to a backing block store and reference manager.
; Options may include serialization, caching, and other configuration.
(connect block-store ref-tracker & opts) => conn

; Initialize a new database. An initial `:root` value may be provided, allowing
; for cheap database copy-on-write cloning.
(create-db! conn db-name & [root]) => db

; Open a database for use.
(open-db conn db-name) => db

; Drop a database from the tracker. Note that this will not remove the block
; data, as it may be shared.
(drop-db! conn db-name)
```

### Database Operations

Databases provide an immutable wrapper around the dynamic connection to the
block and ref stores. Once you are interacting with the database object, most
operations will return a locally-updated copy but not actually change the
backing storage until `commit!` is called.

```clojure
; Retrieve descriptive information about a database, including any user-set
; metadata.
(describe db) =>
{:root Multihash
 :tables {table-name {:count Long, :size Long}}
 :metadata *}

; Update the user metadata attached to a database. The function `f` will be
; called with the current value of the metadata, followed by any provided
; arguments. The result will be used as the new metadata.
(alter-db-meta db f & args) => db'

; Ensure all data has been written to the backing block store and update the
; database's root in the ref tracker.
(commit! db) => db
```

### Table Operations

Tables are collections of records, identified by a string name. Each table name
must be unique within the database.

```clojure
; Add a new table to the database. Options may include pre-defined column
; families and metadata.
(create-table db table-name & opts) => db'

; Retrieve the user metadata attached to a table.
(describe-table db table-name) =>
{:name String
 :count Long  ; total number of records
 :size Long   ; total data size in bytes
 :base {:count Long, :size Long}
 :patch {:count Long, :size Long}
 :families {Keyword {:count Long, :size Long, :fields #{String}}}
 :metadata *}

; Update the user metadata attached to a table. The function `f` will be
; called with the current value of the metadata, followed by any provided
; arguments. The result will be used as the new metadata.
(alter-table-meta db table-name f & args) => db'

; Change the defined field family groups in a table. This requires rebuilding
; the data blocks, and may take some time. The new families should be provided
; as a keyword mapped to a set of field names.
(alter-families db table-name new-families) => db'

; Optimize the database table by merging any patch records and rebalancing the
; data tree indexes.
(optimize-table db table-name) => db'

; Remove a table from the database.
(drop-table db table-name) => db'
```

### Record Operations

The record lookup functions all take a set of `fields` to return information
for. This helps reduce the amount of work done to fetch undesired data from the
store. If the fields are `nil` or not provided, all record data will be
returned.

Record maps are returned with both rank and the primary key attached as
metadata.

```clojure
; Scan the records in a table, returning a sequence of data for the given set of
; fields. If start and end keys are given, only records within the bounds will
; be returned (inclusive). A nil start or end implies the beginning or end of
; the data, respectively.
(scan db table-name & [fields from-pk to-pk]) => (record ...)

; Seek through the records in a table, returning a sequence of data for the
; given set of fields. Like `scan`, but uses record indices instead.
(seek db table-name & [fields from-index to-index]) => (record ...)

; Read a single record from the database, returning data for the given set of
; fields, or nil if the record is not found.
(get-record db table-name primary-key & [fields]) => record

; Write a batch of records to the database, represented as a map of primary key
; values to record data maps.
(write db table-name records) => db'

; Remove a batch of records from the table, identified as a set of primary keys.
(delete db table-name primary-keys) => db'
```


## Other Thoughts

- Should be possible to diff two databases and get a quick list of what changed
  down to the segment level. This would allow for determining whether the change
  was all appends to a table, enabling incremental (log-style) processing.
- Find a good way to naturally parallelize processing based on the segment
  boundaries, allowing each job to fetch only a single block.
- Data trees should be 'constructable' from a sequence of segments - for
  example, say a job needs to batch write a large number of records which will
  result in updating most of the segments in the tree. Rather than one job doing
  the work to update every segment, it should be possible to have something like
  Spark split the new records up by primary key (into ranges matching the
  existing segments where possible), distribute them to worker nodes to
  update the original segments, then assemble them back together and build a new
  data tree index over the updated segments. This gets slightly more complicated
  if segments need to split, and potentially even more complicated if they need
  to merge. Needs more investigation.
