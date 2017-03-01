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


## Goals

**TODO:** Establish usage patterns more clearly

The primary design goals of MerkleDB are:

- Flexible schema-free key-value storage.
- High-parallelism reads and writes to optimize for bulk-processing, where a
  job computes over most or all of the records in the table, but possibly only
  needs access to a subset of the fields in each record.

Secondary goals include:

- Efficient storage utilization via deduplication and structural sharing.
- Light-weight versioning and copy-on-write to support "time travel".

Non goals:

- High-frequency, highly concurrent writes.
- Access control. In this library, all authentication and authorization is
  deferred to the storage layers backing the block store and ref tracker.


## Library API

The library should support the following interactions:

### Connection Operations

A _connection_ is a long-lived handle to the backing data store and db root
tracker. It can be used to open databases for reading and writing.

```clojure
; Create a new connection to a backing block store and reference manager.
; Options may include serialization, caching, and other configuration.
(connect block-store ref-tracker & opts) => conn

; List the names of the databases present.
(list-dbs conn) => #{db-name ...}

; Initialize a new database. An initial `:root` value may be provided, allowing
; for cheap database copy-on-write cloning.
(create-db! conn db-name & [root]) => db

; Open a database for use. An optional argument may be provided, which will
; return the last committed database version occurring before that time.
(open-db conn db-name & [at-inst]) => db

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
{:id Multihash
 :tables {table-name {:count Long, :size Long}}
 :updated-at Instant
 :metadata *}

; Update the user metadata attached to a database. The function `f` will be
; called with the current value of the metadata, followed by any provided
; arguments. The result will be used as the new metadata.
(alter-db-meta db f & args) => db'

; Ensure all data has been written to the backing block store and update the
; database's root in the ref tracker.
(commit! db) => db'
```

### Table Operations

Tables are collections of records, identified by a string name. Each table name
must be unique within the database.

```clojure
; Add a new table to the database. Options may include pre-defined column
; families and metadata.
(create-table db table-name & opts) => db'

; Retrieve descriptive information about a table in the database. The various
; `:count` and `:size` metrics represent the number of records and data size in
; bytes, respectively.
(describe-table db table-name) =>
{:id Multihash
 :name String
 :tablets Long
 :count Long
 :size Long
 :patch {:count Long, :size Long}
 :families {Keyword #{field-names}}
 :metadata *}

; Update the user metadata attached to a table. The function `f` will be
; called with the current value of the metadata, followed by any provided
; arguments. The result will be used as the new metadata.
(alter-table-meta db table-name f & args) => db'

; Change the defined field family groups in a table. This requires rebuilding
; the record segments, and may take some time. The new families should be
; provided as a keyword mapped to a set of field names.
(alter-families db table-name new-families) => db'

; Remove a table from the database.
(drop-table db table-name) => db'
```

### Tablet Operations

Tablets divide up the record keys into ranges and are the basic unit of
parallelism. These operations are lower-level, but intended for use by
high-performance applications.

```clojure
; List the tablets which compose the blocks of primary key ranges for the
; records in the table.
(list-tablets db table-name) =>
({:id Multihash
  :count Long
  :start key-bytes
  :end key-bytes}
 ...)

; Read all the records in the given tablet, returning a sequence of data for
; the given set of fields.
(read-tablet db tablet-id & [fields]) => (record ...)

; Rebuild a table from a sequence of new or updated tablets. Existing table
; settings and metadata are left unchanged.
(build-table db table-name tablet-ids) => db'
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


## Storage Structure

A merkle database is stored as a _merkle tree_, where each node in the tree is
an immutable [content-addressed block](https://github.com/greglook/blocks)
identified by a [multihash](https://github.com/multiformats/clj-multihash) of
its byte content. The data in each block is serialized with a _codec_ and
wrapped in [multicodec headers](https://github.com/multiformats/clj-multicodec)
to support format versioning and future evolution. Initial versions will likely
use [CBOR](https://github.com/greglook/clj-cbor).

Within a node, references to other nodes are represented with _merkle links_,
which combine a multihash target with an optional name and the recursive size
of the referenced block. Because these links are themselves part of the hashed
content of the node, a change to any part of the tree must propagate up to the
root node. The entire immutable tree of data at a specific version is therefore
identified by the multihash of the root node.

### Database Root Node

The root of a database is a block which contains database-wide settings and
maps table names to _table root nodes_.

```clojure
{:data/type :merkle-db/db-root
 :merkle-db.db/tables {"foo" #data/hash "Qm..."}
 :merkle-db/metadata #data/hash "Qm..."
 :time/updated-at #inst "2017-02-19T18:04:27Z"}
```

### Table Root Node

A table root is a block which contains table-specific information and links to
the collections of record data. The records in a table are grouped into
_tablets_, which contain a contiguous range of record primary keys.

Tablets are the leaves of a _data tree_ of index blocks, sorted by primary key.
The data fields for each record are stored in _segments_, which are linked from
each tablet.

In addition to the tablets, tables contain a _patch segment_ linked directly
from the root node. This segment holds complete records (and tombstones) sorted
by pk which should be preferred to any found in the main table body. This is
similar to Clojure's `PersistentVector` 'tails' and allow for amortizing table
updates across multiple operations.

To perform a read from the tree, first the patch segment is consulted, then the
segments from each tablet corresponding to the requested fields are used to look
up record data. The results are merged into a single sequence of records to
return to the client.

```clojure
{:data/type :merkle-db/table-root
 :merkle-db/metadata #data/hash "Qm..."
 :merkle-db.table/data #data/hash "Qm..."
 :merkle-db.table/patch #data/hash "Qm..."
 :merkle-db.table/count 802580
 :merkle-db.table/families {:foo #{:bar "baz"}}
 :merkle-db.table/branching-factor 256
 :merkle-db.table/tablet-size-limit 100000
 :time/updated-at #inst "2017-02-19T18:04:27Z"}
```

### Data Trees

Data trees are modeled after a [B+ tree](https://en.wikipedia.org/wiki/B%2B_tree)
and contain both internal index nodes and leaf tablets. Records in a data tree
are sorted by their _primary key_, which uniquely identifies each record within
the table. Primary keys are just bytes, allowing for pluggable key serialization
formats.

The data tree blocks contain a count of the records under them, so the index is
also an [order statistic tree](https://en.wikipedia.org/wiki/Order_statistic_tree).
A similar metric for the linked block sizes allows for quick data sizing as well.

```clojure
{:data/type :merkle-db/index
 :merkle-db.data/height 2
 :merkle-db.data/count 239502
 :merkle-db.data/keys
 [key-bytes-A  ; encoded key A
  key-bytes-B  ; encoded key B
  ...
  key-bytes-Z] ; encoded key Z
 :merkle-db.data/children
 [#data/hash "Qm..."     ; link to subtree containing pk < A
  #data/hash "Qm..."     ; link to subtree containing A <= pk < B
  #data/hash "Qm..."     ; link to subtree containing B <= pk < ...
  ...
  #data/hash "Qm..."]}   ; link to subtree containing Z <= pk
```

**TODO:** Define the exact algorithm for ordering keys.

### Tablets

Tablets represent contiguous ranges of records, sorted by primary key. The data
for records are stored in _segments_, which are linked from each tablet.

Unlike the internal nodes, tablets may represent significantly more entries than
the tree's branching factor. When tablets grow above a configurable limit, they
are split into two smaller tablets to enable more parallelism when processing
the table.

Tables may define _families_ of fields which are often accessed together, as a
storage optimization for queries. Each family of fields will be written in
separate segment in each tablet, allowing queries to read from only the families
whose data they require.

Tablets always contain at least one _base_ segment, which is used to store the
data from any fields not grouped into a family. _All_ record keys will be
present in the base segment, even if there is no field data present. This makes
sure that the full sequence of keys can be enumerated with only the base.

```clojure
{:data/type :merkle-db/tablet
 :merkle-db.data/count 83029
 :merkle-db.tablet/start-key key-bytes
 :merkle-db.tablet/end-key key-bytes
 :merkle-db.tablet/segments
 {:base #data/hash "Qm..."
  :foo #data/hash "Qm..."}}
```

### Data Segments

The actual record data is stored in the leaf _segments_.

```clojure
{:data/type :merkle-db/segment
 :merkle-db.segment/records
 [[key-bytes-a {"abc" 123, "xyz" true, ...}]
  [key-bytes-b {"abc" 456, "xyz" false, ...}]
  ...]}
```

Segments should not link to any further nodes, and are probably the best
candidate for a custom encoding format. In particular, caching field names at
the beginning of the data and referencing them by number in the actual record
bodies would probably save a lot of space. This may not be a huge win compared
to simply applying compression to the entire segment block, however.


## Other Thoughts

- Consider the best way to optimize for append-only (log-style) writes - measure
  the deduplication factor for some real patterns first, as the normal tree
  algorithms may be sufficient.
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
- Reified transactions can be implemented client-side by adding custom record
  fields on write and utilizing the database-level user metadata feature. Might
  make sense to formalize this with some kind of configurable middleware.
- Similarly, logical versions can be implemented with custom record fields and
  read/rebuild data loading logic. Consider the requirements for middleware
  for this use-case as well - probably not necessary to build into the core
  library, though.
- Multicodec headers are simpler if combined into one compound header, e.g.
  `/merkle-db/v1/cbor/gzip`.
