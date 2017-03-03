MerkleDB Design Doc
===================

The high-level semantics of this library are similar to a traditional key-value
data store:

- A _database_ is a collection of _tables_, along with some user metadata.
- Tables are collections of _records_, which are identified uniquely within the
  table by a primary key.
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
- Light-weight versioning and copy-on-write to support "time travel".

Non goals:

- High-frequency, highly concurrent writes. Initial versions will have simple
  database-wide locking for updates.
- Access control. In this library, all authentication and authorization is
  deferred to the storage layers backing the block store and ref manager.


## Storage Structure

A merkle database is stored as a _merkle tree_, where each node in the tree is
an immutable [content-addressed block](https://github.com/greglook/blocks)
identified by a [multihash](https://github.com/multiformats/clj-multihash) of
its byte content. The data in each block is serialized with a _codec_ and
wrapped in [multicodec headers](https://github.com/multiformats/clj-multicodec)
to support format versioning and feature evolution. Initial versions will likely
use [CBOR](https://github.com/greglook/clj-cbor) and a header like
`/merkle-db/v1/cbor/gzip`.

![MerkleDB database structure](doc/images/db-data-structure.jpg)

Within a node, references to other nodes are represented with _merkle links_,
which combine a multihash target with an optional name and the recursive size
of the referenced block. Because these links are themselves part of the hashed
content of the node, a change to any part of the tree must propagate up to the
root node. The entire immutable tree of data at a specific version is therefore
identified by the multihash of the database root node.

### Database Root Node

The root of a database is a block which contains database-wide settings and
maps table names to _table root nodes_.

```clojure
{:data/type :merkle-db/db-root
 :merkle-db/metadata MerkleLink
 :merkle-db.db/tables {table-name MerkleLink}
 :time/updated-at Instant}
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
 :merkle-db/metadata MerkleLink
 :merkle-db.table/data MerkleLink
 :merkle-db.table/patch MerkleLink
 :merkle-db.table/count Long
 :merkle-db.table/families {Keyword #{field-name}}
 :merkle-db.table/branching-factor Long    ;     256
 :merkle-db.table/tablet-size-limit Long   ; 100,000
 :time/updated-at Instant}
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
 :merkle-db.data/height Long
 :merkle-db.data/count Long
 :merkle-db.data/keys
 [key-bytes-A  ; encoded key A
  key-bytes-B  ; encoded key B
  ...
  key-bytes-Z] ; encoded key Z
 :merkle-db.data/children
 [link-0       ; link to subtree containing pk < A
  link-1       ; link to subtree containing A <= pk < B
  link-2       ; link to subtree containing B <= pk < ...
  ...
  link-26]}    ; link to subtree containing Z <= pk
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
 :merkle-db.data/count Long
 :merkle-db.tablet/start-key key-bytes
 :merkle-db.tablet/end-key key-bytes
 :merkle-db.tablet/segments
 {:base MerkleLink
  family-name MerkleLink}}
```

**TODO:** Should families allow duplicate fields?

### Data Segments

The actual record data is stored in the _data segments_.

```clojure
{:data/type :merkle-db/segment
 :merkle-db.segment/records
 [[key-bytes-a {:abc 123, "xyz" true, ...}]
  [key-bytes-b {:abc 456, "xyz" false, ...}]
  ...]}
```

Segments should not link to any further nodes, and are probably the best
candidate for a custom encoding format. In particular, caching field names at
the beginning of the data and referencing them by number in the actual record
bodies would probably save a lot of space. This may not be a huge win compared
to simply applying compression to the entire segment block, however.


## Client API

The library should support the following client interface:

### Connection Operations

A _connection_ is a long-lived handle to the backing data store and db ref
manager. It can be used to open databases for reading and writing.

```clojure
; Create a new connection to a backing block store and reference manager.
; Options may include serialization, caching, and other configuration.
(connect block-store ref-manager & opts) => conn

; List the names of the databases present.
(list-dbs conn) => #{db-name ...}

; Initialize a new database. An initial `:root` value may be provided, allowing
; for cheap database copy-on-write cloning.
(create-db! conn db-name & [root]) => db

; Open a database for use. An optional argument may be provided, which will
; return the last committed database version occurring before that time.
(open-db conn db-name & [at-inst]) => db

; Drop a database ref. Note that this will not remove the block data, as it
; may be shared.
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
; database's root in the ref manager.
(commit! db) => db'
```

### Table Operations

Tables are collections of records, identified by a string name. Each table name
must be unique within the database.

```clojure
; Add a new table to the database. Options may include pre-defined field
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

; Change the defined field family groups in a table. The new families should be
; provided as a keyword mapped to a set of field names. This requires rebuilding
; the record segments, and may take some time. It can sort of be done "online"
; though, by committing after each tablet is rebuilt.
(alter-families db table-name new-families) => db'

; Remove a table from the database.
(drop-table db table-name) => db'
```

### Record Operations

These operations provide a high-level interface for accessing and manipulating
record data. Record maps are returned with both rank and the primary key
attached as metadata.

The lookup functions all take a set of `fields` to return
information for. This helps reduce the amount of work done to fetch undesired
data from the store. If the fields are `nil` or not provided, all record data
will be returned.

```clojure
; Scan the records in a table, returning a sequence of data for the given set of
; fields. If start and end keys or indices are given, only records within the
; bounds will be returned (inclusive). A nil start or end implies the beginning
; or end of the data, respectively.
(scan db table-name & {:keys [fields from-pk to-pk from-index to-index offset limit]) => (record ...)

; Read a set of records from the database, returning data for the given set of
; fields for each located record.
(get-records db table-name primary-keys & [fields]) => record

; Write a collection of records to the database, represented as a map of primary
; key values to record data maps.
(write db table-name records) => db'

; Remove a set of records from the table, identified by a collection of primary
; keys.
(delete db table-name primary-keys) => db'
```

### Tablet Operations

Tablets divide up the record keys into ranges and are the basic unit of
parallelism. These operations are lower-level and intended for use by
high-performance applications.

```clojure
; List the tablets which compose the blocks of primary key ranges for the
; records in the table.
(list-tablets db table-name) =>
({:id Multihash
  :count Long
  :size Long
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


## Use Cases

This section details how to use MerkleDB to satisfy various usage patterns.

### Random Access

For basic usage, the database records can be accessed directly using the
high-level record operations. This will generally not be as performant, but is
sufficient for simple use-cases.

### Bulk Read

Tablets provide a natural grain to parallelize reads over. Either the whole
table or the tablets covering a specific range of keys can be selected for
querying and read in parallel. Each tablet and the corresponding segments only
need to be read by a single job.

Choosing field families which align with the types of queries done over the
data will improve IO efficiency, because only the required segments will be
loaded for each tablet.

### Bulk Update

Doing large bulk writes with non-sorted primary keys will generally update most
of the tablets within a table. In this case, using the high-level write
operation will generally not be very efficient. Instead, updates may be done in
parallel by applying the following method to each table:

1. List the tablets in the table to be updated.
2. Divide up the record keyspace into ranges matching the tablets.
3. Group the record updates into batches based on which tablet's range they fall
   into.
4. In parallel, process each batch of updates and existing tablet to produce a
   sequence of output tablets (for example, there may be more than one if the
   tablet exceeds the size limit and splits). Write the updated segments and new
   tablets to the backing block store.
6. Build a new index tree over the new set of tablets and update the table.

Choosing field families which align with the types of writes to the table will
reduce IO and improve storage efficiency, because the existing segments can be
re-used from the current version.

### Time-Series Data

This is also known as "append-only" or "log-style" data. Writes only ever add
new data to the table, and deletions are generally rare and occur on large
blocks of old data (aging).

In order to support this pattern, the record keys must be monotonically
increasing. This way, new batches of data can be written as a new tablet, whose
record keys are all greater than any keys already in the table. The new tablet
is appended to the sequence of tablets and a new data tree is built to
incorporate it.

For reads from time-series data, it is desirable to find out only "new"
information to enable incremental processing. Because tablets are immutable and
shared broadly, a simple hash id comparison can be used to detect tablets added
or changed between two versions. The fact that tablets don't overlap makes it
safe for consumers to assume all new tablets are new data.

### Logical Record Versions

Logical versions make it possible to write a record with version _v_, then later
try to write version _v-1_ and wind up with the database still reflecting
version _v_. This makes writes idempotent, guaranteeing the database always
reflects the latest version of a record.

Logical versions can be implemented with custom record fields and read/rebuild
data loading logic.

**TODO:** Reads/writes need to accept a "merge" function which takes the
existing data and the new data and returns the data to use.

### Transaction Metadata

Reified transactions can be implemented by adding custom record fields on write
and utilizing the database-level user metadata to store information about the
transactions. Such metadata might include the timestamp, author, commit message,
etc.

**TODO:** It might be good to formalize this with some kind of middleware.
