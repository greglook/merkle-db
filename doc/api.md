# Client API

The library's client interface is broken up into four main sections, dealing
with the various levels of data storage.


## Connection Operations

A _connection_ is a long-lived handle to the backing data store and db ref
manager. It can be used to open databases for reading and writing.

```clojure
; Create a new connection to a backing block store and reference manager.
; Options may include serialization, caching, and other configuration.
(conn/connect node-store ref-manager opts) => conn

; List information about the current version of each database present.
(conn/list-dbs conn opts) =>
({:merkledag.node/id Multihash
  :merkle-db.database/name String
  :merkle-db.database/version Long
  :merkle-db.database/committed-at Instant}
 ...)

; List information about the version history of a specific database.
(conn/get-db-history conn db-name) => (db-version ...)

; Initialize a new database with some optional root data.
(conn/create-db! conn db-name root-data) => db

; Drop a database ref. Note that this will not remove the block data, as it
; may be shared.
(conn/drop-db! conn db-name)

; Open a database for use.
(conn/open-db conn db-name opts) => db

; Ensure all data has been written to the backing block store and update the
; database's root in the ref manager.
(conn/commit! conn db) => db'
```


## Database Operations

Databases provide an immutable wrapper around the dynamic connection to the
block and ref stores. Once you are interacting with the database object, most
operations will return a locally-updated copy but not actually change the
backing storage until `commit!` is called.

Database values are map-like, and present both the database version attributes
(`:merkledag.node/id`, `:merkle-db.database/name`,
`:merkle-db.database/version`, `:merkle-db.database/committed-at`) as well as
the attributes stored in the database root node.

```clojure
; Databases provide direct keyword access to their properties:
(into {} db) =>
{:merkledag.node/id Multihash
 :merkle-db.database/committed-at Instant
 :merkle-db.database/name String
 :merkle-db.database/version Long
 :merkle-db.database/tables {table-name MerkleLink}
 :merkle-db.record/size Long
 ,,,}

; Additional attributes can be associated:
(assoc db :merkle-db.key/lexicoder :string) => db'

; List information about the tables within a database.
(db/list-tables db opts) =>
({:merkledag.node/id Multihash
  :merkle-db.table/name String
  :merkle-db.record/size Long}
 ...)

; Add a new table to the database. Options may include pre-defined field
; families and metadata.
(db/create-table db table-name opts) => db'

; Return a reified value representing the table.
(db/get-table db table-name) => table

; Update the named table. The function `f` will be called with the current
; table value, followed by any provided arguments. The result will be used as
; the new table.
(db/update-table db table-name f & args) => db'

; Remove a table from the database.
(db/drop-table db table-name) => db'

; Flush local changes to the backing store.
(db/flush! db) => db'
```


## Table Operations

Tables are collections of records, identified by a string name. Each table name
must be unique within the database. These operations provide a high-level
interface for accessing and manipulating record data.

The lookup functions all take a set of `fields` to return
information for. This helps reduce the amount of work done to fetch undesired
data from the store. If the fields are `nil` or not provided, all record data
will be returned.

```clojure
; Read the keys of records present in the table. If start and end keys are
; given, only records within the bounds will be returned. A nil start or end
; implies the beginning or end of the data, respectively.
;
; - :min-key
; - :max-key
; - :reverse
; - :offset
; - :limit
(table/keys table opts) => (key ...)

; Scan the records in a table, returning a sequence of data for the given set of
; fields. If start and end keys or indices are given, only records within the
; bounds will be returned (inclusive). A nil start or end implies the beginning
; or end of the data, respectively.
;
; - :fields
; - :min-key
; - :max-key
; - :reverse
; - :offset
; - :limit
(table/scan table opts) => ([key record] ...)

; Read a set of records from the database, returning data for the given set of
; fields for each located record.
(table/read table record-keys opts) => ([key record] ...)

; Write a collection of records to the database, represented as a map of record
; key values to record data maps. The options may include merge resolution
; functions which control how the updates are applied.
;
; - merge-field
; - merge-record
(table/insert table records opts) => table'

; Remove a set of records from the table, identified by a collection of record
; keys.
(table/delete table record-keys) => table'

; Flush local changes to the backing store.
(table/flush! table) => table'
```

### Partition Operations

Partitions divide up the record keys into ranges and are the basic unit of
parallelism. These operations are lower-level and intended for use by
high-performance applications.

```clojure
; List the partitions which compose the blocks of record key ranges for the
; records in the table.
(table/list-partitions table) =>
({:merkledag.node/id Multihash
  :merkle-db.record/count Long
  :merkle-db.record/size Long
  :merkle-db.record/first-key key-bytes
  :merkle-db.record/last-key key-bytes}
 ...)

; Read all the records in the given partition, returning a sequence of data for
; the given set of fields.
(table/read-partition table node-id fields) => ([k record] ...)

; Add a single new partition to a table. The partition must not overlap with
; existing partitions.
(table/add-partition table partition) => table'

; Rebuild a table from a sequence of new or updated partitions. Existing table
; settings and metadata are left unchanged.
(table/rebuild table partition-ids) => table'
```
