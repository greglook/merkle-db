MerkleDB Design Doc
===================

**TODO:** overview

- A _database_ is an immutable collection of _tables_, along with some user
  metadata.
- Tables are collections of _records_, which are identified uniquely within the
  table by a primary key.
- Each record is an associative collection of _fields_, mapping string field
  names to values.
- Values may have any type that the underlying serialization format supports.
  There is no guarantee that all the values for a given field have the same
  type.


## Goals

Primary/secondary design goals...
- Read optimized (be more specific)
- Bulk-processing oriented, where a job processes most or all of the records in
  the table, but possibly only needs access to a subset of the fields.


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


## Database API

The library should support the following interactions with a database:

```clojure
; List the tables present in the database.
(list-tables db) => #{String ...}

; Retrieve the user metadata attached to a database.
(get-db-meta db) => *

; Set the user metadata attached to a database.
(set-db-meta db value) => db'

; Return the number of records in the given table.
(count db table-name) => Long

; Retrieve the user metadata attached to a table.
(get-table-meta db table-name) => *

; Set the user metadata attached to a table.
(set-table-meta db table-name value) => db'

; Read a single record from the database, returning data for the given set of
; fields, or nil if the record is not found.
(read db table-name primary-key fields) => record

; Scan the records in a table, returning a sequence of data for the given set of
; fields. If start and end keys are given, only records within the bounds will
; be returned (inclusive). A nil start or end implies the beginning or end of
; the data, respectively.
(scan db table-name fields)
(scan db table-name fields from-pk to-pk) => (record ...)

; Seek through the records in a table, returning a sequence of data for the
; given set of fields. Like `scan`, but uses record indices instead.
(seek db table-name fields from-index to-index) => (record ...)

; Write a batch of records to the database, represented as a map of primary key
; values to record data maps.
(write db table-name records) => db'

; Remove a batch of records from the table, identified as a set of primary keys.
(delete db table-name primary-keys) => db'

; Change the defined field family groups in a table. This requires rebuilding
; the data blocks, and may take some time. The new families should be provided
; as a keyword mapped to a set of field names.
(alter-families db table-name new-families) => db'

; Optimize the database table by merging any patch records and rebalancing the
; data tree indexes.
(optimize db table-name) => db'
```


## Other Thoughts

- Should be possible to diff two databases and get a quick list of what changed
  down to the segment level. This would allow for determining whether the change
  was all appends to a table, enabling incremental (log-style) processing.
- Should be a way to naturally parallelize processing based on the segment
  boundaries, allowing each job to fetch only a single block.
