# Structural Design

A database is stored as a _merkle tree_, which is a type of
[merkle DAG](https://github.com/greglook/merkledag-core) structure. Each node in
the tree is an immutable [content-addressed block](https://github.com/greglook/blocks)
identified by a [cryptographic hash](https://github.com/multiformats/clj-multihash)
of its byte content. The data in each block is serialized with a _codec_ and
wrapped in [multistream headers](https://github.com/multiformats/clj-multistream)
to support format versioning and feature evolution. Initial versions will use
the [CBOR](https://github.com/greglook/clj-cbor) format for node data and Snappy
for compression.

![MerkleDB data structure](images/db-data-structure.jpg)

Within a node, references to other nodes are represented with _merkle links_,
which combine a multihash target with an optional name and the recursive size of
the referenced block. These links are themselves part of the hashed content of
the node, so a change to any part of the tree must propagate up to the root
node. The entire immutable tree of data at a specific version can therefore be
identified by the hash of the database root node.


## Database Roots

The root of a database is a block which contains database-wide settings and
maps table names to _table root nodes_.

```clojure
{:data/type :merkle-db/database
 :merkle-db.database/tables {String MerkleLink}
 ,,,}
```

Database roots may contain additional arbitrary entries to support user-set
metadata on the table. For example, this could be used to add a
`:data/description` value to the database.


## Table Roots

A table root is a node which contains table-wide settings and links to
the tree of record data. The records in a table are grouped into _partitions_,
which each contain the data for a contiguous, non-overlapping range of record
keys.

```clojure
{:data/type :merkle-db/table-root
 :merkle-db.record/count Long
 :merkle-db.record/families {Keyword #{field-key}}
 :merkle-db.key/lexicoder Keyword
 :merkle-db.index/fan-out Long       ; 256 children
 :merkle-db.partition/limit Long     ; 10,000 records
 :merkle-db.patch/limit Long         ; 100 changes
 :merkle-db.table/data MerkleLink
 :merkle-db.table/patch MerkleLink
 ,,,}
```

Similar to the database root, tables may also contain additional entries to
attach user-specified metadata to them.


## Data Index Trees

Index trees are modeled after a [B+ tree](https://en.wikipedia.org/wiki/B%2B_tree)
and contain both internal index nodes and leaf partitions. Records in the tree
are sorted by their _id key_, which uniquely identifies each record within
the table. Record keys are just bytes, allowing for pluggable key serialization
formats.

The tree nodes contain a count of the records under them, so the index is also
an [order statistic tree](https://en.wikipedia.org/wiki/Order_statistic_tree).
A similar metric for the linked block sizes allows for quick recursive data
sizing.

```clojure
{:data/type :merkle-db/index
 :merkle-db.record/count Long
 :merkle-db.record/first-key key-bytes
 :merkle-db.record/last-key key-bytes
 :merkle-db.index/height Long
 :merkle-db.index/keys
 [key-bytes-A  ; encoded key A
  key-bytes-B  ; encoded key B
  ...
  key-bytes-Z] ; encoded key Z
 :merkle-db.index/children
 [link-0       ; link to subtree containing pk < A
  link-1       ; link to subtree containing A <= pk < B
  link-2       ; link to subtree containing B <= pk < ...
  ...
  link-26]}    ; link to subtree containing Z <= pk
```


## Partitions

Partitions hold contiguous non-overlapping ranges of records, sorted by id key.
The records' field data is stored in _tablets_, which are linked from each
partition.

Unlike the internal nodes, partitions may contain significantly more entries
than the index tree's fan-out. When partitions grow above this limit, they are
split into two smaller partitions to enable better parallelism when processing
the table.

Tables may define _families_ of fields which are often accessed together, as a
storage optimization for queries. Each family of fields will be written in
separate tablet in each partition, allowing queries to read from only the
families whose data they require.

Partitions always contain at least one _base_ tablet, which is used to store
the data from any fields not grouped into a family. _All_ record keys will be
present in the base tablet, even if there is no field data present. This makes
sure that the full sequence of keys can be enumerated with only the base.

```clojure
{:data/type :merkle-db/partition
 :merkle-db.record/count Long
 :merkle-db.record/families {Keyword #{field-key}}
 :merkle-db.record/first-key key-bytes
 :merkle-db.record/last-key key-bytes
 :merkle-db.partition/membership BloomFilter
 :merkle-db.partition/tablets
 {:base MerkleLink
  family-key MerkleLink}}
```

Partitions store family information locally because occasionally tables may be
in a transitionary state where some partitions' family configuration does not
match the desired one set in the table root.


## Data Tablets

The actual record data is stored in the _tablets_.

```clojure
{:data/type :merkle-db/tablet
 :merkle-db.tablet/records
 [[key-bytes-a {:abc 123, "xyz" true, ...}]
  [key-bytes-b {:abc 456, "xyz" false, ...}]
  ...]}
```

Tablets should not link to any further nodes, and are probably the best
candidate for a custom encoding format. In particular, caching field names at
the beginning of the data and referencing them by number in the actual record
bodies would probably save a lot of space. This may not be a huge win compared
to simply applying compression to the entire tablet block, however.


## Patch Tablets

In addition to the main data tree, tables may contain a _patch tablet_ linked
directly from the root node. This tablet holds complete records and tombstones
sorted by pk which override the main data tree. This is similar to Clojure's
`PersistentVector` tail and allows for amortizing table updates across many
operations. Later, the contents of the patch tablet can be flushed together to
update the main data tree.

Insertions and updates write new full record values to the patch tablet. This
requires a read from the main data tree to fill in values, but saves
tremendously on storage space. Deletions add a tombstone marker to the patch
tablet so that later reads can elide the record.

To perform a batch read, first the patch tablet is consulted. If it contains a
tombstone, the record was deleted. If it contains data, that is taken as the
full record content. If any records were not found, they are read from the main
data tree as normal.

To perform a range scan, both the data tree and the patch tablet are read with
the same range criteria. In the resulting sequence, any records appearing in the
patch tablet replace the values from the data tree. Tombstones will remove a
record from the result if it would have been present; patch data will replace
(not merge with) the data in the tree, or insert the record if it wasn't already
present.

```clojure
{:data/type :merkle-db/patch
 :merkle-db.patch/changes
 [[key-bytes-a {:abc 123, "xyz" true, ...}]
  [key-bytes-b :merkle-db.patch/tombstone]
  ...]}
```
