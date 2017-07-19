# Usage Patterns

This document details how to use MerkleDB to satisfy various usage patterns.


## Random Access

For basic usage, the database records can be accessed directly using the
high-level record operations. This will generally not be as performant, but is
sufficient for simple use-cases.


## Bulk Read

Partitions provide a natural grain to parallelize reads over. Either the whole
table or the partitions covering a specific range of keys can be selected for
querying and read in parallel. Each partition and the corresponding tablets
only need to be read by a single job.

Choosing field families which align with the types of queries done over the
data will improve IO efficiency, because only the required tablets will be
loaded for each partition.


## Bulk Update

Doing large bulk writes with unsorted record keys will generally update most of
the partitions within a table. In this case, using the high-level write
operation will generally not be very efficient. Instead, updates may be done in
parallel by applying the following method to each table:

1. List the partitions in the table to be updated.
2. Divide up the record keyspace into ranges matching the partitions.
3. Group the record updates into batches based on which partition's range they
   fall into.
4. In parallel, process each batch of updates and existing partition to produce
   a sequence of output partitions (for example, there may be more than one if
   the partition exceeds the size limit and splits). Write the updated tablets
   and new partitions to the backing block store.
6. Build a new index tree over the new set of partitions and update the table.

Choosing field families which align with the types of writes to the table will
reduce IO and improve storage efficiency, because the existing tablets can be
re-used from the current version.


## Time-Series Data

This is also known as "append-only" or "log-style" data. Writes only ever add
new data to the table, and deletions are generally rare and occur on large
blocks of old data (aging).

In order to support this pattern, the record keys must be monotonically
increasing. This way, new batches of data can be written as a new partition,
whose record keys are all greater than any keys already in the table. The new
partition is appended to the sequence of partitions and a new index tree is built
to incorporate it.

For reads from time-series data, it is desirable to find out only "new"
information to enable incremental processing. Because partitions are immutable
and shared broadly, a simple hash id comparison can be used to detect partitions
added or changed between two versions. The fact that partitions don't overlap
makes it safe for consumers to assume all new partitions are new data.


## Logical Record Versions

Logical versions make it possible to write a record with version _v_, then later
try to write version _v-1_ and wind up with the database still reflecting
version _v_. This makes writes idempotent, guaranteeing the database always
reflects the latest version of a record.

Logical versions can be implemented with a custom `merge-record` function on
inserts. The function should use a version field on the records to determine
which whole data map to keep when writes occur.


## Transaction Metadata

Reified transactions can be implemented by adding custom record fields on write
and utilizing the database-level user metadata to store information about the
transactions. Such metadata might include the timestamp, author, commit message,
etc.

**TODO:** It might be good to formalize this with some kind of middleware.
