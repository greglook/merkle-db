A Quick Introduction to MerkleDB
================================

Hello there! This document will walk you through a brief introduction to the
MerkleDB library and API. To get started, you'll need a repl with the
`merkle-db.*` namespaces available - running `lein repl` in this library will
work just fine.

Let's start with a fresh namespace:

```clojure
=> (ns merkle-db.playground)
```


## Storage Backends

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
       :store (file-block-store "var/playground/blocks")
       :types graph/codec-types))
#'merkle-db.playground/graph-store
```

Next we need to set up storage for the reference metadata. Fortunately, there's
also a simple file-based implementation that is suitable for local testing:

```clojure
=> (require
     '[merkledag.ref :as mref]
     '[merkledag.ref.file :refer [file-ref-tracker]])

=> (def ref-tracker
     (file-ref-tracker "var/playground/refs.tsv"))
#'merkle-db.playground/ref-tracker

; Do this if you have existing history to load:
=> (merkledag.ref.file/load-history! ref-tracker)
```

These backing stores are [stateful components](https://github.com/stuartsierra/component)
and should be long-lived parts of the system.


## Connections

A _connection_ wraps a graph store and a ref tracker to provide an interface for
managing databases backed by the stores.

```clojure
=> (require '[merkle-db.connection :as conn])

=> (def conn (conn/connect graph-store ref-tracker))
#'merkle-db.playground/conn
```

Connections themselves are not stateful; it is okay to create and destroy them
as needed to serve the application's needs.


## Databases

Let's see what we've got so far:

```clojure
=> (conn/list-dbs conn)
()
```

As expected, there are no databases yet. Let's create one!

### Creating Databases

This repository comes with a copy of the [Iris Plants Database](test/data/iris)
which you can use to test things out.

```clojure
=> (conn/create-db! conn "iris" {:data/title "Iris Plant Database"})
#merkle-db/db
{:data/title "Iris Plant Database",
 :data/type :merkle-db/database,
 :merkle-db.db/committed-at #inst "2017-10-24T20:45:53.698Z",
 :merkle-db.db/name "iris",
 :merkle-db.db/tables {},
 :merkle-db.db/version 1,
 :merkle-db.record/size 118,
 :merkledag.node/id #data/hash "QmUATY6XG4wHA6AWCoed5QpEVAJJncCzEn6TssLMjMHLR4"}
```

Great, we've just created a new empty database named `iris`, and we gave it a
custom attribute to title it. We should be able to see it in the database list
now:

```clojure
=> (conn/list-dbs conn)
({:merkle-db.db/committed-at #inst "2017-10-24T20:45:53.698Z",
  :merkle-db.db/name "iris",
  :merkle-db.db/version 1,
  :merkledag.node/id #data/hash "QmUATY6XG4wHA6AWCoed5QpEVAJJncCzEn6TssLMjMHLR4"})
```

It appears, but notice that we don't get quite as much information back -
databases are tracked by reference, so when doing a listing you'll only see the
attributes stored in the ref-tracker. You can think of the _name_ of a database
as being similar to a _branch_ in a version-control system like git. The name is
stable over time, while the current _version_ changes, resulting in an updated
branch _tip_ which specifies the data at that version.

In merkle-db, the versions are incrementing counters in the ref-tracker, and the
current version of the database is a pointer to the immutable database root
node.

### Working with Databases

Good so far; let's get a handle back to the database and make some changes to
it. `conn/open-db` lets us retrieve the current version of a database by name:

```clojure
=> (def db (conn/open-db conn "iris"))
#'merkle-db.playground/db

=> (:data/title db)
"Iris Plant Database"

=> (assoc db :data/description "For working with the merkle-db API")
#merkle-db/db
{:data/description "For working with the merkle-db API",
 :data/title "Iris Plant Database",
 :data/type :merkle-db/database,
 :merkle-db.db/committed-at #inst "2017-10-24T20:45:53.698Z",
 :merkle-db.db/name "iris",
 :merkle-db.db/tables {},
 :merkle-db.db/version 1,
 :merkle-db.record/size 118,
 :merkledag.node/id #data/hash "QmUATY6XG4wHA6AWCoed5QpEVAJJncCzEn6TssLMjMHLR4"}

=> (:data/description db)
nil
```

Here we can see that databases are immutable values which behave much like
Clojure maps. Our custom title attribute was preserved, we can look things up in
them with keywords, and associate new values in them without changing other
references to the previous value. When adding your own attributes to a database,
you should generally use your own namespaces to prevent conflicts; specifically,
avoid using the `merkle-db` or `merkledag` prefixes.

Note also that adding the description did not change the node-id - manipulating
a database value gives you a _locally updated copy_ which is not persisted until
you commit it!

```clojure
=> (conn/commit! conn *2)
#merkle-db/db
{:data/description "For working with the merkle-db API",
 :data/title "Iris Plant Database",
 :data/type :merkle-db/database,
 :merkle-db.db/committed-at #inst "2017-10-24T21:03:14.293Z",
 :merkle-db.db/name "iris",
 :merkle-db.db/tables {},
 :merkle-db.db/version 2,
 :merkledag.node/id #data/hash "QmY6LL3MArYnyjbijLnbF7JcqhLaC32qnjtN2G9JJq966w"}
```

Two things have changed about our updated database - the node-id is different
(reflecting the inclusion of the description in the hashed data) and the version
has increased to 2. We can see both the original and the new version in the
database history:

```clojure
=> (conn/get-db-history conn "iris")
({:merkle-db.db/committed-at #inst "2017-10-24T21:03:14.293Z",
  :merkle-db.db/name "iris",
  :merkle-db.db/version 2,
  :merkledag.node/id #data/hash "QmY6LL3MArYnyjbijLnbF7JcqhLaC32qnjtN2G9JJq966w"}
 {:merkle-db.db/committed-at #inst "2017-10-24T20:45:53.698Z",
  :merkle-db.db/name "iris",
  :merkle-db.db/version 1,
  :merkledag.node/id #data/hash "QmUATY6XG4wHA6AWCoed5QpEVAJJncCzEn6TssLMjMHLR4"})
```

### Cloning Databases

Sometimes it can be useful to make a copy of a database to do some experimental
work. Since databases are immutable values, we can trivially copy the current
data to a new name:

```clojure
=> (conn/commit! conn "iris2" db)
#merkle-db/db
{:data/description "For working with the merkle-db API",
 :data/title "Iris Plant Database",
 :data/type :merkle-db/database,
 :merkle-db.db/committed-at #inst "2017-10-24T21:11:43.846Z",
 :merkle-db.db/name "iris2",
 :merkle-db.db/tables {},
 :merkle-db.db/version 1,
 :merkledag.node/id #data/hash "QmY6LL3MArYnyjbijLnbF7JcqhLaC32qnjtN2G9JJq966w"}

=> (conn/list-dbs conn)
({:merkle-db.db/committed-at #inst "2017-10-24T21:03:14.293Z",
  :merkle-db.db/name "iris",
  :merkle-db.db/version 2,
  :merkledag.node/id #data/hash "QmY6LL3MArYnyjbijLnbF7JcqhLaC32qnjtN2G9JJq966w"}
 {:merkle-db.db/committed-at #inst "2017-10-24T21:11:43.846Z",
  :merkle-db.db/name "iris2",
  :merkle-db.db/version 1,
  :merkledag.node/id #data/hash "QmY6LL3MArYnyjbijLnbF7JcqhLaC32qnjtN2G9JJq966w"})
```

Now we have a brand new database named `iris2` that shares the same data as the
original `iris` database. Were we to make changes to `iris2` and commit them,
we'd _fork_ the database from its shared history. No extra space has been used
so far, just ref manipulation.

### Dropping Databases

When you are done with a database, you can drop it from the ref-tracker. This
does **not** remove the underlying block data, because it may be shared among
many database versions. Instead, a background janitor process should perform
garbage collection based on what nodes are reachable from the desired set of
database versions to keep.

```clojure
=> (conn/drop-db! conn "iris2")
{:merkle-db.db/committed-at #inst "2017-10-24T21:15:05.572Z", :merkle-db.db/name "iris2", :merkle-db.db/version 2, :merkledag.node/id nil}

=> (conn/list-dbs conn)
({:merkle-db.db/committed-at #inst "2017-10-24T21:03:14.293Z",
  :merkle-db.db/name "iris",
  :merkle-db.db/version 2,
  :merkledag.node/id #data/hash "QmY6LL3MArYnyjbijLnbF7JcqhLaC32qnjtN2G9JJq966w"})
```


## Tables

Now that you understand the basics of databases, let's talk tables. Each
database is a container for some data in its _tables_. Each table corresponds
to a 'kind' of data you might want to use; not every record in a table needs to
have exactly the same shape, but generally they'll have common sets of fields.

```clojure
=> (require '[merkle-db.db :as db])

=> (def db (conn/open-db conn "iris"))
#merkle-db.playground/db
```

### Creating Tables

One way to make a new table is to create it directly in a database:

```
=> (db/create-table db "test" {:data/title "Example test table"})
#merkle-db/db
{:data/description "For working with the merkle-db API",
 :data/title "Iris Plant Database",
 :data/type :merkle-db/database,
 :merkle-db.db/committed-at #inst "2017-10-24T21:03:14.293Z",
 :merkle-db.db/name "iris",
 :merkle-db.db/tables {"test" #merkle-db/table
                       {:data/title "Example test table",
                        :data/type :merkle-db/table,
                        :merkle-db.index/fan-out 256,
                        :merkle-db.partition/limit 1000,
                        :merkle-db.patch/limit 100,
                        :merkle-db.record/count 0,
                        :merkle-db.table/name "test"}},
 :merkle-db.db/version 2,
 :merkle-db.record/size 173,
 :merkledag.node/id #data/hash "QmY6LL3MArYnyjbijLnbF7JcqhLaC32qnjtN2G9JJq966w"}
```

Again, note that the database node id has not changed because the table has
been added as a local modification. In fact, you can see as much by how the
table is directly embedded in the `::db/tables` field. Let's save our work:

```clojure
=> (conn/commit! conn *1)
#merkle-db/db
{:data/description "For working with the merkle-db API",
 :data/title "Iris Plant Database",
 :data/type :merkle-db/database,
 :merkle-db.db/committed-at #inst "2017-10-24T23:00:12.120Z",
 :merkle-db.db/name "iris",
 :merkle-db.db/tables {"test" #merkledag/link ["table:test" #data/hash "QmW5U8F2zsyuRWrEppmy8zpS3LyhYvfkRtba1nsi3Y6HNX" 204]},
 :merkle-db.db/version 3,
 :merkledag.node/id #data/hash "QmTWNVAGsGaN2prJMazum6pAuopSSLZBXeWpj4Q4T1TGND"}

=> (def db *1)
#merkle-db.playground/db
```

The in-line table has become a merkle link, because the table root has been
serialized out into a separate node. This is probably a good time to introduce
the vizualization tools (you will need `graphviz` installed):

```clojure
=> (require '[merkle-db.viz :as viz])

=> (viz/view-database db)
```

We've now got a two-node immutable graph representing our database!

### Working with Tables

Now that we have a table, we need to do something with it. Get the table from
the database and we can start working:

```clojure
=> (db/get-table db "test")
#merkle-db/table
{:data/title "Example test table",
 :data/type :merkle-db/table,
 :merkle-db.index/fan-out 256,
 :merkle-db.partition/limit 1000,
 :merkle-db.patch/limit 100,
 :merkle-db.record/count 0,
 :merkle-db.record/size 204,
 :merkle-db.table/name "test",
 :merkledag.node/id #data/hash "QmW5U8F2zsyuRWrEppmy8zpS3LyhYvfkRtba1nsi3Y6HNX"}

=> (def table *1)
#'merkle-db.playground/table

=> (require '[merkle-db.table :as table])

=> (table/insert table [{:id 1, :foo 123, :bar "baz"}])
; IllegalArgumentException BytesLexicoder cannot encode non-byte-array value: nil (null)  merkle-db.key/eval17099/fn--17102 (key.clj:426)
```

Wait, what happened? When we created the table, we didn't specify any
information about the structure of the records we're planning to store in it. As
a result, we've gotten the default settings to identify records with the primary
key `:merkle-db.record/id` and lexicode them into key values directly from byte
arrays. Not only does our sample record not use that key as an identifier, the
value is a number rather than a byte array! Let's fix this:

```clojure
=> (require '[merkle-db.key :as key])

=> (alter-var-root #'table assoc ::table/primary-key :id ::key/lexicoder :integer)
#merkle-db/table
{:data/title "Example test table",
 :data/type :merkle-db/table,
 :merkle-db.index/fan-out 256,
 :merkle-db.key/lexicoder :integer,
 :merkle-db.partition/limit 1000,
 :merkle-db.patch/limit 100,
 :merkle-db.record/count 0,
 :merkle-db.record/size 204,
 :merkle-db.table/name "test",
 :merkle-db.table/primary-key :id}

=> (table/dirty? table)
true
```

We've updated the table with the necessary attributes, which are reflected as
local modifications; note the absence of the table node id in the result. We
can use the `table/dirty?` method to test for this state. Let's try that insert
again...

```clojure
=> (alter-var-root #'table table/insert [{:id 1, :foo 123, :bar "baz"}])
#merkle-db/table
{:data/title "Example test table",
 :data/type :merkle-db/table,
 :merkle-db.index/fan-out 256,
 :merkle-db.key/lexicoder :integer,
 :merkle-db.partition/limit 1000,
 :merkle-db.patch/limit 100,
 :merkle-db.record/count 1,
 :merkle-db.record/size 204,
 :merkle-db.table/name "test",
 :merkle-db.table/primary-key :id}
```

Great! We now have a table with one record in it. This is still a local change;
let's persist the table now so we don't lose our hard work:

```clojure
=> (alter-var-root #'table table/flush!)
...
```

**TODO:** discuss record upsert logic

**TODO:** update database table, commit db

**TODO:** drop test table, commit db

### Loading Data

**TODO:** load iris dataset into new table, commit

### Reading Data

The simplest way to read data from the table is to ask for specific records:

```clojure
=> (table/read table [...])
...
```

We can also scan the table for all records in sorted order:

```clojure
=> (table/scan table)
...

=> (table/keys table)
...
```
