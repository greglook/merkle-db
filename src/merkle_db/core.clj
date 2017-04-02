(ns merkle-db.core)


(defprotocol IConnection
  "Protocol for interacting with backing connection resources to work with one
  or more databases."

  (list-dbs
    [conn]
    "List information about the available databases.")

  (create-db!
    [conn db-name root-id]
    "Initialize a new database. An initial `:root-id` may be provided, which
    will 'clone' the database at that state.")

  (open-db
    [conn db-name at-inst]
    "Open a database for use. An optional instant argument may be provided,
    which will return the last committed database version occurring before that
    time.")

  (drop-db!
    [conn db-name]
    "Drop a database reference. Note that this will not remove block data, as
    it may be shared."))


(defprotocol IDatabase
  "Protocol for interacting with a database at a specific version."

  (describe
    [db]
    "Retrieve descriptive information about a database, including any user-set
    metadata.")

  (alter-db-meta
    [db f]
    "Update the user metadata attached to a database. The function `f` will be
    called with the current value of the metadata, and the result will be used
    as the new metadata.")

  (commit!
    [db]
    "Ensure all data has been written to the backing block store and update the
    database's root value in the ref manager.")

  ; TODO: lock/unlock?
  #_
  (lock!
    [db client-id]
    [db client-id lock-key]
    "...")

  #_
  (unlock!
    [db client-id lock-key]
    "..."))


(defprotocol ITables
  "..."

  (create-table
    [db table-name opts]
    "...")

  (describe-table
    [db table-name]
    "...")

  (alter-table-meta
    [db table-name f]
    "...")

  (alter-families
    [db table-name new-families]
    "...")

  (drop-table
    [db table-name]
    "...."))


(defprotocol IRecords
  "..."

  (scan
    [db table-name opts]
    "...")

  (get-records
    [db table-name primary-keys fields]
    "...")

  (write
    [db table-name records]
    "...")

  (delete
    [db table-name primary-keys]
    "..."))


(defprotocol IPartitions
  "..."

  (list-partitions
    [db table-name]
    "...")

  (read-partition
    [db partition-id fields]
    "...")

  (build-table
    [db table-name partition-ids]
    "..."))



;; ## Connections

; TODO: should probably use deftypes here
(defrecord Connection
  [block-store
   ref-tracker
   key-codec
   data-codec]

  IConnection

  (list-dbs
    [this]
    ,,,)


  (create-db!
    [this db-name root-id]
    ,,,)


  (open-db
    [this db-name at-inst]
    ,,,)


  (drop-db!
    [this db-name]
    ,,,))


(alter-meta! #'->Connection assoc :private true)
(alter-meta! #'map->Connection assoc :private true)


(defn connect
  [& {:as opts}]
  (map->Connection opts))


#_
(defn create-db!
  "Initialize a new database. An initial `:root` value may be provided,
  allowing for cheap database copy-on-write cloning."
  [conn db-name & {:keys [root metadata]}]
  (throw (UnsupportedOperationException. "NYI")))


#_
(defn open-db
  "Open a database for use."
  [conn db-name]
  (throw (UnsupportedOperationException. "NYI")))


#_
(defn drop-db!
  "Drop a database from the backing tracker. Note that this will not remove the
  block data, as it may be shared."
  [conn db-name]
  (throw (UnsupportedOperationException. "NYI")))



;; ## Databases

(defrecord Database
  [connection
   ,,,])


(alter-meta! #'->Database assoc :private true)
(alter-meta! #'map->Database assoc :private true)
