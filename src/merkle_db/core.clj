(ns merkle-db.core
  (:require
    [merkledag.link :as link]
    [merkledag.refs :as refs]
    [merkledag.refs.memory :refer [memory-ref-tracker]]
    [merkle-db.data :as data]
    [merkle-db.db :as db]
    [merkle-db.node :as node]))


; TODO: api ns for protocols?


(defprotocol IConnection
  "Protocol for interacting with backing connection resources to work with one
  or more databases."

  (list-dbs
    [conn opts]
    "List information about the available databases.")

  ; TODO: inspect database version / history

  (create!
    [conn db-name root-id]
    "Initialize a new database. An initial `:root-id` may be provided, which
    will 'clone' the database at that state.")

  (drop!
    [conn db-name]
    "Drop a database reference. Note that this will not remove block data, as
    it may be shared.")

  (open
    [conn db-name at-inst]
    "Open a database for use. An optional instant argument may be provided,
    which will return the last committed database version occurring before that
    time."))


; TODO: should these methods be on the connection or on the database?
(defprotocol ILockManager
  "A lock manager handles aquiring, refreshing, and releasing a lock on
  databases for updating."

  (lock-info
    [conn db-name]
    "Returns information about the currently-held lock on the database. Returns
    nil if the database is not currently locked.")

  (lock!
    [conn db-name client-id duration]
    "Attempt to acquire a lock to update the database. The provided client-id
    is treated opaquely but should be a useful identifier. The duration is a
    requested period in seconds which the lock will expire after.

    Returns a lock info map on success, or throws an exception on failure with
    details about the current lock holder.")

  (renew-lock!
    [conn db-name lock-key duration]
    "Renew a currently-held lock on the database by providing the key and a new
    requested duration. Returns a lock info map on success. Throws an exception
    if the lock is not held by this process.")

  (unlock!
    [conn db-name lock-id]
    "Release the lock held on the named database. Throws an exception if the
    lock is not held by this process."))


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
    database's root value in the ref manager."))


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



;; Test Implementation

(defrecord Database
  [store tracker db-name root-id]

  IDatabase

  (describe
    [this]
    (when-let [db-root (node/get-data store root-id)]
      (->
        (assoc db-root
               :merkledag.node/id root-id
               ::db/name db-name)
        (cond->
          (::data/metadata db-root)
            (assoc ::data/metadata (node/get-data store (::data/metadata db-root)))))))


  (alter-db-meta
    [this f]
    (let [db-root (node/get-data store root-id)
          db-meta (some->> (::data/metadata db-root) (node/get-data store))
          db-meta' (f db-meta)]
      (if (= db-meta db-meta')
        ; Nothing to change.
        this
        ; Store updated metadata node.
        (let [meta-node (node/store-node! store db-meta')
              meta-link (link/create "meta" (:id meta-node) (:size meta-node))
              db-node (node/store-node! store (assoc db-root ::data/metadata meta-link))]
          (assoc this :root-id (:id db-node))))))


  (commit!
    [this]
    ; TODO: lock db
    ; TODO: check if current version is the same as the version opened at?
    (refs/set-ref! tracker db-name root-id)
    this))


(defrecord Connection
  [store tracker]

  IConnection

  (list-dbs
    [conn opts]
    (refs/list-refs tracker {}))


  (create!
    [conn db-name params]
    ; TODO: lock db
    (refs/set-ref!
      tracker
      db-name
      (or (:root-id params)
          (-> params
              (select-keys [:data/title :data/description ::data/metadata])
              (assoc :data/type :merkle-db/db-root
                     ::db/tables {})
              (->> (node/store-node! store))
              (:id)))))


  (drop!
    [conn db-name]
    ; TODO: lock db
    (refs/set-ref! tracker db-name nil))


  (open
    [conn db-name at-inst]
    (let [version (if at-inst
                    (first (drop-while #(.isBefore ^java.time.Instant at-inst (:time %))
                                       (refs/get-history tracker db-name)))
                    (refs/get-ref tracker db-name))]
      (if (:value version)
        ; Build database.
        (map->Database
          {:store store
           :tracker tracker
           :db-name db-name
           :root-id (:value version)})
        ; No version found.
        (throw (RuntimeException.
                 (str "No version found for database " db-name " at instant " at-inst)))))))


(defn mem-connection
  []
  (map->Connection
    {:store (node/memory-node-store)
     :tracker (memory-ref-tracker)}))
