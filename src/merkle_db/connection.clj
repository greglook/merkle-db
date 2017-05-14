(ns merkle-db.connection
  (:require
    [merkledag.refs :as refs]
    [merkle-db.data :as data]
    [merkle-db.db :as db]
    [merkle-db.node :as node])
  (:import
    merkle_db.db.Database))


(defprotocol IConnection
  "Protocol for interacting with backing connection resources to work with one
  or more databases."

  (list-dbs
    [conn opts]
    "List information about the available databases.")

  (get-db-history
    [conn db-name opts]
    "Retrieve a history of the versions of the database.")

  (create-db!
    [conn db-name parameters]
    "Initialize a new database. An initial `:root-id` may be provided, which
    will 'clone' the database at that state.")

  (drop-db!
    [conn db-name]
    "Drop a database reference. Note that this will not remove block data, as
    it may be shared.")

  (open-db
    [conn db-name opts]
    "Open a database for use. An optional instant argument may be provided,
    which will return the last committed database version occurring before that
    time.")

  (commit!
    [conn db]
    [conn db-name db]
    [conn db-name db opts]
    "Ensure all data has been written to the backing block store and update the
    database's root value in the ref manager."))


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



;; ## Connection Type

(deftype Connection
  [store tracker])


(extend-type Connection

  IConnection

  (list-dbs
    [this opts]
    (refs/list-refs (.tracker this) {}))


  (create-db!
    [this db-name params]
    ; TODO: lock db
    (refs/set-ref!
      (.tracker this)
      db-name
      (or (:root-id params)
          (-> params
              (select-keys [:data/title :data/description ::data/metadata])
              (assoc :data/type :merkle-db/db-root
                     ::db/tables {})
              (->> (node/store-node! (.store this)))
              (:id)))))


  (drop-db!
    [this db-name]
    ; TODO: lock db
    (refs/set-ref! (.tracker this) db-name nil))


  (open-db
    [this db-name opts]
    (let [version (if-let [^java.time.Instant at-inst (:at-inst opts)]
                    (first (drop-while #(.isBefore at-inst (:time %))
                                       (refs/get-history (.tracker this) db-name)))
                    (refs/get-ref (.tracker this) db-name))]
      (if (:value version)
        ; Build database.
        (Database. (.store this) db-name (:version version) (:value version) nil)
        ; No version found.
        (throw (ex-info (str "No version found for database " db-name " with " opts)
                        {:type ::no-database-version
                         :db-name db-name
                         :opts opts})))))


  (commit!
    [this db]
    (commit! this (.db-name db) db))


  (commit!
    [this db-name ^Database db]
    ; TODO: lock db
    ; TODO: check if current version is the same as the version opened at?
    (refs/set-ref! (.tracker this) db-name (.root-id db))
    this))


(alter-meta! #'->Connection assoc :private true)
