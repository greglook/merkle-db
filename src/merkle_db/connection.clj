(ns merkle-db.connection
  (:require
    [merkledag.refs :as refs]
    [merkle-db.data :as data]
    [merkle-db.db :as db]
    [merkle-db.node :as node]))


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
    [conn db-name at-inst]
    "Open a database for use. An optional instant argument may be provided,
    which will return the last committed database version occurring before that
    time."))


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
        (db/->Database store tracker db-name (:value version))
        ; No version found.
        (throw (ex-info (str "No version found for database " db-name " at instant " at-inst)
                        {:type ::no-database-version
                         :db-name db-name
                         :at-inst at-inst}))))))


(alter-meta! #'->Connection assoc :private true)
