(ns merkle-db.connection
  "Connections are stateful components which manage database locking and
  updating. A connection is backed by a node store, a ref tracker, and a lock
  manager."
  (:require
    [clojure.spec :as s]
    [merkledag.core :as mdag]
    [merkledag.node :as node]
    [merkledag.ref :as ref]
    [merkle-db.data :as data]
    [merkle-db.db :as db]
    [merkle-db.lock :as lock])
  (:import
    merkle_db.db.Database))


;; ## Connection Protocols

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
    "Open a database for use.

    - `:version` open a specific version of the database
    - `:lock`
    ")

  (commit!
    [conn db]
    [conn db-name db]
    [conn db-name db opts]
    "Ensure all data has been written to the backing block store and update the
    database's root value in the ref manager.

    - `:force` commit even if the versions don't match
    "))



;; ## Connection Type

; TODO: implement IRef
(deftype Connection
  [store tracker])


(defn- ref-version-info
  "Convert a ref version map into a database version info map."
  [info]
  {::node/id (::ref/value info)
   ::db/name (::ref/name info)
   ::db/version (::ref/version info)
   ::db/committed-at (::ref/time info)})


(extend-type Connection

  IConnection

  (list-dbs
    [this opts]
    (->> opts
         (ref/list-refs (.tracker this))
         (map ref-version-info)))


  (get-db-history
    [this db-name opts]
    (->> (ref/get-history (.tracker this) db-name)
         (map ref-version-info)))


  (create-db!
    [this db-name root-data]
    ; TODO: lock db
    (->>
      (if (::db/tables root-data)
        root-data
        (assoc root-data ::db/tables nil))
      (hash-map ::node/data)
      (mdag/store-node! (.store this))
      (:id)
      (ref/set-ref! (.tracker this) db-name)
      (ref-version-info)
      (db/load-database (.store this))))


  (drop-db!
    [this db-name]
    ; TODO: lock db
    (->>
      nil
      (ref/set-ref! (.tracker this) db-name)
      (ref-version-info)))


  (open-db
    [this db-name opts]
    (let [version (if-let [^java.time.Instant at-inst (:at-inst opts)]
                    (first (drop-while #(.isBefore at-inst (::ref/time %))
                                       (ref/get-history (.tracker this) db-name)))
                    (ref/get-ref (.tracker this) db-name))]
      (if (::ref/value version)
        ; Load database.
        (db/load-database (.store this) (ref-version-info version))
        ; No version found.
        (throw (ex-info (str "No version found for database " db-name " with " opts)
                        {:type ::no-database-version
                         :db-name db-name
                         :opts opts})))))


  (commit!
    ([this db]
     (commit! this (::db/name db) db))
    ([this db-name db]
     (commit! this db-name db nil))
    ([this db-name ^Database db opts]
     ; TODO: validate spec
     (when-not (string? db-name)
       (throw (IllegalArgumentException.
                (str "Cannot commit database without string name: "
                     (pr-str db-name)))))
     (when-not db
       (throw (IllegalArgumentException. "Cannot commit nil database.")))
     ; TODO: lock db
     ; TODO: check if current version is the same as the version opened at?
     (let [root-id (::node/id db)
           node-data (when root-id (mdag/get-data (.store this) root-id))
           root-data (assoc (.root-data db) ::db/tables (.tables db))
           current-version (ref/get-ref (.tracker this) db-name)]
       (if (and (= root-data node-data)
                (= root-id (::node/id current-version))
                (= db-name (::db/name db)))
         ; No data has changed, return current database.
         db
         ; Otherwise, rebuild db node.
         (let [root-node (mdag/store-node! (.store this) root-data)
               version (ref/set-ref! (.tracker this) db-name (:id root-node))]
           (db/update-backing db (.store this) (ref-version-info version))))))))


(alter-meta! #'->Connection assoc :private true)
