(ns merkle-db.connection
  "Connections are stateful components which manage database locking and
  updating. A connection is backed by a node store, a ref tracker, and a lock
  manager."
  (:require
    [clojure.spec :as s]
    [merkledag.ref :as ref]
    [merkle-db.data :as data]
    [merkle-db.db :as db]
    [merkle-db.lock :as lock]
    [merkle-db.node :as node])
  (:import
    merkle_db.db.Database))


;; ## Specs

(s/def ::db-version
  (s/keys :req [::db/name
                :merkledag.node/id
                ::db/version
                :time/updated-at]))



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
  {:merkledag.node/id (::ref/value info)
   ::db/name (::ref/name info)
   ::db/version (::ref/version info)
   :time/updated-at (::ref/time info)})


(extend-type Connection

  IConnection

  (list-dbs
    [this opts]
    (->> {}
         (ref/list-refs (.tracker this))
         (map ref-version-info)))


  (get-db-history
    [this db-name opts]
    (map ref-version-info (ref/get-history (.tracker this) db-name)))


  (create-db!
    [this db-name params]
    ; TODO: lock db
    (->>
      (or (:root-id params)
          (-> params
              (select-keys [:data/title :data/description ::data/metadata])
              (assoc :data/type :merkle-db/db-root
                     ::db/tables {})
              (->> (node/store-node! (.store this)))
              (:id)))
      (ref/set-ref! (.tracker this) db-name)
      ; TODO: return opened database?
      (ref-version-info)))


  (drop-db!
    [this db-name]
    ; TODO: lock db
    (ref-version-info (ref/set-ref! (.tracker this) db-name nil)))


  (open-db
    [this db-name opts]
    (let [version (if-let [^java.time.Instant at-inst (:at-inst opts)]
                    (first (drop-while #(.isBefore at-inst (::ref/time %))
                                       (ref/get-history (.tracker this) db-name)))
                    (ref/get-ref (.tracker this) db-name))]
      (if (::ref/value version)
        ; Build database.
        (Database. (.store this) db-name (::ref/version version) (::ref/value version) nil)
        ; No version found.
        (throw (ex-info (str "No version found for database " db-name " with " opts)
                        {:type ::no-database-version
                         :db-name db-name
                         :opts opts})))))


  (commit!
    ([this db]
     (commit! this (.db-name db) db))
    ([this db-name db]
     (commit! this db-name db nil))
    ([this db-name ^Database db opts]
     ; TODO: lock db
     ; TODO: check if current version is the same as the version opened at?
     (ref/set-ref! (.tracker this) db-name (.root-id db))
     ; TODO: return new database?
     db)))


(alter-meta! #'->Connection assoc :private true)
