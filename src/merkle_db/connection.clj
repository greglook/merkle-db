(ns merkle-db.connection
  "Connections are stateful components which manage database locking and
  updating. A connection is backed by a node store, a ref tracker, and a lock
  manager."
  (:require
    [clojure.string :as str]
    (merkledag
      [core :as mdag]
      [node :as node]
      [ref :as ref])
    (merkle-db
      [data :as data]
      [db :as db]))
  (:import
    merkle_db.db.Database))


;; ## Connection Protocols

(defprotocol IConnection
  "Protocol for interacting with backing connection resources to work with one
  or more databases."

  (list-dbs
    [conn]
    [conn opts]
    "List information about the available databases.

    Options may include:

    - `:named`
      If set to a string, return databases whose names are prefixed by the
      value. This may also be a regular expression to match database names
      against.
    - `:offset`
      Skip this many matching results.
    - `:limit`
      Return at most this many results.")

  (get-db-history
    [conn db-name]
    [conn db-name opts]
    "Retrieve a history of the versions of the database.

    Options may include:

    - `:offset`
      Skip this many matching results.
    - `:limit`
      Return at most this many results.")

  (create-db!
    [conn db-name]
    [conn db-name attrs]
    "Initialize a new database. Optional additional root attributes
    will 'clone' the database at that state.")

  (drop-db!
    [conn db-name]
    "Drop a database reference. Note that this will not remove block data, as
    it may be shared.")

  (open-db
    [conn db-name]
    [conn db-name opts]
    "Open a database for use.

    - `:version` open a specific version of the database
    - `:lock` acquire a lock for updating the database")

  (commit!
    [conn db]
    [conn db-name db]
    [conn db-name db opts]
    "Ensure all data has been written to the backing block store and update the
    database's root value in the ref manager.

    - `:force` commit even if the versions don't match
    - `:unlock` release the database lock, if held"))



;; ## Connection Type

; TODO: implement IRef
(deftype Connection
  [store tracker])


(alter-meta! #'->Connection assoc :private true)


(defn connect
  "Create a new connection to some backing storage and ref tracker."
  [store tracker]
  (->Connection store tracker))



;; ## Protocol Implementation

(defn- ref-version-info
  "Convert a ref version map into a database version info map."
  [info]
  {::node/id (::ref/value info)
   ::db/name (::ref/name info)
   ::db/version (::ref/version info)
   ::db/committed-at (::ref/time info)})


(defn- -list-dbs
  [^Connection conn opts]
  (->
    opts
    (->>
      (ref/list-refs (.tracker conn))
      (map ref-version-info))
    (cond->>
      (:named opts)
        (filter #(if (string? (:named opts))
                   (str/starts-with? (::db/name %) (:named opts))
                   (re-seq (:named opts) (::db/name %))))
      (:offset opts)
        (drop (:offset opts))
      (:limit opts)
        (take (:limit opts)))))


(defn- -get-db-history
  [^Connection conn db-name opts]
  (->
    (->>
      (ref/get-history (.tracker conn) db-name)
      (map ref-version-info))
    (cond->>
      (:offset opts)
        (drop (:offset opts))
      (:limit opts)
        (take (:limit opts)))))


(defn- -create-db!
  [^Connection conn db-name root-data]
  ; TODO: lock db
  (when (::ref/value (ref/get-ref (.tracker conn) db-name))
    (throw (ex-info (str "Cannot create new database: " db-name
                         " already exists")
                    {:type ::db-conflict
                     :db-name db-name})))
  (->>
    (if (::db/tables root-data)
      root-data
      (assoc root-data ::db/tables nil))
    (hash-map ::node/data)
    (mdag/store-node! (.store conn))
    (::node/id)
    (ref/set-ref! (.tracker conn) db-name)
    (ref-version-info)
    (db/load-database (.store conn))))


(defn- -drop-db!
  [^Connection conn db-name]
  ; TODO: lock db
  (->>
    nil
    (ref/set-ref! (.tracker conn) db-name)
    (ref-version-info)))


(defn- -open-db
  [^Connection conn db-name opts]
  (let [version (if-let [^java.time.Instant at-inst (:at-inst opts)]
                  (first (drop-while #(.isBefore at-inst (::ref/time %))
                                     (ref/get-history (.tracker conn) db-name)))
                  (ref/get-ref (.tracker conn) db-name))]
    (if (::ref/value version)
      ; Load database.
      (db/load-database (.store conn) (ref-version-info version))
      ; No version found.
      (throw (ex-info (str "No version found for database " db-name " with " opts)
                      {:type ::no-database-version
                       :db-name db-name
                       :opts opts})))))


(defn- -commit!
  [^Connection conn db-name ^Database db opts]
  ; TODO: validate spec
  (when-not (string? db-name)
    (throw (IllegalArgumentException.
             (str "Cannot commit database without string name: "
                  (pr-str db-name)))))
  (when-not db
    (throw (IllegalArgumentException. "Cannot commit nil database.")))
  ; TODO: lock db
  ; TODO: check if current version is the same as the version opened at?
  (let [db (db/flush! db)
        root-id (::node/id db)
        current-version (ref/get-ref (.tracker conn) db-name)]
    (if (and (= root-id (::node/id current-version))
             (= db-name (::db/name db)))
      ; No data has changed, return current database.
      db
      ; Otherwise, update version.
      (let [version (ref/set-ref! (.tracker conn) db-name root-id)]
        (db/update-backing db (.store conn) (ref-version-info version))))))


(extend-type Connection

  IConnection

  (list-dbs
    ([this]
     (-list-dbs nil))
    ([this opts]
     (-list-dbs opts)))


  (get-db-history
    ([this db-name]
     (-get-db-history this db-name nil))
    ([this db-name opts]
     (-get-db-history this db-name opts)))


  (create-db!
    ([this db-name]
     (-create-db! this db-name nil))
    ([this db-name root-data]
     (-create-db! this db-name root-data)))


  (drop-db!
    [this db-name]
    (-drop-db! this db-name))


  (open-db
    ([this db-name]
     (-open-db this db-name nil))
    ([this db-name opts]
     (-open-db this db-name opts)))


  (commit!
    ([this db]
     (-commit! this (::db/name db) db nil))
    ([this db-name db]
     (-commit! this db-name db nil))
    ([this db-name db opts]
     (-commit! this db-name db opts))))
