(ns merkle-db.connection
  "Connections are stateful components which manage database identity and
  versioning. A connection is generally backed by a merkledag node store and a
  ref tracker.

  More advanced connection implementations may optionally implement additional
  logic controlling updates, such as locking, conflict resolution, and
  authorization rules."
  (:require
    [clojure.string :as str]
    [merkle-db.database :as db]
    [merkledag.core :as mdag]
    [merkledag.node :as node]
    [merkledag.ref :as ref]))


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
    [conn db-name attrs]
    "Initialize a new database. Optional attributes may be provided to merge
    into the root node data. If the attributes include a `:merkledag.node/id`,
    the database will use it as the root node.")

  (drop-db!
    [conn db-name]
    "Drop a database reference. Note that this will not remove block data, as
    it may be shared.")

  (open-db
    [conn db-name]
    [conn db-name opts]
    "Open a database for use.

    - `:version` open a specific version of the database")

  (commit!
    [conn db]
    [conn db opts]
    "Ensure all data has been written to the backing block store and update the
    database's root value in the ref manager.

    - `:force` commit even if the versions don't match"))



;; ## Connection Type

;; TODO: implement IRef
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
  "Internal `list-dbs` implementation."
  ([conn]
   (-list-dbs conn nil))
  ([^Connection conn opts]
   (cond->> (ref/list-refs (.tracker conn) opts)
     true
     (map ref-version-info)

     (:named opts)
     (filter #(if (string? (:named opts))
                (str/starts-with? (::db/name %) (:named opts))
                (re-seq (:named opts) (::db/name %))))

     (:offset opts)
     (drop (:offset opts))

     (:limit opts)
     (take (:limit opts)))))


(defn- -get-db-history
  "Internal `get-db-history` implementation."
  ([conn db-name]
   (-get-db-history conn db-name nil))
  ([^Connection conn db-name opts]
   (cond->> (ref/get-history (.tracker conn) db-name)
     true
     (map ref-version-info)

     (:offset opts)
     (drop (:offset opts))

     (:limit opts)
     (take (:limit opts)))))


(defn- -create-db!
  "Internal `create-db!` implementation."
  [^Connection conn db-name attrs]
  (when (::ref/value (ref/get-ref (.tracker conn) db-name))
    (throw (ex-info (str "Cannot create new database: " db-name
                         " already exists")
                    {:type ::db-conflict
                     :db-name db-name})))
  (let [db (db/store-root! (.store conn) attrs)
        version (->> (::node/id db)
                     (ref/set-ref! (.tracker conn) db-name)
                     (ref-version-info))]
    (db/update-backing db (.store conn) version)))


(defn- -drop-db!
  "Internal `drop-db!` implementation."
  [^Connection conn db-name]
  (->>
    nil
    (ref/set-ref! (.tracker conn) db-name)
    (ref-version-info)))


(defn- -open-db
  "Internal `open-db` implementation."
  ([conn db-name]
   (-open-db conn db-name nil))
  ([^Connection conn db-name opts]
   (let [version (if-let [^java.time.Instant at-inst (:at-inst opts)]
                   (first (drop-while #(.isBefore at-inst (::ref/time %))
                                      (ref/get-history (.tracker conn) db-name)))
                   (ref/get-ref (.tracker conn) db-name))]
     (if (::ref/value version)
       ;; Load database.
       (db/load-database (.store conn) (ref-version-info version))
       ;; No version found.
       (throw (ex-info (str "No version found for database " db-name
                            " with options " (pr-str opts))
                       {:type ::no-database-version
                        :db-name db-name
                        :opts opts}))))))


(defn- -commit!
  "Internal `commit!` implementation."
  ([conn db]
   (-commit! conn (::db/name db) db nil))
  ([conn db-name db]
   (-commit! conn db-name db nil))
  ([^Connection conn db-name db opts]
   (when-not (string? db-name)
     (throw (IllegalArgumentException.
              (str "Cannot commit database without string name: "
                   (pr-str db-name)))))
   (when-not db
     (throw (IllegalArgumentException. "Cannot commit nil database.")))
   ;; TODO: validate spec
   ;; TODO: check if current version is the same as the version opened at?
   (let [db (db/flush! db)
         root-id (::node/id db)
         current-version (ref/get-ref (.tracker conn) db-name)]
     (if (and (= root-id (::node/id current-version))
              (= db-name (::db/name db)))
       ;; No data has changed, return current database.
       db
       ;; Otherwise, update version.
       (let [version (ref/set-ref! (.tracker conn) db-name root-id)]
         (db/update-backing db (.store conn) (ref-version-info version)))))))


(extend Connection

  IConnection

  {:list-dbs -list-dbs
   :get-db-history -get-db-history
   :create-db! -create-db!
   :drop-db! -drop-db!
   :open-db -open-db
   :commit! -commit!})
