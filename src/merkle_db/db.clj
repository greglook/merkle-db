(ns merkle-db.db
  "Core database functions."
  (:require
    [clojure.future :refer [inst? nat-int?]]
    [clojure.spec :as s]
    [clojure.string :as str]
    (merkledag
      [core :as mdag]
      [link :as link]
      [node :as node])
    (merkle-db
      [record :as record]
      [table :as table])
    [multihash.core :as multihash])
  (:import
    java.time.Instant
    merkle_db.table.Table))


;; ## Data Specifications

(def ^:const data-type
  "Value of `:data/type` that indicates a database root node."
  :merkle-db/database)

(def ^:no-doc info-keys
  "Set of keys which may appear in the database info map."
  #{::node/id ::name ::version ::committed-at ::record/size})

;; Database name.
(s/def ::name
  (s/and string?
         #(not (str/includes? % "/"))
         #(<= 1 (count %) 255)))

;; Database version.
(s/def ::version nat-int?)

;; When the database version was committed.
(s/def ::committed-at inst?)

;; Map of table names to node links.
(s/def ::tables (s/map-of ::table/name mdag/link?))

;; Database root node.
(s/def ::node-data
  (s/and
    (s/keys :req [::tables]
            :opt [:time/updated-at])
    #(= data-type (:data/type %))))

;; Information from the database version.
(s/def ::db-info
  (s/keys :req [::node/id
                ::name
                ::version
                ::committed-at]
          :opt [::record/size]))



;; ## Database API

(defprotocol IDatabase
  "Protocol for interacting with a database at a specific version."

  (list-tables
    [db]
    [db opts]
    "Return a sequence of maps with partial information about the tables in the
    database, including their name and total size.

    Options may include:

    - `:named`
      If set to a string, return tables whose names are prefixed by the value.
      This may also be a regular expression to match table names against.
    - `:offset`
      Skip this many matching results.
    - `:limit`
      Return at most this many results.")

  (get-table
    [db table-name]
    "Return a value representing the named table in the database, or nil if no
    such table exists.")

  (set-table
    [db table-name value]
    "Set the state of the named table. Returns an updated database value.")

  (drop-table
    [db table-name]
    "Remove the named table from the database. Returns an updated database
    value.")

  (flush!
    [db]
    "Ensure any pending changes are persistently stored. Returns an updated
    database value."))


(defn create-table
  "Create a new table with the given name and optional attributes to merge into
  the root node data. Returns an updated database value, or throws an exception
  if the table already exists."
  [db table-name attrs]
  (when (get-table db table-name)
    (throw (ex-info
             (str "Cannot create table: database already has a table named "
                  table-name)
             {:type ::table-conflict
              :table-name table-name})))
  (set-table db table-name (table/bare-table nil table-name attrs)))


(defn update-table
  "Update the table by calling `f` with the current table value and the
  remaining `args`. Returns an updated database value, or throws an
  exception if the table does not exist."
  [db table-name f & args]
  (let [table (get-table db table-name)]
    (when-not table
      (throw (ex-info
               (str "Cannot update table: database has no table named "
                    table-name)
               {:type ::no-table
                :table-name table-name})))
    ; TODO: validate table spec?
    ; TODO: set :time/updated-at
    (set-table db table-name (apply f table args))))


(defn rename-table
  "Update the database by moving the table named `from` to the name `to`.
  Returns an updated database value, or throws an exception if table `from`
  does not exist or `to` already exists."
  [db from to]
  (let [table (get-table db from)]
    (when-not table
      (throw (ex-info
               (str "Cannot rename table: database has no table named "
                    from)
               {:type ::no-table
                :from from
                :to to})))
    (when (get-table db to)
      (throw (ex-info
               (str "Cannot rename table: database already has a table named "
                    to)
               {:type ::table-conflict
                :from from
                :to to})))
    (-> db
        (drop-table from)
        (set-table to table))))



;; ## Database Type

;; Databases are implementad as a custom type so they can behave similarly to
;; natural Clojure values.
;;
;; - `store` reference to the merkledag node store backing the database.
;; - `db-info` map of higher-level database properties such as the name,
;;   node-id, size, version opened, etc.
;; - `root-data` map of data stored in the database root, excluding `::tables`.
;; - `tables` map of table names to reified table objects.
(deftype Database
  [store
   db-info
   root-data
   tables
   _meta]

  Object

  (toString
    [this]
    (format "db:%s:%s"
            (::name db-info "?")
            (::version db-info "?")))


  (equals
    [this that]
    (boolean
      (or (identical? this that)
          (when (identical? (class this) (class that))
            (and (= root-data (.root-data ^Database that))
                 (= tables (.tables ^Database that)))))))


  (hashCode
    [this]
    (hash-combine (hash root-data) (hash tables)))


  clojure.lang.IObj

  (meta
    [this]
    _meta)


  (withMeta
    [this meta-map]
    (Database. store db-info root-data tables meta-map))


  clojure.lang.ILookup

  (valAt
    [this k]
    (.valAt this k nil))


  (valAt
    [this k not-found]
    (cond
      (= ::tables k) tables ; TODO: better return val
      (contains? info-keys k) (get db-info k not-found)
      ; TODO: link-expand value
      :else (get root-data k not-found)))


  clojure.lang.IPersistentMap

  (count
    [this]
    (+ (count db-info) (count root-data) 1))


  (empty
    [this]
    (Database. store db-info nil tables _meta))


  (cons
    [this element]
    (cond
      (instance? java.util.Map$Entry element)
        (let [^java.util.Map$Entry entry element]
          (.assoc this (.getKey entry) (.getValue entry)))
      (vector? element)
        (.assoc this (first element) (second element))
      :else
        (loop [result this
               entries element]
          (if (seq entries)
            (let [^java.util.Map$Entry entry (first entries)]
              (recur (.assoc result (.getKey entry) (.getValue entry))
                     (rest entries)))
            result))))


  (equiv
    [this that]
    (.equals this that))


  (containsKey
    [this k]
    (or (= k ::tables)
        (contains? db-info k)
        (contains? root-data k)))


  (entryAt
    [this k]
    (let [v (.valAt this k this)]
      (when-not (identical? this v)
        (clojure.lang.MapEntry. k v))))


  (seq
    [this]
    (seq (concat (seq db-info)
                 (seq root-data)
                 [(clojure.lang.MapEntry. ::tables tables)])))


  (iterator
    [this]
    (clojure.lang.RT/iter (seq this)))


  (assoc
    [this k v]
    (cond
      (= k :data/type)
        (throw (IllegalArgumentException.
                 (str "Cannot change database :data/type from " data-type)))
      (= k ::tables)
        (throw (IllegalArgumentException.
                 (str "Cannot directly set database tables field " k)))
      (contains? info-keys k)
        (throw (IllegalArgumentException.
                 (str "Cannot change database info field " k)))
      :else
        (Database. store db-info (assoc root-data k v) tables _meta)))


  (without
    [this k]
    (cond
      (= k :data/type)
        (throw (IllegalArgumentException.
                 "Cannot remove database :data/type"))
      (= k ::tables)
        (throw (IllegalArgumentException.
                 (str "Cannot remove database tables field " k)))
      (contains? info-keys k)
        (throw (IllegalArgumentException.
                 (str "Cannot remove database info field " k)))
      :else
        (Database.
          store
          db-info
          (not-empty (dissoc root-data k))
          tables
          _meta))))


(alter-meta! #'->Database assoc :private true)


(defn ^:no-doc load-database
  "Load a database from the store, using the version information given."
  [store version]
  (let [node (mdag/get-node store (::node/id version))]
    (->Database
      store
      (assoc version
             ::record/size (node/reachable-size node))
      (dissoc (::node/data node) ::tables)
      (::tables (::node/data node))
      nil)))


(defn ^:no-doc update-backing
  "Update the database to use the given version information."
  [^Database db store db-info]
  (->Database
    store
    db-info
    (.root-data db)
    (.tables db)
    (meta db)))



;; ## Protocol Implementation

(defn- table-link
  "Build a link to a clean table node."
  [table-name table]
  (link/create
    (str "table:" table-name)
    (::node/id table)
    (::record/size table)))


(defn- link-or-table->info
  "Coerce either a link value or a table record to a map of information."
  [[table-name value]]
  (if (mdag/link? value)
    {::node/id (::link/target value)
     ::table/name table-name
     ::record/size (::link/rsize value)}
    (select-keys
      value
      [::node/id ::table/name ::record/size])))


(defn- -list-tables
  "Internal `list-tables` implementation."
  ([db]
   (-list-tables db nil))
  ([^Database db opts]
   (cond->> (map link-or-table->info (.tables db))
     (:named opts)
       (filter #(if (string? (:named opts))
                  (str/starts-with? (::table/name %) (:named opts))
                  (re-seq (:named opts) (::table/name %))))
     (:offset opts)
       (drop (:offset opts))
     (:limit opts)
       (take (:limit opts)))))


(defn- -get-table
  "Internal `get-table` implementation."
  [^Database db table-name]
  (when-let [value (get (.tables db) table-name)]
    (if (mdag/link? value)
      ; Resolve link to stored table root.
      (table/load-table (.store db) table-name value)
      ; Loaded table value.
      value)))


(defn- -set-table
  "Internal `set-table` implementation."
  [^Database db table-name value]
  (let [table (if (table/dirty? value)
                (table/set-backing value (.store db) table-name)
                (table-link table-name value))]
    (->Database
      (.store db)
      (.db-info db)
      (.root-data db)
      (assoc (.tables db) table-name table)
      (._meta db))))


(defn- -drop-table
  "Internal `drop-table` implementation."
  [^Database db table-name]
  (->Database
    (.store db)
    (.db-info db)
    (.root-data db)
    (dissoc (.tables db) table-name)
    (._meta db)))


(defn- flush-tables!
  "Takes a map of tables and flushes any pending changes. Returns a map of
  table names to node links."
  [tables]
  (reduce-kv
    (fn [acc table-name value]
      (assoc
        acc table-name
        (if (mdag/link? value)
          value
          (table-link table-name (table/flush! value false)))))
    {} tables))


(defn- -flush!
  "Internal `flush!` implementation."
  [^Database db]
  (let [tables (flush-tables! (.tables db))
        root-data (assoc (.root-data db)
                         :data/type data-type
                         ::tables tables)]
    (when-not (s/valid? ::node-data root-data)
      (throw (ex-info (str "Cannot write invalid database root node: "
                           (s/explain-str ::node-data root-data))
                      {:type ::invalid-node})))
    (let [node (mdag/store-node! (.store db) nil root-data)
          db-info (assoc (.db-info db)
                         ::node/id (::node/id node)
                         ::record/size (node/reachable-size node))
          db-meta (assoc (._meta db)
                         ::node/links (::node/links node))]
      (->Database
        (.store db)
        db-info
        (dissoc (::node/data node) ::tables)
        tables
        db-meta))))


(extend Database

  IDatabase

  {:list-tables -list-tables
   :get-table -get-table
   :set-table -set-table
   :drop-table -drop-table
   :flush! -flush!})
