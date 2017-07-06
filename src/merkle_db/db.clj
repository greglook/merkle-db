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
      [data :as data]
      [table :as table])
    [multihash.core :as multihash])
  (:import
    java.time.Instant
    merkle_db.table.Table))


;; ## Data Specifications

;; Database name.
;; TODO: disallow certain characters like '/'
(s/def ::name (s/and string? #(<= 1 (count %) 512)))

;; Database version.
(s/def ::version nat-int?)

;; When the database version was committed.
(s/def ::committed-at inst?)

;; Map of table names to node links.
(s/def ::tables (s/map-of ::table/name link/merkle-link?))

;; Database root node.
(s/def ::node-data
  (s/keys :req [::tables]
          :opt [:time/updated-at]))

(def ^:no-doc info-keys
  "Set of keys which may appear in the database info map."
  #{::node/id ::name ::version ::committed-at ::data/size})

;; Information from the database version.
(s/def ::db-info
  (s/keys :req [::node/id
                ::name
                ::version
                ::committed-at]
          :opt [::data/size]))



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
  "Create a new table with the given name and options. Returns an updated
  database value, or throws an exception if the table already exists."
  [db table-name opts]
  (when (get-table db table-name)
    (throw (ex-info
             (str "Cannot create table: database already has a table named "
                  table-name)
             {:type ::table-conflict
              :table-name table-name})))
  (set-table db table-name (table/bare-table nil table-name opts)))


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


(defn- link-or-table->info
  "Coerce either a link value or a table record to a map of information."
  [[table-name value]]
  (if (link/merkle-link? value)
    {::node/id (::link/target value)
     ::table/name table-name
     ::data/size (::link/rsize value)}
    (select-keys
      value
      [::node/id ::table/name ::data/size])))



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
             ::data/size (node/reachable-size node))
      (dissoc (::node/data node) :data/type ::tables)
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

(extend-type Database

  IDatabase

  (list-tables
    ([this]
     (list-tables this nil))
    ([this opts]
     (cond->> (map link-or-table->info (.tables this))
       (:named opts)
         (filter #(if (string? (:named opts))
                    (str/starts-with? (::table/name %) (:named opts))
                    (re-seq (:named opts) (::table/name %))))
       (:offset opts)
         (drop (:offset opts))
       (:limit opts)
         (take (:limit opts)))))


  (get-table
    [this table-name]
    (when-let [value (get (.tables this) table-name)]
      (if (link/merkle-link? value)
        ; Resolve link to stored table root.
        (table/load-table (.store this) table-name value)
        ; Dirty table value.
        value)))


  (set-table
    [this table-name value]
    (when-not (s/valid? ::table/node-data (.root-data value))
      (throw (ex-info
               "Updated table is not valid"
               {:type ::invalid-table
                :errors (s/explain-data ::table/node-data
                                        (.root-data value))})))
    (let [table (if (table/dirty? value)
                  (table/set-backing
                    value
                    (.store this)
                    table-name)
                  (link/create
                    (str "table:" table-name)
                    (::node/id value)
                    (::data/size value)))]
      (->Database
        (.store this)
        (.db-info this)
        (.root-data this)
        (assoc (.tables this) table-name table)
        (._meta this))))


  (drop-table
    [this table-name]
    (->Database
      (.store this)
      (.db-info this)
      (.root-data this)
      (dissoc (.tables this) table-name)
      (._meta this)))


  (flush!
    [this]
    (let [tables (reduce-kv
                   (fn [acc table-name value]
                     (assoc
                       acc table-name
                       (if (link/merkle-link? value)
                         value
                         (let [table (table/flush! value false)]
                           (link/create
                             (str "table:" table-name)
                             (::node/id table)
                             (::data/size table))))))
                   {} (.tables this))
          node (mdag/store-node!
                 (.store this)
                 nil
                 (assoc (.root-data this)
                        :data/type :merkle-db/db
                        ::tables tables))]
      (->Database
        (.store this)
        (assoc (.db-info this)
               ::node/id (::node/id node)
               ::data/size (node/reachable-size node))
        (dissoc (::node/data node)
                :data/type
                ::tables)
        tables
        (assoc (._meta this)
               ::node/links (::node/links node))))))
