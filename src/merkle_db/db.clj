(ns merkle-db.db
  "Core database functions."
  (:require
    [clojure.future :refer [inst? nat-int?]]
    [clojure.spec :as s]
    [merkledag.core :as mdag]
    [merkledag.link :as link]
    [merkledag.node :as node]
    [merkle-db.data :as data]
    [merkle-db.table :as table]
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

(def info-keys
  #{::node/id ::name ::version ::committed-at})

;; Information from the database version.
(s/def ::version-info
  (s/keys :req [::node/id
                ::name
                ::version
                ::committed-at]))

;; Description of the database.
(s/def ::description
  (s/merge ::node-data ::version-info))



;; ## Database API

(defprotocol IDatabase
  "Protocol for interacting with a database at a specific version."

  (list-tables
    [db opts]
    "Return a sequence of maps with partial information about the tables in the
    database, including their name and total size.

    Options may include:

    ...")

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
  ; TODO: review how this works
  (let [table (Table. nil
                      {::table/name table-name}
                      (table/root-data opts)
                      nil
                      true
                      nil)]
    (set-table db table-name table)))


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



;; ## Database Type

;; Databases are implementad as a custom type so they can behave similarly to
;; natural Clojure values.
;;
;; - `store` reference to the merkledag node store backing the database.
;; - `version-info` data from the reference version the database was opened at.
;; - `root-data` map of data stored in the database root, excluding `::tables`.
;; - `tables` map of table names to reified table objects.
(deftype Database
  [store
   version-info
   root-data
   tables
   _meta]

  Object

  (toString
    [this]
    (format "db:%s:%s"
            (::name version-info "?")
            (::version version-info "?")))


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
    (Database. store version-info root-data tables meta-map))


  clojure.lang.ILookup

  (valAt
    [this k]
    (.valAt this k nil))


  (valAt
    [this k not-found]
    (cond
      (= ::tables k) tables ; TODO: better return val
      (contains? info-keys k) (get version-info k not-found)
      ; TODO: if val here is a link, auto-resolve it
      :else (get root-data k not-found)))


  clojure.lang.IPersistentMap

  (count
    [this]
    (+ (count root-data) (count version-info) 1))


  (empty
    [this]
    (Database. store version-info nil tables _meta))


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
        (contains? version-info k)
        (contains? root-data k)))


  (entryAt
    [this k]
    (let [v (.valAt this k this)]
      (when-not (identical? this v)
        (clojure.lang.MapEntry. k v))))


  (seq
    [this]
    (seq (concat (seq version-info)
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
                 (str "Cannot change database version-info field " k)))
      :else
        (Database. store version-info (assoc root-data k v) tables _meta)))


  (without
    [this k]
    (cond
      (= k ::tables)
        (throw (IllegalArgumentException.
                 (str "Cannot remove database tables field " k)))
      (contains? info-keys k)
        (throw (IllegalArgumentException.
                 (str "Cannot remove database version-info field " k)))
      :else
        (Database.
          store
          tables
          (not-empty (dissoc root-data k))
          version-info
          _meta))))


(alter-meta! #'->Database assoc :private true)


; TODO: constructor functions



;; ## Protocol Implementation

(extend-type Database

  IDatabase

  (list-tables
    [this opts]
    ; TODO: figure out useful options
    (map (fn link->info
           [[table-name value]]
           (if (link/merkle-link? value)
             {::node/id (::link/target value)
              ::table/name table-name
              ::data/size (::link/rsize value)}
             (select-keys
               value
               [::node/id ::table/name ::data/size])))
         (.tables this)))


  (get-table
    [this table-name]
    (when-let [value (get (.tables this) table-name)]
      (if (link/merkle-link? value)
        ; Resolve link to stored table root.
        (let [node (mdag/get-node (.store this) value)]
          (Table.
            (.store this)
            {::table/name table-name
             ::node/id (::link/target value)
             ::data/size (::link/rsize value)}
            (::node/data node)
            nil
            false
            {::node/links (::node/links node)}))
        ; Dirty table value.
        value)))


  (set-table
    [this table-name ^Table value]
    (when-not (s/valid? ::table/node-data (.root-data value))
      (throw (ex-info
               "Updated table is not valid"
               {:type ::invalid-table
                :errors (s/explain-data ::table/node-data
                                        (.root-data value))})))
    (let [table (if (.dirty? value)
                  (Table.
                    (.store this)
                    {::table/name table-name}
                    (.root-data value)
                    (.patch-data value)
                    true
                    (._meta value))
                  (link/create
                    (str "table:" table-name)
                    (::node/id value)
                    (::data/size value)))]
      (Database.
        (.store this)
        (assoc (.tables this) table-name table)
        (.root-data this)
        (.version-info this)
        (._meta this))))


  (drop-table
    [this table-name]
    (Database.
      (.store this)
      (dissoc (.tables this) table-name)
      (.root-data this)
      (.version-info this)
      (._meta this))))


(defn load-database
  "Load a database from the store, using the version information given."
  [store version-info]
  (let [root-data (mdag/get-data store (::node/id version-info))]
    (->Database store
                (::tables root-data)
                (dissoc root-data ::tables)
                version-info
                nil)))


(defn ^:no-doc update-backing
  "Update the database to use the given version information."
  [^Database db store version-info]
  (->Database store
              (.tables db)
              (.root-data db)
              version-info
              (meta db)))
