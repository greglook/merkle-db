(ns merkle-db.db
  (:require
    [clojure.future :refer [inst? nat-int?]]
    [clojure.spec :as s]
    [merkledag.core :as mdag]
    [merkledag.link :as link]
    [merkledag.node :as node]
    [merkle-db.data :as data]
    [merkle-db.table :as table]
    [multihash.core :as multihash]))


;; ## Specs

;; Database name.
(s/def ::name (s/and string? #(<= 1 (count %) 512)))

;; Database version.
(s/def ::version nat-int?)

;; When the database version was committed.
(s/def ::committed-at inst?)

;; Map of table names to node links.
(s/def ::tables (s/map-of ::table/name link/merkle-link?))

;; Description of a specific version of a database.
(s/def ::version-info
  (s/keys :req [::node/id  ; TODO: just ::root-id or ::root ?
                ::name
                ::version
                ::committed-at]))

;; Database root node.
(s/def ::root-node
  (s/keys :req [::tables]
          :opt [:time/updated-at]))

;; Description of the database.
(s/def ::description
  (s/merge ::version-info
           ::root-node))



;; ## Database Protocols

(defprotocol ITables
  "Protocol for interacting with a database at a specific version."

  (list-tables
    [db opts]
    "Return a sequence of maps about the tables in the database, including
    their name and total size.")

  (create-table
    [db table-name opts]
    "...")

  (describe-table
    [db table-name]
    "...")

  (alter-table
    [db table-name f]
    "...")

  (drop-table
    [db table-name]
    "..."))


(defprotocol IRecords
  "..."

  (scan
    [db table-name opts]
    "...")

  (get-records
    [db table-name primary-keys opts]
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
    [db partition-id opts]
    "...")

  (build-table
    [db table-name partition-ids]
    "..."))



;; ## Database Type

;; Databases are implementad as a custom type so they can behave similarly to
;; natural Clojure values.
;;
;; - `store` reference to the merkledag node store backing the database.
;; - `tables` map of table names to reified table objects.
;; - `root-data` map of data stored in the database root with `::tables`
;;   removed.
;; - `version-info` data about the reference version the database was checked
;;   out as.
(deftype Database
  [store
   tables
   root-data
   version-info
   _meta]

  Object

  (toString
    [this]
    (format "db:%s:%d %s"
            (::name version-info "?")
            (::version version-info "?")
            (hash root-data)))


  (equals
    [this that]
    (boolean
      (or (identical? this that)
          (when (identical? (class this) (class that))
            (let [that ^Database that]
              (and (= tables (.tables that))
                   (= root-data (.root-data that))))))))


  (hashCode
    [this]
    (hash [tables root-data]))


  clojure.lang.IObj

  (meta
    [this]
    _meta)


  (withMeta
    [this meta-map]
    (Database. store tables root-data version-info meta-map))


  clojure.lang.ILookup

  (valAt
    [this k]
    (.valAt this k nil))


  (valAt
    [this k not-found]
    (cond
      (= ::tables k) tables
      (contains? version-info k) (get version-info k)
      ; TODO: if val here is a link, auto-resolve it
      :else (get root-data k not-found)))


  clojure.lang.IPersistentMap

  (count
    [this]
    (+ 1 (count root-data) (count version-info)))


  (empty
    [this]
    (Database. store tables nil version-info _meta))


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
    (not (identical? this (.valAt this k this))))


  (entryAt
    [this k]
    (let [v (.valAt this k this)]
      (when-not (identical? this v)
        (clojure.lang.MapEntry. k v))))


  (seq
    [this]
    (seq (concat [(clojure.lang.MapEntry. ::tables tables)]
                 (seq version-info)
                 (seq root-data))))


  (iterator
    [this]
    (clojure.lang.RT/iter (seq this)))


  (assoc
    [this k v]
    (cond
      (= k ::tables)
        (throw (RuntimeException. "NYI"))
      (contains? version-info k)
        (throw (IllegalArgumentException.
                 (str "Cannot change database version-info field " k)))
      :else
        (Database. store tables (assoc root-data k v) version-info _meta)))


  (without
    [this k]
    (cond
      (= k ::tables)
        (throw (IllegalArgumentException.
                 (str "Cannot remove database tables field " k)))
      (contains? version-info k)
        (throw (IllegalArgumentException.
                 (str "Cannot remove database version-info field " k)))
      :else
        (Database. store tables (not-empty (dissoc root-data k)) version-info _meta)))


  ITables

  (list-tables
    [this opts]
    (map (fn [[table-name link]]
           {::node/id (::link/target link)
            ::table/name table-name
            ::data/size (::link/rsize link)})
         tables))


  (create-table
    [this table-name opts]
    (when (get tables table-name)
      (throw (ex-info (format "Cannot create table: already a table named %s"
                              (pr-str table-name))
                      {:type ::table-name-conflict
                       :name table-name})))
    (let [data (table/root-data opts)
          ; TODO: validate spec
          node (mdag/store-node! store nil data)
          link (mdag/link (str "table:" table-name) node)]
      (Database. store (assoc tables table-name link) root-data version-info _meta)))


  (describe-table
    [this table-name]
    (when-let [link (get tables table-name)]
      ; TODO: wrap in custom data type?
      (let [node (mdag/get-node store link)]
        (assoc (::node/data node)
               ::table/name table-name
               ::node/id (::link/target link)
               ::data/size (::link/rsize link)))))


  (alter-table
    [this table-name f]
    ; TODO: validate spec
    (if-let [table (some->> (get tables table-name)
                            (mdag/get-data store))]
      ; Update table data.
      (let [table' (f table) ; TODO: set :time/updated-at?
            table-node (mdag/store-node! store nil table') ; TODO: should re-use or update links from previous node 
            link (mdag/link (str "table:" table-name) table-node)]
        (Database. store (assoc tables table-name link) root-data version-info _meta))
      ; Couldn't find table.
      (throw (ex-info (str "Database has no table named: "
                           (pr-str table-name))
                      {:type ::no-such-table}))))


  (drop-table
    [this table-name]
    (Database. store (dissoc tables table-name) root-data version-info _meta)))


(alter-meta! #'->Database assoc :private true)


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
