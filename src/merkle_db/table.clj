(ns merkle-db.table
  "Tables are named top-level containers of records. Generally, a single table
  corresponds to a certain 'kind' of data. Tables also contain configuration
  determining how keys are encoded and records are stored."
  (:require
    [clojure.future :refer [pos-int?]]
    [clojure.spec :as s]
    (merkledag
      [core :as mdag]
      [link :as link]
      [node :as node])
    (merkle-db
      [data :as data]
      [index :as index]
      [key :as key]
      [partition :as part]))
  (:import
    java.time.Instant))


;; ## Data Specifications

;; Table names are non-empty strings.
;; TODO: disallow certain characters like '/'
(s/def ::name (s/and string? #(<= 1 (count %) 127)))

;; Table data is a link to the root of the data tree.
(s/def ::data link/merkle-link?)

;; Tables may have a patch tablet containing recent unmerged data.
(s/def ::patch link/merkle-link?)

;; Table root node.
(s/def ::node-data
  (s/keys :req [::data/count
                ::index/branching-factor
                ::part/limit]
          :opt [::data
                ::patch
                ::data/families
                ::key/lexicoder
                :time/updated-at]))

(def ^:no-doc info-keys
  "Set of keys which may appear in the table info map."
  #{::node/id ::name ::data/size})

(s/def ::table-info
  (s/keys :req [::node/id
                ::name
                ::version
                ::committed-at]))



;; ## Table API

(defprotocol ITable
  "Protocol for an immutable table of record data."

  ;; Records

  (scan
    [table opts]
    "Scan the table, returning data from records which match the given options.
    Returns a lazy sequence of vectors which each hold a record key and a map
    of the record data.

    If start and end keys or indices are given, only records within the
    bounds will be returned (inclusive). A nil start or end implies the beginning
    or end of the data, respectively.

    Options may include:

    - `:fields`
      Only return data for the selected set of fields. If not provided, all
      fields are returned.
    - `:from-key`
      Return records with keys equal to or greater than the marker.
    - `:to-key`
      Return records with keys equal to or less than the marker.
    - `:from-index`
      Return records with indexes equal to or greater than the marker.
    - `:to-index`
      Return records with indexes equal to or less than the marker.
    - `:offset`
      Skip this many records in the output.
    - `:limit`
      Return at most this many records.")

  (get-records
    [table id-keys opts]
    "Read a set of records from the database, returning data for each present
    record.

    Options may include:

    - `:fields`
      Only return data for the selected set of fields. If not provided, all
      fields are returned.")

  (write
    [table records opts]
    "Write a collection of records to the database, represented as a map of
    record key values to record data maps.")

  (delete
    [table id-keys]
    "Remove a set of records from the table, identified by a collection of
    id keys. Returns an updated table.")

  ;; Partitions

  (list-partitions
    [table]
    "List the partitions which comprise the table. Returns a sequence of
    partition nodes.")

  (read-partition
    [table partition-id opts]
    "Read all the records in the identified partition."))



;; ## Utility Functions

(defn root-data
  "Construct a map for a new table root node."
  [opts]
  (merge {::index/branching-factor 256
          ::part/limit 100000}
          opts
          {::data/count 0
           :time/updated-at (Instant/now)}))



;; ## Table Type

;; Tables are implementad as a custom type so they can behave similarly to
;; natural Clojure values.
;;
;; - `store` reference to the merkledag node store backing the database.
;; - `table-info` map of higher-level table properties such as table-name,
;;   node-id, and recursive size.
;; - `root-data` map of data stored in the table root.
;; - `patch-data` sorted map of loaded patch record data.
;; - `dirty?` flag to indicate whether the table data has been changed since
;;   the node was loaded.
(deftype Table
  [store
   table-info
   root-data
   patch-data
   dirty?
   _meta]

  Object

  (toString
    [this]
    (format "table:%s %s"
            (::name table-info "?")
            (hash root-data)))


  (equals
    [this that]
    (boolean
      (or (identical? this that)
          (when (identical? (class this) (class that))
            (and (= root-data (.root-data ^Table that))
                 (= patch-data (.patch-data ^Table that)))))))


  (hashCode
    [this]
    (hash-combine (hash root-data) (hash patch-data)))


  clojure.lang.IObj

  (meta
    [this]
    _meta)


  (withMeta
    [this meta-map]
    (Table. store table-info root-data patch-data dirty? meta-map))


  clojure.lang.ILookup

  (valAt
    [this k]
    (.valAt this k nil))


  (valAt
    [this k not-found]
    (if (contains? info-keys k)
      (get table-info k not-found)
      ; TODO: link-expand value if it's not ::data or ::patch
      (get root-data k not-found)))


  clojure.lang.IPersistentMap

  (count
    [this]
    (+ (count root-data) (count table-info)))


  (empty
    [this]
    (Table.
      store
      (dissoc table-info ::node/id)
      (root-data nil)
      nil
      true
      _meta))


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
    (seq (concat (seq table-info) (seq root-data))))


  (iterator
    [this]
    (clojure.lang.RT/iter (seq this)))


  (assoc
    [this k v]
    (if (contains? info-keys k)
      (throw (IllegalArgumentException.
               (str "Cannot change table info field " k)))
      (let [root' (assoc root-data k v)]
        (if (= root-data root')
          this
          (Table. store table-info root' patch-data true _meta)))))


  (without
    [this k]
    (if (contains? info-keys k)
      (throw (IllegalArgumentException.
               (str "Cannot remove table info field " k)))
      (let [root' (not-empty (dissoc root-data k))]
        (if (= root-data root')
          this
          (Table. store table-info root' patch-data true _meta))))))


(alter-meta! #'->Table assoc :private true)


; TODO: constructor functions


; TODO: put this in protocol?
(defn flush!
  "Ensure that all local state has been persisted to the storage backend and
  return an updated non-dirty table."
  ([table]
   (flush! table false))
  ([^Table table apply-patches?]
   (if (.dirty? table)
     (let [[patch-link data-link]
             (if (or (seq? (.patch-data table)) (::patch table))
               (if apply-patches?
                 ; Combine patch-data and patch tablet and update data tree.
                 [nil (throw (UnsupportedOperationException. "updated data-tree link"))]
                 ; flush any patch-data to the patch tablet
                 ; if patch tablet overflows, update main data tree
                 (throw (UnsupportedOperationException. "NYI")))
               ; No patch data or tablet.
               [nil (::data table)])
           node (mdag/store-node!
                  (.store table)
                  nil
                  (-> (.root-data table)
                      (assoc :data/type :merkle-db/table)
                      (dissoc ::patch ::data)
                      (cond->
                        patch-link (assoc ::patch patch-link)
                        data-link (assoc ::data data-link))))]
       (->Table
         (.store table)
         (assoc (.table-info table)
                ::node/id (::node/id node)
                ::data/size (node/reachable-size node))
         (dissoc (::node/data node) :data/type)
         nil
         false
         (._meta table)))
     ; Table is clean, return directly.
     table)))



;; ## Protocol Implementation

(extend-type Table

  ITable

  ;; Records

  (scan
    [this opts]
    (throw (UnsupportedOperationException. "NYI")))


  (get-records
    [this primary-keys opts]
    (throw (UnsupportedOperationException. "NYI")))


  (write
    [this records]
    (throw (UnsupportedOperationException. "NYI")))


  (delete
    [this primary-keys]
    (throw (UnsupportedOperationException. "NYI")))


  ;; Partitions

  (list-partitions
    [this]
    (throw (UnsupportedOperationException. "NYI")))


  (read-partition
    [this partition-id opts]
    (throw (UnsupportedOperationException. "NYI"))))
