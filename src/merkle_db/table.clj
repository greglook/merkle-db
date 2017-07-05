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
    - `:start-key`
      Return records with keys equal to or greater than the marker.
    - `:end-key`
      Return records with keys equal to or less than the marker.
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

  (insert
    [table records opts]
    "Insert some record data into the database, represented by a collection
    of pairs of record key values and field maps.

    Options may include:

    - `:merge-field`
      A function which will be called with `(f field-key old-val new-val)`, and
      should return the new value to use for that field. By default, `new-val`
      is used directly.
    - `:merge-record`
      A function which will be called with `(f record-key old-data new-data)`,
      and should return the data map to use for the record. By default, this
      merges the data maps and removes nil-valued fields.")

  (delete
    [table id-keys]
    "Remove some records from the table, identified by a collection of id keys.
    Returns an updated table.")

  ;; Partitions

  (list-partitions
    [table]
    "List the partitions which comprise the table. Returns a sequence of
    partition nodes.")

  (read-partition
    [table partition-id opts]
    "Read all the records in the identified partition."))



;; ## Utility Functions

; (merge-patch patch-tablet patch-data) => patch-tablet
; (apply-patch data-tree patch-data) => data-link



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
    ; TODO: something different with ::data or ::patch?
    (if (contains? info-keys k)
      (get table-info k not-found)
      ; TODO: link-expand value
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
      {::data/count 0}
      (empty patch-data)
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
          (Table.
            store
            (dissoc table-info ::node/id)
            root'
            patch-data
            true
            _meta)))))


  (without
    [this k]
    (if (contains? info-keys k)
      (throw (IllegalArgumentException.
               (str "Cannot remove table info field " k)))
      (let [root' (dissoc root-data k)]
        (if (= root-data root')
          this
          (Table.
            store
            (dissoc table-info ::node/id)
            root'
            patch-data
            true
            _meta))))))


(alter-meta! #'->Table assoc :private true)


(defn ^:no-doc bare-table
  "Build a new ephemeral table value without a backing store."
  [store table-name opts]
  (->Table
    store
    {::name table-name}
    (merge
      {::index/branching-factor index/default-branching-factor
       ::part/limit part/default-limit}
      (dissoc opts ::data ::patch)
      {::data/count 0})
    (sorted-map-by key/compare)
    true
    nil))


(defn- update-patch
  "Returns a new `Table` value with the given function applied to update its
  patch data."
  [^Table table f & args]
  (let [patch' (apply f (.patch-data table) args)]
    (if (= patch' (.patch-data table))
      table
      (->Table
        (.store table)
        (dissoc (.table-info table) ::node/id)
        (.root-data table)
        patch'
        true
        (._meta table)))))


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

; TODO: put this somewhere else
(def tombstone ::tombstone)


; TODO: put this somewhere else
(defn- tombstone?
  "Returns true if the value x is a tombstone."
  [x]
  (identical? ::tombstone x))


(defn- remove-tombstones
  "Returns a lazy sequence with tombstoned records removed."
  [rs]
  (remove (comp tombstone? second) rs))


(defn- prepare-patch
  "Takes a full set of patch data and returns a cleaned sequence based on the
  given options."
  [patch opts]
  (when (seq patch)
    (cond->> patch
      (:start-key opts)
        (drop-while #(neg? (key/compare (first %) (:start-key opts))))
      (:end-key opts)
        (take-while #(not (neg? (key/compare (:end-key opts) (first %)))))
      (:fields opts)
        (map (fn [[k r]]
               [k (if (map? r) (select-keys r (:fields opts)) r)])))))


(defn- patch-seq
  "Combines an ordered sequence of patch data with a lazy sequence of record
  keys and data. Any records present in the patch will appear in the output
  sequence, replacing any equivalent keys from the sequence."
  [patch records]
  (lazy-seq
    (cond
      ; No more patch data, return records directly.
      (empty? patch)
        records
      ; No more records, return patch with tombstones removed.
      (empty? records)
        (remove (comp tombstone? second) patch)
      ; Next key is in both patch and records.
      (= (ffirst patch) (ffirst records))
        (cons (first patch) (patch-seq (next patch) (next records)))
      ; Next key is in patch, not in records.
      (key/before? (ffirst patch) (ffirst records))
        (cons (first patch) (patch-seq (next patch) records))
      ; Next key is in records, not in patch.
      :else
        (cons (first records) (patch-seq patch (next records))))))


(extend-type Table

  ITable

  ;; Records

  (scan
    [^Table this opts]
    ; TODO: apply lexicoder
    (->>
      (patch-seq
        ; Merged patch data to apply to the records.
        (prepare-patch (.patch-data this) opts) ; TODO: merge ::patch
        ; Lazy sequence of matching records from the index tree.
        (when-let [data-node (mdag/get-data
                               (.store this)
                               (::data (.node-data this)))]
          (if (or (:start-key opts) (:end-key opts))
            (index/read-range
              (.store this)
              data-node
              (:fields opts)
              (:start-key opts)
              (:end-key opts))
            (index/read-all
              (.store this)
              data-node
              (:fields opts)))))
      (remove-tombstones)))


  (get-records
    [^Table this id-keys opts]
    ; TODO: apply lexicoder
    (let [id-keys (set id-keys)
          patch-map (.patch-data this) ; TODO: merge ::patch
          patch-entries (select-keys patch-map id-keys)
          extra-keys (apply disj id-keys (keys patch-entries))
          patch-entries (cond->> (remove-tombstones patch-entries)
                          (seq (:fields opts))
                            (map (fn [[k r]] [k (select-keys r (:fields opts))])))
          data-entries (when-let [data-node (and (seq extra-keys)
                                                 (mdag/get-data
                                                   (.store this)
                                                   (::data (.node-data this))))]
                         (index/read-batch
                           (.store this)
                           data-node
                           (:fields opts)
                           extra-keys))]
      (concat patch-entries data-entries)))


  (insert
    [this records opts]
    ; TODO: apply lexicoder
    (let [{:keys [merge-record merge-field]} opts
          update-record (cond
                          merge-record merge-record
                          merge-field
                          (fn merge-fields
                            [_ old-data new-data]
                            (reduce
                              (fn [data field-key]
                                (let [])
                                (assoc data
                                       field-key
                                       (merge-field field-key
                                                    (get old-data field-key)
                                                    (get new-data field-key))))
                              {} (distinct (concat (keys old-data) (keys new-data)))))
                          :else
                          (fn merge-simple
                            [_ old-data new-data]
                            (into {}
                                  (filter (comp some? val))
                                  (merge old-data new-data))))
          extant (into {} (get-records this (map first records) nil))
          new-records (map
                        (fn [[k data]]
                          [k (update-record k (get extant k) data)])
                        records)]
      ; Add new data maps to patch-data.
      (update-patch this into new-records)))


  (delete
    [this id-keys]
    ; TODO: apply lexicoder
    ; Add tombstones to patch-data.
    (update-patch this into (map vector id-keys (repeat tombstone)))))
