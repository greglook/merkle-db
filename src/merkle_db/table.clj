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

(def data-type
  "Value of `:data/type` that indicates a table root node."
  :merkle-db/table)

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
    [table]
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
    [table id-keys]
    [table id-keys opts]
    "Read a set of records from the database, returning data for each present
    record.

    Options may include:

    - `:fields`
      Only return data for the selected set of fields. If not provided, all
      fields are returned.")

  (insert
    [table records]
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

; (merge-patch patch-tablet pending) => patch-tablet
; (apply-patch data-tree pending) => data-link



;; ## Table Type

;; Tables are implementad as a custom type so they can behave similarly to
;; natural Clojure values.
;;
;; - `store` reference to the merkledag node store backing the database.
;; - `table-info` map of higher-level table properties such as table-name,
;;   node-id, and recursive size.
;; - `root-data` map of data stored in the table root.
;; - `pending` sorted map of keys to pending record updates.
;; - `dirty` flag to indicate whether the table data has been changed since
;;   the node was loaded.
(deftype Table
  [store
   table-info
   root-data
   pending
   dirty
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
                 (= pending (.pending ^Table that)))))))


  (hashCode
    [this]
    (hash-combine (hash root-data) (hash pending)))


  clojure.lang.IObj

  (meta
    [this]
    _meta)


  (withMeta
    [this meta-map]
    (Table. store table-info root-data pending dirty meta-map))


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
      (empty pending)
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
    (when (= k :data/type)
      (throw (IllegalArgumentException.
               (str "Cannot change table :data/type from " data-type))))
    (when (contains? info-keys k)
      (throw (IllegalArgumentException.
               (str "Cannot change table info field " k))))
    (let [root' (assoc root-data k v)]
      (if (= root-data root')
        this
        (Table.
          store
          (dissoc table-info ::node/id)
          root'
          pending
          true
          _meta))))


  (without
    [this k]
    (when (= k :data/type)
      (throw (IllegalArgumentException.
               "Cannot remove table :data/type")))
    (when (contains? info-keys k)
      (throw (IllegalArgumentException.
               (str "Cannot remove table info field " k))))
    (let [root' (dissoc root-data k)]
      (if (= root-data root')
        this
        (Table.
          store
          (dissoc table-info ::node/id)
          root'
          pending
          true
          _meta)))))


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


(defn ^:no-doc load-table
  "Load a table node from the store."
  [store table-name target]
  (let [node (mdag/get-node store target)]
    (->Table
      store
      {::name table-name
       ::node/id (::node/id node)
       ::data/size (node/reachable-size node)}
      (::node/data node)
      (sorted-map-by key/compare)
      false
      {::node/links (::node/links node)})))


(defn ^:no-doc set-backing
  "Change the backing store and info for a table."
  [^Table table store table-name]
  (->Table
    store
    {::name table-name}
    (.root-data table)
    (.pending table)
    (.dirty table)
    (._meta table)))


(defn- update-pending
  "Returns a new `Table` value with the given function applied to update its
  pending data."
  [^Table table f & args]
  (let [patch' (apply f (.pending table) args)]
    (if (= patch' (.pending table))
      table
      (->Table
        (.store table)
        (dissoc (.table-info table) ::node/id)
        (.root-data table)
        patch'
        true
        (._meta table)))))


; TODO: put this in protocol?
(defn dirty?
  "Return true if the table has local non-persisted modifications."
  [^Table table]
  (.dirty table))


; TODO: put this in protocol?
(defn flush!
  "Ensure that all local state has been persisted to the storage backend and
  return an updated non-dirty table."
  ([table]
   (flush! table false))
  ([^Table table apply-patches?]
   (if (dirty? table)
     (let [[patch-link data-link]
             (if (or (seq? (.pending table)) (::patch table))
               (if apply-patches?
                 ; Combine pending changes and patch tablet and update data tree.
                 [nil (throw (UnsupportedOperationException. "updated data-tree link"))]
                 ; flush any pending changes to the patch tablet
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


(defn- table-lexicoder
  "Construct a lexicoder for the keys in a table."
  [table]
  (key/lexicoder (::key/lexicoder table :bytes)))


(defn- key-decoder
  "Return a function which will decode the keys in record entries with the
  given lexicoder."
  [lexicoder]
  (fn [[k r]] [(key/decode lexicoder k) r]))


(defn- -scan
  [^Table table opts]
  (let [lexicoder (table-lexicoder table)
        start-key (some->> (:start-key opts) (key/encode lexicoder))
        end-key (some->> (:end-key opts) (key/encode lexicoder))]
    (->>
      (patch-seq
        ; Merged patch data to apply to the records.
        (prepare-patch
          (.pending table) ; TODO: merge ::patch
          {:fields (:fields opts)
           :start-key start-key
           :end-key end-key})
        ; Lazy sequence of matching records from the index tree.
        (when-let [data-node (mdag/get-data
                               (.store table)
                               (::data (.root-data table)))]
          (if (or start-key end-key)
            (index/read-range
              (.store table)
              data-node
              (:fields opts)
              start-key
              end-key)
            (index/read-all
              (.store table)
              data-node
              (:fields opts)))))
      (remove-tombstones)
      (map (key-decoder lexicoder)))))


(defn- -get-records
  [^Table table id-keys opts]
  (let [lexicoder (table-lexicoder table)
        id-keys (into #{} (map (partial key/encode lexicoder)) id-keys)
        patch-map (.pending table) ; TODO: merge ::patch
        patch-entries (select-keys patch-map id-keys)
        extra-keys (apply disj id-keys (keys patch-entries))
        patch-entries (cond->> (remove-tombstones patch-entries)
                        (seq (:fields opts))
                          (map (fn [[k r]] [k (select-keys r (:fields opts))])))
        data-entries (when-let [data-node (and (seq extra-keys)
                                               (mdag/get-data
                                                 (.store table)
                                                 (::data (.root-data table))))]
                       (index/read-batch
                         (.store table)
                         data-node
                         (:fields opts)
                         extra-keys))]
    (->> (concat patch-entries data-entries)
         (map (key-decoder lexicoder)))))


(defn- -insert
  [table records opts]
  (let [{:keys [merge-record merge-field]} opts
        lexicoder (table-lexicoder table)
        update-record (cond
                        merge-record merge-record
                        merge-field
                        (fn merge-fields
                          [_ old-data new-data]
                          (reduce
                            (fn [data field-key]
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
        extant (into {} (get-records table (map first records) nil))
        new-records (map
                      (fn [[k data]]
                        [(key/encode lexicoder k)
                         (update-record k (get extant k) data)])
                      records)]
    ; Add new data maps to pending changes.
    ; TODO: adjust ::data/count based on extant keys
    (update-pending table into new-records)))


(defn- -delete
  [table id-keys]
  (let [lexicoder (key/lexicoder (::key/lexicoder table :bytes))]
    ; TODO: need to look up whether the keys exist;
    ; don't add tombstones for absent keys, adjust ::data/count
    (update-pending
      table into
      (map (fn [k] [(key/encode lexicoder k) tombstone])
           id-keys))))


(extend-type Table

  ITable

  ;; Records

  (scan
    ([this]
     (-scan this nil))
    ([this opts]
     (-scan this opts)))


  (get-records
    ([this id-keys]
     (-get-records this id-keys nil))
    ([this id-keys opts]
     (-get-records this id-keys opts)))


  (insert
    ([this records]
     (-insert this records nil))
    ([this records opts]
     (-insert this records opts)))


  (delete
    [this id-keys]
    (-delete this id-keys)))
