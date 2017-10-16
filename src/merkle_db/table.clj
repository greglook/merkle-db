(ns merkle-db.table
  "Tables are named top-level containers of records. Generally, a single table
  corresponds to a certain 'kind' of data. Tables also contain configuration
  determining how keys are encoded and records are stored."
  (:refer-clojure :exclude [keys read])
  (:require
    [clojure.future :refer [nat-int? pos-int?]]
    [clojure.spec :as s]
    [clojure.string :as str]
    [merkledag.core :as mdag]
    [merkledag.node :as node]
    [merkle-db.index :as index]
    [merkle-db.key :as key]
    [merkle-db.partition :as part]
    [merkle-db.patch :as patch]
    [merkle-db.record :as record])
  (:import
    java.time.Instant))


;; ## Data Specifications

(def ^:const data-type
  "Value of `:data/type` that indicates a table root node."
  :merkle-db/table)

(def ^:no-doc info-keys
  "Set of keys which may appear in the table info map."
  #{::node/id ::name ::record/size})

;; Table names are strings which conform to some restrictions.
(s/def ::name
  (s/and string?
         #(not (str/includes? % "/"))
         #(<= 1 (count %) 127)))

;; Table data is a link to the root of the data tree.
(s/def ::record mdag/link?)

;; Tables may have a patch tablet containing recent unmerged data.
(s/def ::patch mdag/link?)

;; Time the table was last modified.
(s/def ::modified-at #(instance? Instant %))

;; Table root node.
(s/def ::node-data
  (s/and
    (s/keys :req [::record/count
                  ::index/fan-out
                  ::part/limit
                  ::modified-at]
            :opt [::data
                  ::patch
                  ::patch/limit
                  ::record/families
                  ::key/lexicoder])
    #(= data-type (:data/type %))))

(s/def ::table-info
  (s/keys :req [::node/id
                ::name
                ::version
                ::committed-at]))



;; ## Table API

(defprotocol ITable
  "Protocol for an immutable table of record data."

  ;; Records

  (keys
    [table]
    [table opts]
    "Scan the table, returning keys of the stored records which match the given
    options. Returns a lazy sequence of keys, or nil if the table is empty.

    If min and max keys are given, only records within the bounds will be
    returned (inclusive). A nil min or max implies the beginning or end of
    the data, respectively.

    Options may include:

    - `:min-key`
      Return records with keys equal to or greater than the marker.
    - `:max-key`
      Return records with keys equal to or less than the marker.
    - `:reverse`
      Reverse the order the keys are returned in.
    - `:offset`
      Skip this many records in the output.
    - `:limit`
      Return at most this many records.")

  (scan
    [table]
    [table opts]
    "Scan the table, returning data from records which match the given options.
    Returns a lazy sequence of vectors which each hold a record key and a map
    of the record data, or nil if the table is empty.

    If min and max keys or indices are given, only records within the
    bounds will be returned (inclusive). A nil min or max implies the beginning
    or end of the data, respectively.

    Options may include:

    - `:fields`
      Only return data for the selected set of fields. If provided, only
      records with data for one or more of the fields are returned, otherwise
      all fields and records (including empty ones) are returned.
    - `:min-key`
      Return records with keys equal to or greater than the marker.
    - `:max-key`
      Return records with keys equal to or less than the marker.
    - `:reverse`
      Reverse the order the keys are returned in.
    - `:offset`
      Skip this many records in the output.
    - `:limit`
      Return at most this many records.")

  (read
    [table id-keys]
    [table id-keys opts]
    "Read a set of records from the database, returning data for each present
    record. Returns a sequence of vectors which each hold a record key and a map
    of the record data, or nil if no records were found.

    Options may include:

    - `:fields`
      Only return data for the selected set of fields. If provided, only
      records with data for one or more of the fields are returned, otherwise
      all fields and records are returned.")

  (insert
    [table records]
    [table records opts]
    "Insert some record data into the database, represented by a collection
    of pairs of record key values and field maps. Returns an updated table.

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

  (flush!
    [table]
    [table opts]
    "Ensure that all local state has been persisted to the storage backend.
    Returns an updated persisted table.

    Options may include:

    - `:apply-patches?`
      If true, the current patch data will be merged into the main data tree
      and the returned table will have no patch link.")

  ;; Partitions

  (list-partitions
    [table]
    "List the partitions which comprise the table. Returns a sequence of
    partition nodes.")

  (read-partition
    [table partition-id opts]
    "Read all the records in the identified partition."))



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
      {::record/count 0}
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
      {::index/fan-out index/default-fan-out
       ::part/limit part/default-limit
       ::patch/limit patch/default-limit}
      (dissoc opts ::data ::patch)
      {::record/count 0})
    (sorted-map)
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
       ::record/size (node/reachable-size node)}
      (::node/data node)
      (sorted-map)
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



;; ## Protocol Implementation

(defn- table-lexicoder
  "Construct a lexicoder for the keys in a table."
  [table]
  (key/lexicoder (::key/lexicoder table :bytes)))


(defn- key-decoder
  "Return a function which will decode the keys in record entries with the
  given lexicoder."
  [lexicoder]
  (fn decode-entry [[k r]] [(key/decode lexicoder k) r]))


(defn- load-changes
  "Return the table's patch changes as a map from keys to records or
  tombstones. This will load the table's patch tablet if present and merge in
  any locally pending changes."
  [^Table table]
  (into
    (sorted-map)
    (concat
      (when-let [patch (some->> (::patch table)
                                (mdag/get-data (.store table)))]
        (::patch/changes patch))
      (.pending table))))


(defn- -keys
  "Internal `keys` implementation."
  ([table]
   (-keys table nil))
  ([table opts]
   (seq (map first (scan table opts)))))


(defn- -scan
  "Internal `scan` implementation."
  ([table]
   (-scan table nil))
  ([^Table table opts]
   (let [lexicoder (table-lexicoder table)
         min-key (some->> (:min-key opts) (key/encode lexicoder))
         max-key (some->> (:max-key opts) (key/encode lexicoder))]
     (->
       (patch/patch-seq
         ; Merged patch data to apply to the records.
         (patch/filter-changes
           (load-changes table)
           {:fields (:fields opts)
            :min-key min-key
            :max-key max-key})
         ; Lazy sequence of matching records from the index tree.
         (when-let [data-node (mdag/get-data
                                (.store table)
                                (::data (.root-data table)))]
           (if (or min-key max-key)
             (index/read-range
               (.store table)
               data-node
               (:fields opts)
               min-key
               max-key)
             (index/read-all
               (.store table)
               data-node
               (:fields opts)))))
       (cond->>
         (and (:offset opts) (pos? (:offset opts)))
           (drop (:offset opts))
         (and (:limit opts) (nat-int? (:offset opts)))
           (take (:limit opts)))
       (->>
         (map (key-decoder lexicoder)))
       (seq)))))


(defn- -read
  "Internal `read` implementation."
  ([table id-keys]
   (-read table id-keys nil))
  ([^Table table id-keys opts]
   (let [lexicoder (table-lexicoder table)
         id-keys (into (sorted-set) (map (partial key/encode lexicoder)) id-keys)
         patch-map (load-changes table)
         patch-changes (filter (comp id-keys first) patch-map)
         extra-keys (apply disj id-keys (map first patch-changes))
         patch-changes (patch/filter-changes patch-changes {:fields (:fields opts)})
         data-entries (when-let [data-node (and (seq extra-keys)
                                                (mdag/get-data
                                                  (.store table)
                                                  (::data (.root-data table))))]
                        (index/read-batch
                          (.store table)
                          data-node
                          (:fields opts)
                          extra-keys))]
     (->> (concat patch-changes data-entries)
          (patch/remove-tombstones)
          (map (key-decoder lexicoder))
          (seq)))))


(defn- record-updater
  "Construct a new record updating function from the given options. The
  resulting function accepts three arguments, the record key, the existing
  record data (or nil), and the new record data map."
  [{:keys [merge-record merge-field]}]
  (cond
    (and merge-record merge-field)
      (throw (IllegalArgumentException.
               "Record updates cannot make use of both :merge-field and :merge-record at the same time."))

    (fn? merge-record)
      merge-record

    merge-record
      (throw (IllegalArgumentException.
               (str "Record update :merge-record must be a function: "
                    (pr-str merge-record))))

    (or (map? merge-field) (fn? merge-field))
      (fn merge-fields
        [_ old-data new-data]
        (let [merger (if (map? merge-field)
                       (fn [fk l r]
                         (if-let [f (get merge-field fk)]
                           (f l r)
                           r))
                       merge-field)]
          (reduce
            (fn [data field-key]
              (let [left (get old-data field-key)
                    right (get new-data field-key)
                    value (merger field-key left right)]
                (if (some? value)
                  (assoc data field-key value)
                  data)))
            {} (distinct (concat (clojure.core/keys old-data)
                                 (clojure.core/keys new-data))))))

    merge-field
      (throw (IllegalArgumentException.
               (str "Record update :merge-field must be a function or map of field functions: "
                    (pr-str merge-field))))

    :else
      (fn merge-simple
        [_ old-data new-data]
        (into {} (filter (comp some? val)) (merge old-data new-data)))))


(defn- -insert
  "Internal `insert` implementation."
  ([table records]
   (-insert table records nil))
  ([table records opts]
   (let [{:keys [merge-record merge-field]} opts
         lexicoder (table-lexicoder table)
         update-record (record-updater opts)
         extant (into {} (-read table (map first records)))
         new-records (map
                       (fn [[k data]]
                         [(key/encode lexicoder k)
                          (update-record k (get extant k) data)])
                       records)]
     ; Add new data maps to pending changes.
     (-> table
         (update-pending into new-records)
         (update ::record/count + (- (count records) (count extant)))))))


(defn- -delete
  "Internal `delete` implementation."
  [table id-keys]
  (let [lexicoder (key/lexicoder (::key/lexicoder table :bytes))
        extant (-read table id-keys {:fields {}})]
    (-> table
        (update-pending
          into
          (comp
            (map first)
            (map (fn [k] [(key/encode lexicoder k) ::patch/tombstone])))
          extant)
        (update ::record/count - (count extant)))))


(defn- flush-changes
  "Returns a vector containing a link to a patch tablet and an index tree."
  [^Table table full?]
  (let [changes (load-changes table)]
    (if (seq changes)
      ; Check for force or limit overflow.
      (if (or full? (< (::patch/limit table 0) (count changes)))
        ; Combine pending changes and patch tablet and update data tree.
        [nil (index/update-tree (.store table)
                                table
                                (mdag/get-data (.store table) (::data table))
                                changes)]
        ; Flush any pending changes to the patch tablet.
        [(->> (patch/from-changes changes)
              (mdag/store-node! (.store table) nil)
              (mdag/link "patch"))
         (::data table)])
      ; No patch data or tablet.
      [nil (::data table)])))


(defn- -flush!
  "Ensure that all local state has been persisted to the storage backend and
  return an updated non-dirty table."
  ([table]
   (-flush! table false))
  ([^Table table apply-patches?]
   (if (dirty? table)
     (let [[patch-link data-link] (flush-changes table apply-patches?)
           root-data (-> (.root-data table)
                         (assoc :data/type data-type)
                         (dissoc ::patch ::data)
                         (cond->
                           patch-link (assoc ::patch patch-link)
                           data-link (assoc ::data data-link)))]
       (when-not (s/valid? ::node-data root-data)
         (throw (ex-info (str "Cannot write invalid table root node: "
                              (s/explain-str ::node-data root-data))
                         {:type ::invalid-root})))
       (let [node (mdag/store-node! (.store table) nil root-data)
             table-info (assoc (.table-info table)
                               ::node/id (::node/id node)
                               ::record/size (node/reachable-size node))
             table-meta (assoc (._meta table)
                               ::node/links (::node/links node))]
         (->Table
           (.store table)
           table-info
           (::node/data node)
           (sorted-map)
           false
           table-meta)))
     ; Table is clean, return directly.
     table)))


(extend Table

  ITable

  {:keys -keys
   :scan -scan
   :read -read
   :insert -insert
   :delete -delete
   :flush! -flush!})


; TODO: table statistics:
; - count of records in table
; - cumulative table node size
; - patch data:
;   - record count
;   - node size
; - tree data:
;   - root height
;   - index node count
;   - cumulative index node size
;   - partition count
;   - cumulative partition/tablet node size
