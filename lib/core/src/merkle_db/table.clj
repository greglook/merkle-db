(ns merkle-db.table
  "Tables are collections of records which specify how the records are
  identified and stored. A table is presented as an immutable value; functions
  in this namespace return a new version of the table argument, keeping the
  original unchanged. Modifications are kept in memory until `flush!` is
  called."
  (:refer-clojure :exclude [keys read])
  (:require
    [clojure.spec.alpha :as s]
    [clojure.string :as str]
    [merkle-db.index :as index]
    [merkle-db.key :as key]
    [merkle-db.partition :as part]
    [merkle-db.patch :as patch]
    [merkle-db.record :as record]
    [merkledag.core :as mdag]
    [merkledag.link :as link]
    [merkledag.node :as node]))


;; ## Data Specifications

(def ^:no-doc info-keys
  "Set of keys which may appear in the table info map."
  #{::node/id ::name ::record/size})

;; Table names are strings which conform to some restrictions.
(s/def ::name
  (s/and string?
         #(not (str/includes? % "/"))
         #(<= 1 (count %) 127)))

;; Configuration for the primary key fields used to identify records in the
;; table.
(s/def ::primary-key ::record/id-field)

;; Table data is a link to the root of the data tree.
(s/def ::data mdag/link?)

;; Tables may have a patch tablet containing recent unmerged data.
(s/def ::patch mdag/link?)

;; Table root node.
(s/def ::node-data
  (s/and
    (s/keys :req [::record/count
                  ::index/fan-out
                  ::part/limit]
            :opt [::primary-key
                  ::data
                  ::patch
                  ::patch/limit
                  ::record/families
                  ::key/lexicoder])
    #(= :merkle-db/table (:data/type %))))


(s/def ::table-info
  (s/keys :req [::node/id
                ::name]))



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
      ; TODO: link-expand value?
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
               (str "Cannot change table :data/type from " :merkle-db/table))))
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



;; ## Constructors

; TODO: better names and docs

(defn new-table
  "Create a new table value backed by the given graph store. Any options given
  will be merged into the table root."
  [store table-name opts]
  (->Table
    store
    {::name table-name}
    (merge
      {::index/fan-out index/default-fan-out
       ::part/limit part/default-limit
       ::patch/limit patch/default-limit}
      (dissoc opts ::data ::patch)
      {:data/type :merkle-db/table
       ::record/count 0})
    (sorted-map)
    true
    nil))


(defn load-table
  "Load a table root node from the graph store."
  [store table-name target]
  (let [node (mdag/get-node store target)
        data-type (get-in node [::node/data :data/type])]
    (when (not= data-type :merkle-db/table)
      (throw (ex-info (format "Expected node at %s to be a merkle-db table, but found %s"
                              target data-type)
                      {:type ::bad-target
                       :expected :merkle-db/table
                       :actual data-type})))
    (->Table
      store
      {::name table-name
       ::node/id (::node/id node)
       ::record/size (node/reachable-size node)}
      (::node/data node)
      (sorted-map)
      false
      {::node/links (::node/links node)})))


(defn set-backing
  "Change the backing store and info for a table."
  [^Table table store table-name]
  (->Table
    store
    {::name table-name}
    (.root-data table)
    (.pending table)
    (.dirty table)
    (._meta table)))



;; ## Internal Functions

(defn- table-lexicoder
  "Construct a lexicoder for the keys in a table."
  [table]
  (key/lexicoder (::key/lexicoder table :bytes)))


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


(defn- filter-records
  "Takes a sequence of record entries and returns a filtered sequence based on
  the given options."
  [opts records]
  (when (seq records)
    (cond->> records
      (:min-key opts)
        (drop-while #(key/before? (first %) (:min-key opts)))
      (:max-key opts)
        (take-while #(not (key/after? (first %) (:max-key opts))))
      (:fields opts)
        (map (fn select-fields
               [[k r]]
               [k (if (map? r) (select-keys r (:fields opts)) r)])))))


(defn- read-batch
  "Retrieve a batch of records from the table by key. Returns a sequence of
  record entries."
  [^Table table id-keys opts]
  (let [id-keys (set id-keys)
        patch-map (load-changes table)
        patch-changes (filter (comp id-keys key) patch-map)
        extra-keys (apply disj id-keys (map key patch-changes))
        patch-changes (filter-records {:fields (:fields opts)} patch-changes)
        data-entries (when-let [data-node (and (seq extra-keys)
                                               (mdag/get-data
                                                 (.store table)
                                                 (::data table)))]
                       (index/read-batch
                         (.store table)
                         data-node
                         (:fields opts)
                         extra-keys))]
    (patch/patch-seq patch-changes data-entries)))



;; ## Read API

(defn scan
  "Scan the table, returning data from records which match the given options.
  Returns a lazy sequence of record data maps, sorted by key.

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
  - `:reverse` (NYI)
    Reverse the order the keys are returned in.
  - `:offset`
    Skip this many records in the output.
  - `:limit`
    Return at most this many records."
  ([table]
   (scan table nil))
  ([^Table table opts]
   (let [lexicoder (table-lexicoder table)
         min-k (some->> (:min-key opts) (key/encode lexicoder))
         max-k (some->> (:max-key opts) (key/encode lexicoder))
         opts (assoc opts :min-key min-k, :max-key max-k)]
     (->
       (patch/patch-seq
         ; Merged patch data to apply to the records.
         (filter-records opts (load-changes table))
         ; Lazy sequence of matching records from the index tree.
         (when-let [data-node (mdag/get-data
                                (.store table)
                                (::data (.root-data table)))]
           (if (or min-k max-k)
             (index/read-range
               (.store table)
               data-node
               (:fields opts)
               min-k
               max-k)
             (index/read-all
               (.store table)
               data-node
               (:fields opts)))))
       (cond->>
         (seq (:fields opts))
           (remove (comp empty? second))
         ; OPTIMIZE: push down offset to skip subtrees
         (and (:offset opts) (pos? (:offset opts)))
           (drop (:offset opts))
         (and (:limit opts) (nat-int? (:limit opts)))
           (take (:limit opts)))
       (->>
         (map (partial record/decode-entry
                       lexicoder
                       (::primary-key table))))))))


(defn keys
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
  - `:reverse` (NYI)
    Reverse the order the keys are returned in.
  - `:offset`
    Skip this many records in the output.
  - `:limit`
    Return at most this many records."
  ([table]
   (keys table nil))
  ([table opts]
   ; OPTIMIZE: push down key-only read to avoid loading non-base tablets
   (map (comp ::record/id meta)
        (scan table opts))))


(defn read
  "Read a set of records from the database, returning data for each present
  record. Returns a sequence of record data maps.

  Options may include:

  - `:fields`
    Only return data for the selected set of fields. If provided, only
    records with data for one or more of the fields are returned, otherwise
    all fields and records are returned."
  ([table ids]
   (read table ids nil))
  ([^Table table ids opts]
   (if (seq ids)
     (let [lexicoder (table-lexicoder table)
           id-keys (into (sorted-set) (map (partial key/encode lexicoder)) ids)]
       (->
         (read-batch table id-keys opts)
         ; TODO: is this really the best place for this filtering?
         (cond->>
           (seq (:fields opts))
             (remove (comp empty? second)))
         (->>
           (map (partial record/decode-entry
                         lexicoder
                         (::primary-key table))))))
     (list))))



;; ## Record Updates

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


(defn- record-updater
  "Construct a new record updating function from the given options. The
  resulting function accepts three arguments, the record key, the existing
  record data (or nil), and the new record data map."
  [{:keys [update-record update-field]}]
  (cond
    (and update-record update-field)
      (throw (IllegalArgumentException.
               "Record updates cannot make use of both :update-field and :update-record at the same time."))

    (fn? update-record)
      update-record

    update-record
      (throw (IllegalArgumentException.
               (str "Record update :update-record must be a function: "
                    (pr-str update-record))))

    (or (map? update-field) (fn? update-field))
      (record/field-merger update-field)

    update-field
      (throw (IllegalArgumentException.
               (str "Record update :update-field must be a function or map of field functions: "
                    (pr-str update-field))))

    :else
      (fn update-simple
        [_ old-data new-data]
        (merge old-data new-data))))


(defn insert
  "Insert a collection of records into the table. Returns an updated table
  value which includes the data.

  Options may include:

  - `:update-field`
    A function which will be called with `(f field-key old-val new-val)`, and
    should return the new value to use for that field. By default, `new-val`
    is used directly.
  - `:update-record`
    A function which will be called with `(f record-key old-data new-data)`,
    and should return the data map to use for the record. By default, this
    merges the data maps and removes nil-valued fields."
  ([table records]
   (insert table records nil))
  ([table records opts]
   (if (seq records)
     (let [lexicoder (table-lexicoder table)
           update-rec (record-updater opts)
           records (mapv (partial record/encode-entry
                                  lexicoder
                                  (::primary-key table))
                         records)
           extant (into {} (read-batch table (map first records) nil))
           records (reduce
                     (fn [acc [k r]]
                       (let [prev (get acc k)
                             data (update-rec k prev r)]
                         (assoc acc k (not-empty data))))
                     extant records)
           added (- (count records) (count extant))]
       ; Add new data maps to pending changes.
       (-> table
           (update-pending into records)
           (update ::record/count + added)))
     ; No records inserted, return table unchanged.
     table)))


(defn delete
  "Remove the identified records from the table. Returns an updated version of
  the table which does not contain records whose id was in the collection."
  [table ids]
  (if (seq ids)
    (let [lexicoder (table-lexicoder table)
          id-keys (mapv (partial key/encode lexicoder) ids)
          extant-keys (into #{} (map first) (read-batch table id-keys nil))]
      (-> table
          (update-pending
            into
            (map #(vector % ::patch/tombstone))
            extant-keys)
          (update ::record/count - (count extant-keys))))
    ; No records deleted, return table unchanged.
    table))



;; ## Table Persistence

(defn dirty?
  "True if the table has local non-persisted modifications."
  [^Table table]
  (.dirty table))


(defn- flush-changes
  "Returns a vector containing a link to a patch tablet and an index tree."
  [^Table table full?]
  (let [changes (load-changes table)]
    (if (seq changes)
      ; Check for force or limit overflow.
      (if (or full? (< (::patch/limit table 0) (count changes)))
        ; Combine pending changes and patch tablet and update data tree.
        [nil (when-let [data (index/update-tree
                               (.store table)
                               table
                               (mdag/get-data (.store table) (::data table))
                               changes)]
               (mdag/link "data" data))]
        ; Flush any pending changes to the patch tablet.
        [(->> (patch/from-changes changes)
              (mdag/store-node! (.store table) nil)
              (mdag/link "patch"))
         (::data table)])
      ; No patch data or tablet.
      [nil (::data table)])))


(defn flush!
  "Ensure that all local state has been persisted to the storage backend.
  Returns an updated persisted table.

  Options may include:

  - `:apply-patch?`
    If true, any current patch data will be merged into the main data tree."
  ([table]
   (flush! table nil))
  ([^Table table opts]
   (if (.dirty table)
     (let [[patch-link data-link] (flush-changes table (:apply-patch? opts))
           root-data (-> (.root-data table)
                         (assoc :data/type :merkle-db/table)
                         (dissoc ::patch ::data)
                         (cond->
                           patch-link (assoc ::patch patch-link)
                           data-link (assoc ::data data-link)))]
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
