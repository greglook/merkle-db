(ns merkle-db.table
  "Tables are named top-level containers of records. Generally, a single table
  corresponds to a certain 'kind' of data. Tables also contain configuration
  determining how keys are encoded and records are stored."
  (:refer-clojure :exclude [keys read])
  (:require
    [clojure.spec.alpha :as s]
    [clojure.string :as str]
    [merkle-db.graph :as graph]
    [merkle-db.index :as index]
    [merkle-db.key :as key]
    [merkle-db.partition :as part]
    [merkle-db.patch :as patch]
    [merkle-db.record :as record]
    [merkledag.core :as mdag]
    [merkledag.link :as link]
    [merkledag.node :as node])
  (:import
    java.time.Instant))


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
    - `:reverse` (NYI)
      Reverse the order the keys are returned in.
    - `:offset`
      Skip this many records in the output.
    - `:limit`
      Return at most this many records.")

  (scan
    [table]
    [table opts]
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
      Return at most this many records.")

  (read
    [table id-keys]
    [table id-keys opts]
    "Read a set of records from the database, returning data for each present
    record. Returns a sequence of record data maps.

    Options may include:

    - `:fields`
      Only return data for the selected set of fields. If provided, only
      records with data for one or more of the fields are returned, otherwise
      all fields and records are returned.")

  (insert
    [table records]
    [table records opts]
    "Insert some record data into the database, represented by a collection
    of record data maps. Returns an updated table.

    Options may include:

    - `:update-field`
      A function which will be called with `(f field-key old-val new-val)`, and
      should return the new value to use for that field. By default, `new-val`
      is used directly.
    - `:update-record`
      A function which will be called with `(f record-key old-data new-data)`,
      and should return the data map to use for the record. By default, this
      merges the data maps and removes nil-valued fields.")

  (delete
    [table id-keys]
    "Remove some records from the table, identified by a collection of id keys.
    Returns an updated table.")

  (dirty?
    [table]
    "Return true if the table has local non-persisted modifications.")

  (flush!
    [table]
    [table opts]
    "Ensure that all local state has been persisted to the storage backend.
    Returns an updated persisted table.

    Options may include:

    - `:apply-patch?`
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


(defn ^:no-doc bare-table
  "Build a new detached table value with the given backing store."
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



;; ## Protocol Implementation

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


(defn- -keys
  "Internal `keys` implementation."
  ([table]
   (-keys table nil))
  ([table opts]
   ; OPTIMIZE: push down key-only read to avoid loading non-base tablets
   (map (comp ::record/id meta)
        (scan table opts))))


(defn- -scan
  "Internal `scan` implementation."
  ([table]
   (-scan table nil))
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
         (and (:offset opts) (pos? (:offset opts)))
           (drop (:offset opts))
         (and (:limit opts) (nat-int? (:limit opts)))
           (take (:limit opts)))
       (->>
         (map (partial record/decode-entry
                       lexicoder
                       (::primary-key table))))))))


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
                                               (graph/get-link!
                                                 (.store table)
                                                 table
                                                 (::data table)))]
                       (index/read-batch
                         (.store table)
                         data-node
                         (:fields opts)
                         extra-keys))]
    (patch/patch-seq patch-changes data-entries)))


(defn- -read
  "Internal `read` implementation."
  ([table ids]
   (-read table ids nil))
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


(defn- -insert
  "Internal `insert` implementation."
  ([table records]
   (-insert table records nil))
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


(defn- -delete
  "Internal `delete` implementation."
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


(defn -dirty?
  "Internal `dirty?` implementation."
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


(defn- -flush!
  "Ensure that all local state has been persisted to the storage backend and
  return an updated non-dirty table."
  ([table]
   (-flush! table nil))
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


(extend Table

  ITable

  {:keys -keys
   :scan -scan
   :read -read
   :insert -insert
   :delete -delete
   :dirty? -dirty?
   :flush! -flush!})



;; ## Utility Functions

(defn table-stats
  "Calculate statistics about the structure of the given table."
  [^Table table]
  ; TODO: pending/dirty info?
  (reduce
    (fn [stats data]
      (let [count-node (fn [s] (update s :count (fnil inc 0)))
            track (fn [m k new-data]
                    (let [s (get m k)
                          v (get new-data k)]
                      (assoc m
                             k
                             (-> s
                                 (update :min (fnil min v) v)
                                 (update :max (fnil max v) v)
                                 (update :sum (fnil + 0) v)))))]
        (case (:type data)
          :patch
            (assoc stats :patch (select-keys data [:size :changes]))

          :index
            (-> stats
                (update (:type data) count-node)
                (update (:type data) track :size data)
                (update (:type data) track :children data)
                (update-in [(:type data) :heights (:height data)] (fnil inc 0)))

          :partition
            (-> stats
                (update (:type data) count-node)
                (update (:type data) track :size data)
                (update (:type data) track :records data))

          :tablet
            (-> stats
                (update-in [(:type data) (:family data)] count-node)
                (update-in [(:type data) (:family data)] track :size data))

          ; Unknown node type.
          (-> stats
              (update :unknown count-node)
              (update :unknown track :size data)
              (update-in [:unknown :types] (fnil conj #{}) (:type data))))))
    {}
    (graph/find-nodes-2
      (.store table)
      #{}
      (conj (clojure.lang.PersistentQueue/EMPTY)
            (::data table)
            (::patch table))
      (fn [node]
        (let [data (::node/data node)]
          (case (:data/type data)
            :merkle-db/patch
              [[{:type :patch
                 :size (::node/size node)
                 :changes (count (::patch/changes data))}]
               nil]

            :merkle-db/index
              [[{:type :index
                 :size (::node/size node)
                 :height (::index/height data)
                 :children (count (::index/children data))}]
               (::index/children data)]

            :merkle-db/partition
              [(cons
                 {:type :partition
                  :size (::node/size node)
                  :records (::record/count data)}
                 (mapv
                   (fn [[family-key link]]
                     {:type :tablet
                      :size (::link/rsize link)
                      :family family-key})
                   (::part/tablets data)))
               ; Don't visit tablets, we don't want to load the data.
               nil]

            ; Some other node type.
            [[{:type (:data/type data)
               :size (node/reachable-size node)
               :external? true}]
             ; Don't visit links from unknown node types.
             nil]))))))


(defn print-stats
  "Render the stats for a table."
  [stats]
  (let [patch-size (get-in stats [:patch :size])
        index-size (get-in stats [:index :size :sum])
        part-size (get-in stats [:partition :size :sum])
        tablet-sizes (map (comp :sum :size) (vals (:tablet stats)))
        unknown-size (get-in stats [:unknown :size :sum])
        table-size (reduce (fnil + 0 0) 0 (list* patch-size
                                                 index-size
                                                 part-size
                                                 unknown-size
                                                 tablet-sizes))
        table-pct #(* (/ (double %) table-size) 100.0)
        unit-str (fn [scale units n]
                   (-> (->> (map vector (iterate #(/ (double %) scale) n) units)
                            (drop-while #(< scale (first %)))
                            (first))
                       (as-> [n u]
                         (if (integer? n)
                           (format "%d%s" n u)
                           (format "%.2f%s" (double n) u)))))
        byte-str (partial unit-str 1024 [" B" " KB" " MB" " GB" " TB" " PB"])
        count-str (partial unit-str 1000 [nil " K" " M" " B" " T"])
        stats-str (fn [data k f]
                    (format "min %s, mean %s, max %s"
                            (f (get-in data [k :min]))
                            (f (/ (get-in data [k :sum]) (:count data)))
                            (f (get-in data [k :max]))))]
    (println "Records:" (count-str (get-in stats [:partition :records :sum])))
    (println "Total size:" (byte-str table-size))
    (when-let [patch (:patch stats)]
      (printf "%d patch changes (%s, %.1f%%)\n"
              (:changes patch)
              (byte-str patch-size)
              (table-pct patch-size)))
    (when-let [index (:index stats)]
      (printf "%d index nodes (%s, %.1f%%)\n"
              (:count index)
              (byte-str index-size)
              (table-pct index-size))
      (printf "    Layers: %s\n"
              (->> (range)
                   (map #(get-in index [:heights (inc %)]))
                   (take-while some?)
                   (str/join " > ")))
      (println "    Sizes:" (stats-str index :size byte-str))
      (println "    Children:" (stats-str index :children count-str)))
    (when-let [part (:partition stats)]
      (printf "%d partitions (%s, %.1f%%)\n"
              (:count part)
              (byte-str part-size)
              (table-pct part-size))
      (println "    Sizes:" (stats-str part :size byte-str))
      (println "    Records:" (stats-str part :records count-str)))
    (doseq [family (cons :base (sort (clojure.core/keys (dissoc (:tablet stats) :base))))
            :let [tablet (get-in stats [:tablet family])]
            :when tablet]
      (printf "%d %s tablets (%s, %.1f%%)\n"
              (:count tablet)
              (name family)
              (byte-str (get-in tablet [:size :sum]))
              (table-pct (get-in tablet [:size :sum])))
      (println "    Sizes:" (stats-str tablet :size byte-str)))))
