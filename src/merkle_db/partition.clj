(ns merkle-db.partition
  "Partitions contain non-overlapping ranges of the records witin a table.
  Partition nodes contain metadata about the contained records and links to the
  tablets where the data for each field family is stored."
  (:require
    [clojure.future :refer [pos-int?]]
    [clojure.spec :as s]
    [merkledag.core :as mdag]
    [merkledag.node :as node]
    [merkle-db.bloom :as bloom]
    [merkle-db.graph :as graph]
    [merkle-db.key :as key]
    [merkle-db.patch :as patch]
    [merkle-db.record :as record]
    [merkle-db.tablet :as tablet]))


(def ^:const data-type
  "Value of `:data/type` that indicates a partition node."
  :merkle-db/partition)


;; Maximum number of records to allow in each partition.
(s/def ::limit pos-int?)

;; Map of family keys (and `:base`) to links to the corresponding tablets.
(s/def ::tablets (s/map-of keyword? mdag/link?))

;; Bloom filter providing probable membership testing for record keys contained
;; in the partition.
(s/def ::membership bloom/filter?)

;; Partition node.
(s/def ::node-data
  (s/and
    (s/keys :req [::tablets
                  ::membership
                  ::record/count
                  ::record/families
                  ::record/first-key
                  ::record/last-key])
    #(= data-type (:data/type %))))



;; ## Partition Limits

(def default-limit
  "The default number of records to build partitions up to."
  10000)


(defn max-limit
  "Return the maximum size a valid partition can have given the parameters."
  [params]
  (::limit params default-limit))


(defn min-limit
  "Return the minimum size a valid partition can have given the parameters."
  [params]
  (int (Math/ceil (/ (max-limit params) 2))))


(defn split-limited
  "Returns a sequence of groups of the elements of `coll` such that:
  - No group has more than `limit` elements
  - The number of groups is minimized
  - Groups are approximately equal in size

  This method eagerly realizes the input collection."
  [limit coll]
  (let [cnt (count coll)
        n (min (int (Math/ceil (/ cnt limit))) cnt)]
    (when (pos? cnt)
      (loop [i 0
             mark 0
             groups []
             xs coll]
        (if (seq xs)
          (let [mark' (int (* (/ (inc i) n) cnt))
                [head tail] (split-at (- mark' mark) xs)]
            (recur (inc i) (int mark') (conj groups head) tail))
          groups)))))



;; ## Construction Functions

(defn- store-tablet!
  "Store the given tablet data. Returns a tuple of the family key and named
  link to the new node."
  [store family-key tablet]
  (let [tablet (cond-> tablet
                 (not= family-key :base)
                 (tablet/prune))]
    (when (seq (tablet/read-all tablet))
      [family-key
       (mdag/link
         (if (namespace family-key)
           (str (namespace family-key) ":" (name family-key))
           (name family-key))
         (mdag/store-node! store nil tablet))])))


(defn from-records
  "Constructs a new partition from the given map of record data. The records
  will be split into tablets matching the given families, if provided. Returns
  the node data for the persisted partition."
  [store params records]
  (let [records (vec (sort-by first (patch/remove-tombstones records))) ; TODO: don't sort?
        limit (max-limit params)
        families (or (::record/families params) {})]
    (when (< limit (count records))
      (throw (ex-info
               (format "Cannot construct a partition from %d records overflowing limit %d"
                       (count records) limit)
               {::record/count (count records)
                ::limit limit})))
    (when (seq records)
      (->>
        {:data/type data-type
         ::tablets (into {}
                         (map #(store-tablet! store (key %) (tablet/from-records (val %))))
                         (record/split-data families records))
         ::membership (into (bloom/create limit)
                            (map first)
                            records)
         ::record/count (count records)
         ::record/families families
         ::record/first-key (first (first records))
         ::record/last-key (first (last records))}
        (mdag/store-node! store nil)
        (::node/data)))))


(defn partition-records
  "Divides the given records into one or more partitions. Returns a sequence of
  node data for the persisted partitions."
  [store params records]
  (->> records
       (patch/remove-tombstones)
       (split-limited (max-limit params))
       (mapv #(from-records store params %))))



;; ## Read Functions

(defn- get-tablet
  "Return the tablet data for the given family key."
  [store part family-key]
  (graph/get-link! store part (get (::tablets part) family-key)))


(defn- choose-tablets
  "Selects a list of tablet names to query over, given a mapping of tablet
  names to sets of the contained fields and the desired set of field data. If
  selected-fields is empty, returns all tablets."
  [tablet-fields selected]
  (if (seq selected)
    ; Use field selection to filter tablets to load.
    (-> (dissoc tablet-fields :base)
        (->> (keep #(when (some selected (val %)) (key %))))
        (set)
        (as-> chosen
          (if (seq (apply disj selected (mapcat tablet-fields chosen)))
            (conj chosen :base)
            chosen)))
    ; No selection provided, return all field data.
    (-> tablet-fields keys set (conj :base))))


(defn- record-seq
  "Combines lazy sequences of partial records into a single lazy sequence
  containing key/data tuples."
  [field-seqs]
  (lazy-seq
    (when-let [next-key (some->> (seq (keep ffirst field-seqs))
                                 (apply key/min))] ; TODO: key/max for reverse
      (let [has-next? #(= next-key (ffirst %))
            next-data (->> field-seqs
                           (filter has-next?)
                           (map (comp second first))
                           (apply merge))
            next-seqs (keep #(if (has-next? %) (next %) (seq %))
                            field-seqs)]
        (cons [next-key next-data] (record-seq next-seqs))))))


(defn- read-tablets
  "Performs a read across the tablets in the partition by selecting based on
  the desired fields. The reader function is called on each selected tablet
  along with any extra args, producing a collection of lazy record sequences
  which are combined into a single sequence of key/record pairs."
  [store part fields read-fn & args]
  ; OPTIMIZE: use transducer instead of intermediate sequences.
  (let [tablets (choose-tablets (::record/families part) (set fields))
        field-seqs (map #(apply read-fn (get-tablet store part %) args) tablets)
        records (record-seq field-seqs)]
    (if (seq fields)
      (->>
        records
        (map (juxt first #(select-keys (second %) fields)))
        (remove (comp empty? second)))
      records)))


(defn read-all
  "Read a lazy sequence of key/map tuples which contain the requested field data
  for every record in the partition."
  [store part fields]
  (read-tablets store part fields tablet/read-all))


(defn read-batch
  "Read a lazy sequence of key/map tuples which contain the requested field
  data for the records whose keys are in the given collection."
  [store part fields record-keys]
  ; OPTIMIZE: use the membership filter to weed out keys which are definitely not present.
  (read-tablets store part fields tablet/read-batch record-keys))


(defn read-range
  "Read a lazy sequence of key/map tuples which contain the field data for the
  records whose keys lie in the given range, inclusive. A nil boundary includes
  all records in that range direction."
  [store part fields min-key max-key]
  (read-tablets store part fields tablet/read-range min-key max-key))



;; ## Update Functions

(defn- emit-parts
  "Chop up some pending records into full valid partitions. Returns a tuple
  containing a vector of serialized partitions and a vector containing any
  remaining records."
  [store params pending]
  (let [part-size (max-limit params)
        threshold (+ part-size (min-limit params))]
    (loop [result []
           records pending]
      (if (<= threshold (count records))
        ; Serialize a full partition using the pending records.
        (let [[output remnant] (split-at part-size records)
              part (from-records store params output)]
          (recur (conj result part) remnant))
        ; Not enough to guarantee validity, so continue.
        [result records]))))


(defn- patch-records
  "Apply changes to the given partition, combining with any pending records to
  produce a sequence of new partitions and any newly pending records."
  [store params pending part changes]
  (let [records (read-all store part nil)
        records' (concat pending (patch/patch-seq changes records))]
    (cond
      ; All data was removed from the partition.
      (empty? records')
        nil
      ; Original partition data was unchanged by updates.
      (= records records')
        [[part] nil]
      ; Output partitions if we've accumulated enough records.
      :else
        (emit-parts store params records'))))


(defn- merge-into
  "Combines a non-empty vector of partitions and some trailing records to
  produce a valid final sequence of partitions. Returns a result tuple with the
  updated sequence of partitions, or records if underflowing."
  [store params parts pending]
  (loop [parts (vec parts)
         pending pending]
    (if (<= (min-limit params) (count pending))
      ; Enough records to make at least one valid partition.
      [0 (into parts (partition-records store params pending))]
      ; Need more records to form a partition.
      (if (seq parts)
        ; Join records from the last partition into pending.
        (recur (pop parts) (concat (read-all store (peek parts) nil) pending))
        ; No more partitions to join, return records result.
        [-1 pending]))))


(defn update-partitions!
  "Apply patch changes to a sequence of partitions and redistribute the results
  to create a new sequence of valid, updated partitions. Each input tuple
  should have the form `[partition changes]`, where changes **must be sorted**
  by key. `carry` may be a result tuple with height 0 and a vector of
  partitions, or height -1 and a vector of records.

  Returns a result tuple with height 0 and a sequence of updated, valid, stored
  partitions, or tuple with height -1 and a sequence of records if there were
  not enough records in the result to create a valid partition. The sequence of
  partitions may be empty if all records were removed."
  [store params carry inputs]
  (when (and carry (pos? (first carry)))
    (throw (IllegalArgumentException.
             (str "Cannot carry index subtrees into a partition update: "
                  (pr-str carry)))))
  (loop [outputs (if (and carry (zero? (first carry)))
                   (vec (second carry))
                   [])
         pending (when (and carry (neg? (first carry)))
                   (vec (second carry)))
         inputs inputs]
    (if (seq inputs)
      ; Process next partition in the sequence.
      (let [[part changes] (first inputs)]
        (if (and (nil? pending) (empty? changes))
          ; No pending records or changes, so use "pass through" logic.
          (recur (conj outputs part) nil (next inputs))
          ; Load partition data or use pending records.
          (if (and (empty? changes) (<= (min-limit params) (count pending)))
            ; Avoid changing an existing partition.
            (let [parts (partition-records store params pending)]
              (recur (conj (into outputs parts) part) nil (next inputs)))
            ; Load updated partition records into pending.
            (let [[parts pending] (patch-records store params pending part changes)]
              (recur (into outputs parts) pending (next inputs))))))
      ; No more partitions to process.
      (if (empty? pending)
        ; No pending data to handle, we're done.
        (when (seq outputs)
          [0 outputs])
        ; Merge loose records backward into partitions.
        (merge-into store params outputs pending)))))


(defn carry-back
  "Carry an orphaned node back from later in the tree. Returns a result vector
  with height zero and the updated sequence of partitions, or a -1 height result
  if there were not enough records left."
  [store params parts carry]
  (prn ::carry-back (count parts) carry)
  (cond
    (nil? carry)
      [0 parts]

    (zero? (first carry))
      [0 (into parts (second carry))]

    (neg? (first carry))
      (merge-into store params parts (second carry))

    :else
      (throw (IllegalArgumentException.
               (str "Illegal carry type: " (pr-str carry))))))
