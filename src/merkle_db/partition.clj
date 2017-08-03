(ns merkle-db.partition
  "Partitions contain non-overlapping ranges of the records witin a table.
  Partition nodes contain metadata about the contained records and links to the
  tablets where the data for each field family is stored."
  (:require
    [clojure.future :refer [pos-int?]]
    [clojure.spec :as s]
    (merkledag
      [core :as mdag]
      [node :as node])
    (merkle-db
      [bloom :as bloom]
      [key :as key]
      [patch :as patch]
      [record :as record]
      [tablet :as tablet]
      [validate :as validate])))


;; ## Specs

(def ^:const data-type
  "Value of `:data/type` that indicates a partition node."
  :merkle-db/partition)

(def default-limit
  "The default number of records to build partitions up to."
  10000)

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
    (s/keys :req [::limit
                  ::tablets
                  ::membership
                  ::record/count
                  ::record/families
                  ::record/first-key
                  ::record/last-key])
    #(= data-type (:data/type %))))


(defn validate
  [store params part]
  (when (validate/check :data/type
          (= data-type (:data/type part))
          (str "Expected partition to have :data/type of " data-type
               " but got: " (pr-str (:data/type part))))
    (validate/check ::spec
      (s/valid? ::node-data part)
      (s/explain-str ::node-data part))
    ; TODO: warn when partition limit or families don't match params
    (when (<= (::limit part) (::record/count params))
      (validate/check ::underflow
        (<= (Math/ceil (/ (::limit part) 2)) (::record/count part))
        "Partition is at least half full if tree has at least :merkle-db.partition/limit records"))
    (validate/check ::overflow
      (<= (::record/count part) (::limit part))
      "Partition has at most :merkle-db.partition/limit records")
    ; TODO: first-key matches actual first record key
    ; TODO: last-key matches actual last record key
    ; TODO: record/count is accurate
    ; TODO: every key present tests true against membership filter
    (when (::record/first-key params)
      (validate/check ::record/first-key
        (not (key/before? (::record/first-key part) (::record/first-key params)))
        "First key in partition is within the subtree boundary"))
    (when (::record/last-key params)
      (validate/check ::record/last-key
        (not (key/after? (::record/last-key part) (::record/last-key params)))
        "Last key in partition is within the subtree boundary"))
    (validate/check ::base-tablet
      (:base (::tablets part))
      "Partition contains a base tablet")
    (doseq [[tablet-family link] (::tablets part)]
      (validate/check-link store link
        #(tablet/validate
            (assoc params
                   ::record/families (::record/families part)
                   ::record/family-key tablet-family)
           %))))
  {::record/count (::record/count part)})



;; ## Utilities

(defn- get-tablet
  "Return the tablet data for the given family key."
  [store part family-key]
  (mdag/get-data store (get (::tablets part) family-key)))


(defn- store-tablet!
  "Store the given tablet data and return the family key and updated id."
  [store family-key tablet]
  (let [node (mdag/store-node!
               store
               nil
               (cond-> tablet
                 (not= family-key :base)
                 (tablet/prune-records)))]
    [family-key
     (mdag/link
       (if (namespace family-key)
         (str (namespace family-key) ":" (name family-key))
         (name family-key))
       node)]))


(defn- divide-tablets
  "Take a map of tablet links and a split key and returns a vector of two
  tablet maps, each containing the left and right tablet splits, respectively."
  [store tablets split-key]
  (reduce-kv
    (fn divide
      [[left right] family-key tablet-link]
      (let [tablet (mdag/get-data store tablet-link)]
        (cond
          ; All tablet data is in the left split.
          (key/before? (tablet/last-key tablet) split-key)
          [(assoc left family-key tablet-link) right]

          ; All tablet data is in the right split.
          (key/after? (tablet/first-key tablet) split-key)
          [left (assoc right family-key tablet-link)]

          ; Split tablet into two pieces.
          :else
          (let [[ltab rtab] (tablet/split tablet split-key)]
            [(conj left (store-tablet! store family-key ltab))
             (conj right (store-tablet! store family-key rtab))]))))
    [{} {}]
    tablets))


(defn split
  "Split the partition into two partitions at the given key. All records less
  than the split key will be contained in the first partition, all others in
  the second."
  [store part split-key]
  (when-not (and (key/after? split-key (::record/first-key part))
                 (key/before? split-key (::record/last-key part)))
    (throw (ex-info (format "Cannot split partition with key %s which falls outside the contained range [%s, %s]"
                            split-key (::record/first-key part) (::record/last-key part))
                    {:split-key split-key
                     :first-key (::record/first-key part)
                     :last-key (::record/last-key part)})))
  (let [defaults (select-keys part [:data/type ::record/families ::limit])]
    ; Construct new partitions from left and right tablet maps.
    (map
      (fn make-part
        [tablets]
        (let [base-keys (tablet/keys (mdag/get-data store (:base tablets)))]
          (assoc defaults
                 ::tablets tablets
                 ::membership (into (bloom/create (::limit part)) base-keys)
                 ::record/count (count base-keys)
                 ::record/first-key (first base-keys)
                 ::record/last-key (last base-keys))))
      (divide-tablets store (::tablets part) split-key))))


(defn join
  "Join two partitions into a single partition. The partition key ranges must
  not overlap."
  [store left right]
  ; TODO: implement
  (throw (UnsupportedOperationException. "NYI: join partitions")))



;; ## Read Functions

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
    (cond->> records
      (seq fields)
        (map (juxt first #(select-keys (second %) fields))))))


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
  [store part fields start-key end-key]
  (read-tablets store part fields tablet/read-range start-key end-key))



;; ## Update Functions

(defn- store-node!
  [store data]
  (let [node (mdag/store-node! store nil data)]
    (vary-meta
      (::node/data node)
      merge
      (select-keys node [::node/id
                         ::node/size
                         ::node/links]))))


(defn- update-tablet!
  "Apply inserts and tombstone deletions to the data contained in the given
  tablet. Returns an updated map with a link to the new tablet."
  [store deletions tablets [family-key additions]]
  (->
    (if-let [tablet-link (get tablets family-key)]
      ; Load existing tablet to update it.
      (->
        (mdag/get-data store tablet-link)
        (or (throw (ex-info (format "Couldn't find tablet %s in backing store"
                                    family-key)
                            {:family family-key
                             :link tablet-link})))
        (tablet/update-records additions deletions))
      ; Create new tablet and store it.
      (tablet/from-records additions))
    (as-> tablet'
      (if (seq (tablet/keys tablet'))
        (conj tablets (store-tablet! store family-key tablet'))
        (dissoc tablets family-key)))))


(defn from-records
  "Constructs new partitions from the given map of record data. The records
  will be split into tablets matching the given families, if provided. Returns
  a sequence of persisted partitions."
  [store parameters records]
  (let [records (sort-by first (patch/remove-tombstones records))
        limit (or (::limit parameters) default-limit)
        families (or (::record/families parameters) {})]
    (into
      []
      (map
        (fn make-partition
          [partition-records]
          (store-node!
            store
            {:data/type data-type
             ::limit limit
             ::tablets (into {}
                             (map #(store-tablet! store (key %) (tablet/from-records (val %))))
                             (record/split-data families partition-records))
             ::membership (into (bloom/create limit)
                                (map first)
                                partition-records)
             ::record/count (count partition-records)
             ::record/families families
             ::record/first-key (first (first partition-records))
             ::record/last-key (first (last partition-records))})))
      (record/partition-limited limit records))))


(defn apply-patch!
  "Performs an update across the tablets in the given partition to merge in the
  patch changes. Returns three possible values:

  - If the changes deleted every record in the partition, returns `nil`.
  - If there are fewer than `(/ (::limit part) 2)` records left, a single
    *unserialized* tablet map containing the full record data.
  - Otherwise, a sequence of *serialized* partition nodes."
  [store part changes]
  (let [limit (or (::limit part) default-limit)
        deletion? (comp patch/tombstone? second)
        additions (remove deletion? changes)
        deleted-keys (set (map first (filter deletion? changes)))
        virtual-tablet (-> (read-all store part nil)
                           (tablet/from-records)
                           (tablet/update-records additions deleted-keys))
        record-count (count (tablet/read-all virtual-tablet))]
    (cond
      ; Empty partitions are not valid, so return nil.
      (zero? record-count) nil

      ; Partition would be less than half full - return virtual tablet for
      ; carrying.
      (< record-count (int (Math/ceil (/ limit 2)))) virtual-tablet

      ; Otherwise, divide up into one or more valid serialized partitions.
      :else (from-records store part (tablet/read-all virtual-tablet)))))
