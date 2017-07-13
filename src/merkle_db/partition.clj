(ns merkle-db.partition
  "Partitions contain non-overlapping ranges of the records witin a table.
  Partition nodes contain metadata about the contained records and links to the
  tablets where the data for each field family is stored."
  (:require
    [clojure.future :refer [pos-int?]]
    [clojure.spec :as s]
    (merkledag
      [core :as mdag]
      [link :as link])
    (merkle-db
      [bloom :as bloom]
      [data :as data]
      [key :as key]
      [patch :as patch]
      [tablet :as tablet])))


;; ## Specs

(def ^:const data-type
  "Value of `:data/type` that indicates a partition node."
  :merkle-db/partition)

(def default-limit
  "The default number of records to build partitions up to."
  100000)

;; Maximum number of records to allow in each partition.
(s/def ::limit pos-int?)

;; Bloom filter providing probable membership testing for record keys contained
;; in the partition.
(s/def ::membership bloom/filter?)

;; First key present in the partition.
(s/def ::first-key key/key?)

;; Last key present in the partition.
(s/def ::last-key key/key?)

;; Map of family keys (and `:base`) to links to the corresponding tablets.
(s/def ::tablets (s/map-of keyword? link/merkle-link?))

;; Partition node.
(s/def :merkle-db/partition
  (s/and
    (s/keys :req [::data/count
                  ::data/families
                  ::limit
                  ::membership
                  ::first-key
                  ::last-key
                  ::tablets])
    #(= data-type (:data/type %))))



;; ## Utilities

(defn- partition-approx
  "Returns a lazy sequence of `n` lists containing the elements of `coll` in
  order, where each list is approximately the same size."
  [n coll]
  (->>
    [nil
     (->> (range (inc n))
          (map #(int (* (/ % n) (count coll))))
          (partition 2 1))
     coll]
    (iterate
      (fn [[_ [[start end :as split] & splits] xs]]
        (when-let [length (and split (- end start))]
          [(seq (take length xs))
           splits
           (drop length xs)])))
    (drop 1)
    (take-while first)
    (map first)))


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
  (when-not (and (key/after? split-key (::first-key part))
                 (key/before? split-key (::last-key part)))
    (throw (ex-info (format "Cannot split partition with key %s which falls outside the contained range [%s, %s]"
                            split-key (::first-key part) (::last-key part))
                    {:split-key split-key
                     :first-key (::first-key part)
                     :last-key (::last-key part)})))
  (let [defaults (select-keys part [:data/type ::data/families ::limit])]
    ; Construct new partitions from left and right tablet maps.
    (map
      (fn make-part
        [tablets]
        (let [base-keys (tablet/keys (mdag/get-data store (:base tablets)))]
          (assoc defaults
                 ::data/count (count base-keys)
                 ::membership (into (bloom/create (::limit part)) base-keys)
                 ::first-key (first base-keys)
                 ::last-key (last base-keys)
                 ::tablets tablets)))
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
                                 (apply key/min))]
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
  (->> (set fields)
       (choose-tablets (::data/families part))
       (map (::tablets part))
       (map (partial mdag/get-data store))
       (map #(apply read-fn % args))
       (record-seq)
       (map (juxt first #(select-keys (second %) fields)))))


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

(defn- map-field-families
  "Build a map from field keys to the family the field belongs to."
  [families]
  (into {}
        (mapcat #(map vector (second %) (repeat (first %))))
        families))


(defn- group-families
  "Build a map from family keys to maps which contain the field data for the
  corresponding family. Fields not grouped in a family will be added to
  `:base`."
  [field->family data]
  (reduce-kv
    (fn split-data
      [groups field value]
      (update groups (field->family field :base) assoc field value))
    {} data))


(defn- append-record-updates
  "Add record data to a map of `updates` from family keys to vectors of pairs
  of record keys and family-specific data. The function _always_ appends a pair
  to the `:base` family vector, to ensure the key is represented there."
  [families updates [record-key data]]
  (->
    (map-field-families families)
    (group-families data)
    (update :base #(or % {}))
    (->>
      (reduce-kv
        (fn assign-updates
          [updates family fdata]
          (update updates family (fnil conj []) [record-key fdata]))
        updates))))


(defn- update-tablet!
  "Apply inserts and tombstone deletions to the data contained in the given
  tablet. Returns an updated map with a link to the new tablet."
  [store deletions tablets [family-key additions]]
  (->>
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
    ; TODO: if this returns nil we need to remove the link
    (store-tablet! store family-key)
    (conj tablets)))


(defn from-records
  "Constructs new partitions from the given map of record data. The records
  will be split into tablets matching the given families, if provided."
  [store parameters records]
  (let [records (sort-by first records)
        limit (or (::limit parameters) default-limit)
        families (or (::data/families parameters) {})
        part-count (inc (int (/ (count records) limit)))]
    (map
      (fn make-partition
        [partition-records]
        {:data/type data-type
         ::data/families families
         ::data/count (count partition-records)
         ::limit limit
         ::membership (into (bloom/create limit) (map first partition-records))
         ::first-key (first (first partition-records))
         ::last-key (first (last partition-records))
         ::tablets (->>
                     partition-records
                     (reduce (partial append-record-updates families) {})
                     (map (juxt key #(tablet/from-records (val %))))
                     (map (partial apply store-tablet! store))
                     (into {}))})
      (partition-approx part-count records))))


(defn apply-patch!
  "Performs an update across the tablets in the partition to merge in the given
  patch changes. Returns a sequence of zero or more partitions."
  [store part changes]
  (let [limit (or (::limit part) default-limit)
        additions (remove (comp patch/tombstone? second) changes)
        deletions (set (map first (filter (comp patch/tombstone? second)
                                          changes)))
        tablets (->> additions
                     (reduce (partial append-record-updates
                                      (::data/families part))
                             {})
                     (reduce (partial update-tablet! store deletions)
                             (::tablets part)))
        added-keys (map first additions)
        record-count (count (tablet/read-all (mdag/get-data store (:base tablets))))
        part-count (inc (int (/ record-count limit)))]
    (if (< 1 part-count)
      ; Records still fit into one partition
      [(assoc part
              ::data/count record-count
              ::tablets tablets
              ::membership (into (::membership part) added-keys)
              ::first-key (apply key/min (::first-key part) added-keys)
              ::last-key (apply key/max (::last-key part) added-keys))]
      ; Partition must be split into multiple.
      (->>
        (read-all store part nil)
        (partition-approx part-count)
        (map
          (fn make-partition
            [partition-records]
            ; TODO: build partitions from records
            ))))))
