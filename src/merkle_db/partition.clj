(ns merkle-db.partition
  "Partitions contain non-overlapping ranges of the records witin a table.
  Partition nodes contain metadata about the contained records and links to the
  tablets where the data for each field family is stored."
  (:require
    [clojure.future :refer [pos-int?]]
    [clojure.spec :as s]
    [merkledag.link :as link]
    [merkle-db.bloom :as bloom]
    [merkle-db.data :as data]
    [merkle-db.key :as key]
    [merkle-db.node :as node]
    [merkle-db.tablet :as tablet]))


(s/def ::limit pos-int?)
(s/def ::membership bloom/filter?)
(s/def ::first-key key/bytes?)
(s/def ::last-key key/bytes?)
(s/def ::tablets (s/map-of keyword? link/merkle-link?))


(s/def :merkle-db/partition
  (s/keys :req [::data/families
                ::data/count
                ::limit
                ::membership
                ::first-key
                ::last-key
                ::tablets]))



;; ## Utilities

(defn- store-tablet!
  "Store the given tablet data and return the family key and updated id."
  [store family-key tablet]
  (let [node (node/store-node!
               store
               (cond-> tablet
                 (not= family-key :base)
                 (tablet/prune-records)))]
    [family-key
     (link/create
       (if (namespace family-key)
         (str (namespace family-key) ":" (name family-key))
         (name family-key))
       (:id node)
       (:size node))]))


(defn- divide-tablets
  "Take a map of tablet links and a split key and returns a vector of two
  tablet maps, each containing the left and right tablet splits, respectively."
  [store tablets split-key]
  (reduce-kv
    (fn divide
      [[left right] family-key tablet-link]
      (let [tablet (node/get-data store tablet-link)]
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
  (let [defaults (select-keys part [:data/type ::data/families ::limit])
        [left right] (divide-tablets store (::tablets part) split-key)]
    ; Construct new partitions from left and right tablet maps.
    [(let [base-keys (tablet/keys (node/get-data store (:base left)))]
       (assoc defaults
              ::data/count (count base-keys)
              ::membership (into (bloom/create (::limit part)) base-keys)
              ::first-key (first base-keys)
              ::last-key (last base-keys)
              ::tablets left))
     (let [base-keys (tablet/keys (node/get-data store (:base right)))]
       (assoc defaults
              ::data/count (count base-keys)
              ::membership (into (bloom/create (::limit part)) base-keys)
              ::first-key (first base-keys)
              ::last-key (last base-keys)
              ::tablets right))]))


(defn join
  "Join two partitions into a single partition. The partition key ranges must
  not overlap."
  [store left right]
  ; TODO: implement
  (throw (UnsupportedOperationException. "NYI")))



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
       (map (partial node/get-data store))
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


(defn read-slice
  "Read a lazy sequence of key/map tuples which contain the field data for the
  records whose ranks lie in the given range, inclusive. A nil boundary includes
  all records in that range direction."
  [store part fields start-index end-index]
  (let [base (node/get-data store (:base (::tablets part)))
        start (and start-index (tablet/nth-key base start-index))
        end (and end-index (tablet/nth-key base end-index))]
    (read-tablets store part fields tablet/read-range start end)))



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
    (group-families (map-field-families families) data)
    (update :base #(or % {}))
    (->>
      (reduce-kv
        (fn assign-updates
          [updates family fdata]
          (update updates family (fnil conj []) [record-key fdata]))
        updates))))


(defn- update-tablet!
  "Apply an updating function to the data contained in the given tablet.
  Returns an updated tablets map."
  [store f tablets [family-key tablet-updates]]
  (->>
    (if-let [tablet-link (get tablets family-key)]
      ; Load existing tablet to update it.
      (->
        (node/get-data store tablet-link)
        (or (throw (ex-info (format "Couldn't find tablet %s in backing store"
                                    family-key)
                            {:family family-key
                             :link tablet-link})))
        (tablet/update-records f tablet-updates))
      ; Create new tablet and store it.
      (tablet/from-records f tablet-updates))
    (store-tablet! store family-key)
    (conj tablets)))


(defn add-records!
  "Performs an update across the tablets in the partition to merge in the given
  record data."
  [store part f records]
  (let [tablets (->> records
                     (reduce (partial append-record-updates (::data/families part)) {})
                     (reduce (partial update-tablet! store f) (::tablets part)))
        record-keys (map first records)
        record-count (count (tablet/read-all (node/get-data store (:base tablets))))]
    (assoc part
           ::data/count record-count
           ::tablets tablets
           ::membership (into (::membership part) record-keys)
           ::first-key (apply key/min (::first-key part) record-keys)
           ::last-key (apply key/max (::last-key part) record-keys))))


(defn from-records
  "Constructs a new partition from the given map of record data. The records
  will be split into tablets matching the given families, if provided."
  [store parameters f records]
  (let [records (sort-by first key/compare records)
        limit (or (::limit parameters) 100000)
        families (or (::data/families parameters) {})
        tablets (->> records
                     (reduce (partial append-record-updates families) {})
                     (map (juxt key #(tablet/from-records f (val %))))
                     (map (partial apply store-tablet! store))
                     (into {}))]
    ; FIXME: this may result in a partition larger than the limit in size
    {:data/type :merkle-db/partition
     ::data/families families
     ::data/count (count records)
     ::limit limit
     ::membership (into (bloom/create limit) (map first records))
     ::first-key (first (first records))
     ::last-key (first (last records))
     ::tablets tablets}))



;; ## Deletion Functions

#_
(defn remove-records
  [store part record-keys]
  (let [update-tablet! (fn [tablet-id]
                         (:id (node/update-data! store
                                                 tablet-id
                                                 tablet/remove-batch
                                                 record-keys)))]
    (->> (::tablets part)
         (into {} (map (juxt key #(update (val %) :id update-tablet!))))
         (assoc part ::tablets))))
