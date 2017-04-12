(ns merkle-db.partition
  (:refer-clojure :exclude [read])
  (:require
    [clojure.future :refer [any? nat-int?]]
    [clojure.set :as set]
    [clojure.spec :as s]
    [merkledag.link :as link]
    [merkle-db.bloom :as bloom]
    [merkle-db.key :as key]
    [merkle-db.node :as node]
    [merkle-db.tablet :as tablet]))


(s/def :merkle-db.data/count nat-int?)

(s/def :merkle-db.data/families
  (s/map-of keyword? (s/coll-of any? :kind set?)))

(s/def ::membership bloom/filter?)
(s/def ::first-key key/bytes?)
(s/def ::last-key key/bytes?)
(s/def ::tablets (s/map-of keyword? link/merkle-link?))

(s/def :merkle-db/partition
  (s/keys :req [:merkle-db.data/count
                ::membership
                ::first-key
                ::last-key
                ::tablets]
          :opt [:merkle-db.data/families]))



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
       (choose-tablets (:merkle-db.data/families part))
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
  [families] #_ ([family-key #{field-key ...}])
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


(defn- store-tablet!
  "Store the given tablet data and return the family key and updated id."
  [store family-key tablet]
  (->>
    (cond-> tablet
      (not= family-key :base)
      (tablet/prune-records))
    (node/store-node! store)
    (:id)
    (vector family-key)))


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
        (tablet/merge-records f tablet-updates))
      ; Create new tablet and store it.
      (tablet/from-records f tablet-updates))
    (store-tablet! store family-key)
    (conj tablets)))


(defn add-records!
  "Performs an update across the tablets in the partition to merge in the given
  record data."
  [store part f records]
  (let [tablets (->> records
                     (reduce (partial append-record-updates (:merkle-db.data/families part)) {})
                     (reduce (partial update-tablet! store f) (::tablets part)))
        record-keys (map first records)
        record-count (count (tablet/read-all (node/get-data store (:base tablets))))]
    (assoc part
           :merkle-db.data/count record-count
           ::tablets tablets
           ::membership (reduce bloom/insert (::membership part) record-keys)
           ::first-key (apply key/min (::first-key part) record-keys)
           ::last-key (apply key/max (::last-key part) record-keys))))


(defn from-records
  "Constructs a new partition from the given map of record data. The records
  will be split into tablets matching the given families, if provided."
  [store families f records]
  (let [records (sort-by first key/compare records)
        membership (reduce bloom/insert
                           (bloom/create (count records))
                           (map first records))
        tablets (->> records
                     (reduce (partial append-record-updates families) {})
                     (map (juxt key #(tablet/from-records f (val %))))
                     (map (partial apply store-tablet! store))
                     (into {}))]
    (cond->
      {:data/type :merkle-db/partition
       :merkle-db.data/count (count records)
       ::membership membership
       ::first-key (first (first records))
       ::last-key (first (last records))
       ::tablets tablets}
      (seq families)
        (assoc :merkle-db.data/families families))))




; TODO: split
; TODO: join



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
