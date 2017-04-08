(ns merkle-db.partition
  (:refer-clojure :exclude [read])
  (:require
    [bigml.sketchy.bloom :as bloom]
    [clojure.future :refer [any? nat-int?]]
    [clojure.set :as set]
    [clojure.spec :as s]
    [merkle-db.key :as key]
    [merkle-db.node :as node]
    [merkle-db.tablet :as tablet]))


; TODO: pull in merkle-dag
(declare merkle-link?)


; Wrap the bigml bloom filter so we can encode it better and control printing.
(defrecord MembershipFilter
  [bins bits k])


(s/def :merkle-db.data/count nat-int?)

(s/def :merkle-db.data/families
  (s/map-of keyword? (s/coll-of any? :kind set?)))

(s/def ::membership (partial instance? MembershipFilter))
(s/def ::first-key key/bytes?)
(s/def ::last-key key/bytes?)
(s/def ::tablets (s/map-of keyword? merkle-link?))

(s/def :merkle-db/partition
  (s/keys :req [:merkle-db.data/count
                ::start-key
                ::end-key
                ::tablets]
          :opt [:merkle-db.data/families]))



;; ## Constructors

(defn- partition-families
  "Builds a field family map from a map of tablet keys to link values."
  [store tablet-ids]
  (-> {}
      (into (map (juxt key #(tablet/fields-present (node/get-data store (val %)))))
            tablet-ids)
      (dissoc :base)))


; TODO: how useful is this outside of testing?
(defn from-tablets
  "Constructs a new partition from the given map of tablets. The argument should
  be a map from tablet keys (including `:base`) to tablet node ids."
  [store tablet-ids]
  (when-not (:base tablet-ids)
    (throw (ex-info "Cannot construct a partition without a base tablet"
                    {:tablets tablet-ids})))
  (let [base (node/get-data store (:base tablet-ids))
        base-records (vec (tablet/read base))
        families (partition-families store tablet-ids)]
    (cond->
      {:data/type :merkle-db/partition
       :merkle-db.data/count (count base-records)
       ::membership (map->MembershipFilter (bloom/create (count base-records) 0.01))
       ::first-key (first (first base-records))
       ::last-key (first (last base-records))
       ::tablets tablet-ids}
      (seq families)
        (assoc :merkle-db.data/families families))))


(defn from-records
  "Constructs a new partition from the given map of record data. The records
  will be split into tablets matching the given families, if provided."
  [store families records]
  ; TODO: implement
  ,,,)



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


; TODO: need more tightly scoped read functions to take advantage of membership
; filter for batch-get and handle slice indexes.
(defn read-tablets
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



;; ## Update Functions

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
  [field->family updates [record-key data]]
  (->
    (group-families field->family data)
    (update :base #(or % {}))
    (->>
      (reduce-kv
        (fn assign-updates
          [updates family fdata]
          (update updates family (fnil conj []) [record-key fdata]))
        updates))))


(defn- update-tablets!
  "Apply an updating function to the data contained in the given tablets."
  [store f tablets [family-key tablet-updates]]
  (let [tablet-link (get tablets family-key)
        tablet (or (node/get-data store tablet-link)
                   (throw (ex-info (format "Couldn't find tablet %s in backing store"
                                           family-key)
                                   {:family family-key
                                    :link tablet-link})))]
    (->>
      tablet-updates
      (tablet/merge-records tablet f)
      (node/store-node! store)
      (:id)
      (assoc-in tablets family-key))))


(defn add-records!
  "Performs an update across the tablets in the partition to merge in the given
  record data."
  [store part f records]
  (let [field->family (reduce (fn [ftf [family fields]]
                                (reduce #(assoc %1 %2 family) ftf fields))
                              {} (:merkle-db.data/families part))
        ; Update partition tablet map.
        tablets (->> records
                     (reduce (partial append-record-updates field->family) {})
                     (reduce (partial update-tablets! store f) (::tablets part)))
        record-count (count (tablet/read (node/get-data store (:base tablets))))]
    (assoc part
           :merkle-db.data/count record-count
           ::tablets tablets
           ::membership (reduce bloom/insert (::membership part) (keys records))
           ::first-key (apply key/min (::first-key part) (keys records))
           ::last-key (apply key/max (::last-key part) (keys records)))))


; TODO: split
; TODO: merge



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
