(ns merkle-db.partition
  (:refer-clojure :exclude [read])
  (:require
    [clojure.future :refer [any? nat-int?]]
    [clojure.set :as set]
    [clojure.spec :as s]
    [merkle-db.key :as key]
    [merkle-db.node :as node]
    [merkle-db.tablet :as tablet]))


; TODO: pull in merkle-dag
(declare merkle-link?)


(s/def :merkle-db.data/count nat-int?)

; TODO: bloom filter for key membership

(s/def ::first-key key/bytes?)
(s/def ::last-key key/bytes?)

(s/def :merkle-db.data.partition.tablet/data merkle-link?)
(s/def :merkle-db.data.partition.tablet/fields (s/coll-of any? :kind set?))

(s/def ::tablets
  (s/map-of keyword? (s/keys :req-un [:merkle-db.data.partition.tablet/id]
                             :opt-un [:merkle-db.data.partition.tablet/fields])))

(s/def :merkle-db/partition
  (s/keys :req [:merkle-db.data/count
                ::start-key
                ::end-key
                ::tablets]))



;; ## Constructors

(defn from-tablets
  "Constructs a new partition from the given map of tablets. The argument should
  be a map from tablet keys (including `:base`) to tablet node ids."
  [store tablet-ids]
  (when-not (:base tablet-ids)
    (throw (ex-info "Cannot construct a partition without a base tablet"
                    {:tablets tablet-ids})))
  (let [base (node/get-data store (:base tablet-ids))
        records (vec (tablet/read base))]
    {:data/type :merkle-db/partition
     :merkle-db.data/count (count records)
     ::first-key (first (first (tablet/read base)))
     ::last-key (first (last (tablet/read base)))
     ::tablets (-> tablet-ids
                   (->>
                     (map (fn [[k id]]
                            [k {:id id
                                :fields (tablet/fields-present
                                          (node/get-data store id))}]))
                     (into {}))
                   (update :base dissoc :fields))}))



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


(defn- tablet-families
  "Returns a map of tablet keys to sets of fields contained in those tablets."
  [part]
  (into {}
        (map (juxt key (comp :fields val)))
        (dissoc (::tablets part) :base)))


(defn read-tablets
  "Performs a read across the tablets in the partition by selecting based on
  the desired fields. The reader function is called on each selected tablet
  along with any extra args, producing a collection of lazy record sequences
  which are combined into a single sequence of key/record pairs."
  [store part fields read-fn & args]
  (->> (set fields)
       (choose-tablets (tablet-families part))
       (map (comp :id (::tablets part)))
       (map (partial node/get-data store))
       (map #(apply read-fn % args))
       (record-seq)
       (map (juxt first #(select-keys (second %) fields)))))



;; ## Update Functions

(defn- append-record-updates
  [field->family updates [record-key data]]
  (->
    data
    (->>
      (reduce
        (fn split-data
          [acc [fk v]]
          (update acc (field->family fk :base) assoc fk v))
        {}))
    (update :base #(or % {}))
    (->>
      (reduce
        (fn assign-updates
          [updates [family fdata]]
          (update updates family (fnil conj []) [record-key fdata]))
        updates))))


(defn- update-tablets
  [store f tablets [family-key tablet-updates]]
  (let [tablet (or (node/get-data store (get-in tablets [family-key :id]))
                   (throw (ex-info (format "Couldn't find tablet %s in backing store"
                                           family-key)
                                   {:family family-key})))]
    (->> (tablet/add-records
           tablet
           (if (= :base family-key)
             (fn [k p n]
               (or (f k p n) {}))
             f)
           tablet-updates)
         (node/put! store)
         (node/meta-id)
         (assoc-in tablets [family-key :id]))))


(defn add-records
  "Performs an update across the tablets in the partition to merge in the given
  record data."
  [store part f records]
  (let [families (tablet-families part)
        field->family (reduce (fn [ff [family fields]]
                                (reduce #(assoc %1 %2 family) ff fields))
                              {} families)
        ; Update partition tablet map.
        tablets (->> records
                     (reduce (partial append-record-updates field->family) {})
                     (reduce (partial update-tablets store f) (::tablets part)))
        part' (assoc part
                     :merkle-db.data/count (count (tablet/read (node/get-data store (get-in tablets [:base :id]))))
                     ::tablets tablets
                     ;::membership (reduce bloom/add (::membership part) (keys records))
                     ::first-key (apply key/min (::first-key part) (keys records))
                     ::last-key (apply key/max (::last-key part) (keys records)))]
    (node/put! store part')))



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
