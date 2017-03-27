(ns merkle-db.partition
  (:refer-clojure :exclude [read])
  (:require
    [clojure.set :as set]
    [clojure.spec :as s]
    [merkle-db.key :as key]
    [merkle-db.tablet :as tablet]))


; TODO: pull in merkle-dag
(declare merkle-link?)


(s/def :merkle-db.data/count pos-int?)

; TODO: bloom filter for key membership

; TODO: should be persistent-bytes?
(s/def ::start-key bytes?)
(s/def ::end-key bytes?)

(s/def :merkle-db.data.partition.tablet/data merkle-link?)
(s/def :merkle-db.data.partition.tablet/fields (s/coll-of any? :kind set?))

(s/def ::tablets
  (s/map-of keyword? (s/keys :req-un [:merkle-db.data.partition.tablet/data]
                             :opt-un [:merkle-db.data.partition.tablet/fields])))

(s/def :merkle-db/partition
  (s/keys :req [:merkle-db.data/count
                ::start-key
                ::end-key
                ::tablets]))



;; ## Constructors

,,,



;; ## Read Functions

; read fields for all records
; read fields for a batch of ids
; read fields for a range of ids
; read fields for a slice of ids

(defn- choose-tablets
  "Selects a list of tablet names to query over, given a mapping of tablet
  names to sets of the contained fields and the desired set of field data. If
  selected-fields is nil, returns all tablets."
  [tablet-fields selected-fields]
  (if (empty? selected-fields)
    (if (nil? selected-fields)
      ; No selection provided, return all field data.
      (-> tablet-fields keys set (conj :base))
      ; Deliberately provided an empty collection; return no data.
      nil)
    ; Use field selection to filter tablets to load.
    (-> (dissoc tablet-fields :base)
        (->> (keep #(when (some selected-fields (val %)) (key %))))
        (set)
        (as-> chosen
          (if (seq (apply set/difference
                          selected-fields
                          (map tablet-fields chosen)))
            (conj chosen :base)
            chosen)))))


(defn- record-seq
  "Combines lazy sequences of partial records into a single lazy sequence
  containing key/data tuples."
  [field-seqs]
  (lazy-seq
    (when-let [next-key (some->> (seq (keep ffirst field-seqs))
                                 (apply key/min))]
      (cons [next-key
             (->> field-seqs
                  (keep #(when (= next-key (ffirst %))
                           (second (first %))))
                  (apply merge))]
            (record-seq
              (keep #(if (= next-key (ffirst %)) (next %) (seq %))
                    field-seqs))))))


(defn- tablet-fields
  "Returns a map of tablet keys to sets of fields contained in those tablets."
  [part]
  (into {}
        (map (juxt key (comp :fields val)))
        (::tablets part)))


#_
(defn read-tablets
  "Performs a read across the tablets in the partition by selecting based on
  the desired fields. The reader function is called on each selected tablet
  along with any extra args, producing a collection of lazy record sequences
  which are combined into a single sequence of key/record pairs."
  [node-store part fields read-fn & args]
  (->> (set fields)
       (choose-tablets (tablet-fields part))
       (map (comp :data (::tablets part)))
       (map (partial node/get-data node-store))
       (map tablet/read)
       (record-seq)
       (map (juxt first #(select-keys (second %) fields)))))



;; ## Update Functions

,,,



;; ## Deletion Functions

,,,
