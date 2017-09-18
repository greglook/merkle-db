(ns merkle-db.patch
  "Patches are applied on top of tables in order to efficiently store changes
  while re-using old indexed data."
  (:require
    [clojure.future :refer [pos-int?]]
    [clojure.spec :as s]
    (merkle-db
      [key :as key]
      [record :as record])))


(def ^:const data-type
  "Value of `:data/type` that indicates a patch tablet node."
  :merkle-db/patch)


(def default-limit
  "The default number of changes to hold in a patch tablet."
  100)


(defn tombstone?
  "Returns true if the value is a tombstone."
  [x]
  (identical? ::tombstone x))


;; Maximum number of changes to allow in a patch tablet.
(s/def ::limit pos-int?)

;; Records are stored as a key/data-or-tombstone pair.
(s/def ::entry
  (s/tuple ::record/key (s/or :data ::record/data :tombstone tombstone?)))

;; Sorted vector of record entries.
(s/def ::changes
  (s/coll-of ::entry :kind vector?))

;; Patch tablet node data.
(s/def ::node-data
  (s/and
    (s/keys :req [::changes])
    #(= data-type (:data/type %))))



;; ## Construction

(defn from-changes
  "Construct a patch tablet node data from a map of change data."
  [changes]
  {:data/type data-type
   ::changes (vec changes)})



;; ## Utility Functions

(defn remove-tombstones
  "Returns a lazy sequence of record entries with tombstoned records removed."
  [rs]
  (remove (comp tombstone? second) rs))


(defn filter-changes
  "Takes a full set of patch data and returns a filtered sequence based on the
  given options."
  [patch opts]
  (when (seq patch)
    (cond->> patch
      (:start-key opts)
        (drop-while #(key/before? (first %) (:start-key opts)))
      (:end-key opts)
        (take-while #(not (key/after? (first %) (:end-key opts))))
      (:fields opts)
        (map (fn [[k r]]
               [k (if (map? r) (select-keys r (:fields opts)) r)])))))


(defn patch-seq
  "Combines an ordered sequence of patch data with a lazy sequence of record
  keys and data. Any records present in the patch will appear in the output
  sequence, replacing any equivalent keys from the sequence. If the changed
  value is a tombstone, the record will not appear in the sequence."
  [patch records]
  (letfn [(maybe-cons
            [[patch-key patch-val :as entry] more-records]
            (let [tail (patch-seq (next patch) more-records)]
              (if (tombstone? patch-val) tail (cons entry tail))))]
    (lazy-seq
      (cond
        ; No more patch data, return records directly.
        (empty? patch)
          records
        ; No more records, return remaining patch data.
        (empty? records)
          patch
        ; Next key is in both patch and records.
        (= (ffirst patch) (ffirst records))
          (maybe-cons (first patch) (next records))
        ; Next key is in patch, not in records.
        (key/before? (ffirst patch) (ffirst records))
          (maybe-cons (first patch) records)
        ; Next key is in records, not in patch.
        :else
          (cons (first records) (patch-seq patch (next records)))))))
