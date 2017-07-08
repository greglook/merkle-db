(ns merkle-db.patch
  "Patches are applied on top of tables in order to efficiently store changes
  while re-using old indexed data."
  (:require
    [clojure.future :refer [pos-int?]]
    [clojure.spec :as s]
    (merkle-db
      [key :as key])))


(def ^:const data-type
  "Value of `:data/type` that indicates a tablet node."
  :merkle-db/patch)

(def tombstone
  "Value which marks the deletion of a record."
  ::tombstone)


(defn tombstone?
  "Returns true if the value x is a tombstone."
  [x]
  (identical? ::tombstone x))



;; ## Specs

;; Maximum number of changes to allow in a patch tablet.
(s/def ::limit pos-int?)

;; Records are stored as a key/data-or-tombstone pair.
(s/def ::change-entry
  (s/tuple key/bytes? (s/or :record map? :tombstone tombstone?)))

;; Sorted vector of record entries.
(s/def ::changes
  (s/coll-of ::change-entry :kind vector?))

;; Tablet node.
(s/def :merkle-db/patch
  (s/keys :req [::changes]))



;; ## Utility Functions

; (merge-patch patch-tablet pending) => patch-tablet
; (apply-patch data-tree pending) => data-link


(defn remove-tombstones
  "Returns a lazy sequence of record entries with tombstoned records removed."
  [rs]
  (remove (comp tombstone? second) rs))


(defn filter-patch
  "Takes a full set of patch data and returns a cleaned sequence based on the
  given options."
  [patch opts]
  (when (seq patch)
    (cond->> patch
      (:start-key opts)
        (drop-while #(neg? (key/compare (first %) (:start-key opts))))
      (:end-key opts)
        (take-while #(not (neg? (key/compare (:end-key opts) (first %)))))
      (:fields opts)
        (map (fn [[k r]]
               [k (if (map? r) (select-keys r (:fields opts)) r)])))))


(defn patch-seq
  "Combines an ordered sequence of patch data with a lazy sequence of record
  keys and data. Any records present in the patch will appear in the output
  sequence, replacing any equivalent keys from the sequence."
  [patch records]
  (lazy-seq
    (cond
      ; No more patch data, return records directly.
      (empty? patch)
        records
      ; No more records, return patch with tombstones removed.
      (empty? records)
        (remove (comp tombstone? second) patch)
      ; Next key is in both patch and records.
      (= (ffirst patch) (ffirst records))
        (cons (first patch) (patch-seq (next patch) (next records)))
      ; Next key is in patch, not in records.
      (key/before? (ffirst patch) (ffirst records))
        (cons (first patch) (patch-seq (next patch) records))
      ; Next key is in records, not in patch.
      :else
        (cons (first records) (patch-seq patch (next records))))))
