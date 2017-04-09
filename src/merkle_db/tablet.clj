(ns merkle-db.tablet
  "Functions for working with tablet data."
  (:refer-clojure :exclude [merge])
  (:require
    [clojure.spec :as s]
    [merkle-db.key :as key]))


(s/def ::record-entry
  (s/tuple key/bytes? map?))

(s/def ::records
  (s/coll-of ::record-entry :kind vector?))

(s/def :merkle-db/tablet
  (s/keys :req [::records]))



;; ## Constructors

(def empty-tablet
  "An empty tablet data value."
  {:data/type :merkle-db/tablet
   ::records []})


(defn from-records
  "Construct a tablet from a map of record keys to field data."
  [records]
  (assoc empty-tablet ::records (vec (sort-by key key/compare records))))



;; ## Read Functions

(defn read-all
  "Read a sequence of key/map tuples which contain the field data for all the
  records in the tablet."
  [tablet]
  (::records tablet))


(defn read-batch
  "Read a lazy sequence of key/map tuples which contain the field data for the
  records whose keys are in the given collection."
  [tablet record-keys]
  ; TODO: record-keys must be a collection of PersistentByte objects
  ; OPTIMIZE: divide up the range by binary-searching for keys in the batch.
  (filter (comp (set record-keys) first) (::records tablet)))


(defn read-range
  "Read a lazy sequence of key/map tuples which contain the field data for the
  records whose keys lie in the given range, inclusive. A nil boundary includes
  all records in that range direction."
  [tablet start-key end-key]
  ; OPTIMIZE: binary-search to the starting point and then iterate.
  (cond->> (::records tablet)
    start-key
      (drop-while #(neg? (key/compare (first %) start-key)))
    end-key
      (take-while #(not (neg? (key/compare end-key (first %)))))))


;; NOTE: slice reads work at the tablet level, but are problematic when extended
;; to partitions. Not every tablet will have the full (or the same) set of
;; record entries, so without knowing the set of keys that fall into the
;; canonical slice in the base tablet, pulling data out of family tablets
;; becomes more complicated. Effectively, the code would need to read the base
;; tablet to discover the start and end keys for the slice, then do a
;; read-range on the family tablets for those keys.
#_
(defn read-slice
  "Read a lazy sequence of key/map tuples which contain the field data for the
  records whose indices lie in the given range, inclusive. A nil boundary
  includes all records in that range direction."
  [tablet start-index end-index]
  ; OPTIMIZE: input is a vector, so find a way to get a seq starting at the
  ; desired index.
  (cond->> (::records tablet)
    start-index
      (drop start-index)
    end-index
      (take (- (inc end-index) (or start-index 0)))))



;; ## Update Functions

(defn merge-fields
  "Merge updated fields from the `right` map into the `left` map, dropping any
  fields which are nil-valued."
  [_ left right]
  (->> (clojure.core/merge left right)
       (remove (comp nil? val))
       (into {})
       (not-empty)))


(defn merge-records
  "Update a tablet by merging record data into it.

  For each record in the `records` map, the function `f` will be called with
  the record key, the old data (or nil, if the key is absent), and the new
  data. The result will be used as the new data for that record. Nil results
  are promoted to empty record maps."
  [tablet f records]
  ; OPTIMIZE: do this in one pass instead of sorting
  (->> (::records tablet)
       (map (fn [[k v]]
              [k (if-let [updates (get records k)]
                   (or (f k v updates) {})
                   v)]))
       (concat (map #(vector % (or (f % nil (get records %)) {}))
                    (clojure.set/difference
                      (set (keys records))
                      (set (map first (::records tablet))))))
       (sort-by first key/compare)
       (vec)
       (assoc tablet ::records)))



;; ## Deletion Functions

(defn remove-records
  "Update the tablet by removing certain record keys from it. Returns nil if
  the resulting tablet is empty."
  [tablet record-keys]
  {:pre [(every? key/bytes? record-keys)]}
  (->
    (->>
      (::records tablet)
      (remove (comp (set record-keys) first))
      (vec))
    (as-> records
      (when (seq records)
        (assoc tablet ::records records)))))


(defn prune-records
  "Update a tablet by removing empty records from the data."
  [tablet]
  (update tablet ::records #(vec (remove (comp empty? second) %))))



;; ## Utilities

(defn fields-present
  "Scans the records in a tablet to determine the full set of fields present."
  [tablet]
  (set (mapcat (comp keys second) (::records tablet))))


(defn first-key
  "Return the first record key present in the tablet."
  [tablet]
  (first (first (::records tablet))))


(defn last-key
  "Return the last record key present in the tablet."
  [tablet]
  (first (peek (::records tablet))))


(defn split
  "Split the tablet into two tablets at the given key. All records less than the
  split key will be contained in the first tablet, all others in the second. "
  [tablet split-key]
  (let [fkey (first-key tablet)
        lkey (last-key tablet)]
    (when-not (and (key/after? split-key fkey)
                   (key/before? split-key lkey))
      (throw (ex-info (format "Cannot split tablet with key %s which falls outside the record range [%s, %s]"
                              split-key fkey lkey)
                      {:split-key split-key
                       :first-key fkey
                       :last-key lkey}))))
  (let [before-split? #(neg? (key/compare % split-key))]
    [(->>
       (::records tablet)
       (take-while before-split?)
       (vec)
       (assoc empty-tablet ::records))
     (->>
       (::records tablet)
       (drop-while before-split?)
       (vec)
       (assoc empty-tablet ::records))]))


(defn merge
  "Merge the two tablets into a single tablet. The tablets key ranges must not
  overlap."
  [left right]
  ; TODO: validate that left and right don't overlap
  ; TODO: implement
  (throw (UnsupportedOperationException. "NYI")))
