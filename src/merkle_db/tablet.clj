(ns merkle-db.tablet
  "Functions for working with tablet data."
  (:refer-clojure :exclude [keys])
  (:require
    [clojure.spec :as s]
    [merkle-db.key :as key]))


;; Records are stored as a key/data pair.
(s/def ::record-entry
  (s/tuple key/bytes? map?))

;; Sorted vector of record entries.
(s/def ::records
  (s/coll-of ::record-entry :kind vector?))

;; Tablet node.
(s/def :merkle-db/tablet
  (s/keys :req [::records]))



;; ## Key Functions

(defn keys
  "Return a sequence of the keys in the tablet."
  [tablet]
  (map first (::records tablet)))


(defn first-key
  "Return the first record key present in the tablet."
  [tablet]
  (first (first (::records tablet))))


(defn last-key
  "Return the last record key present in the tablet."
  [tablet]
  (first (peek (::records tablet))))


(defn nth-key
  "Return the nth key present in the tablet data."
  [tablet n]
  (first (nth (::records tablet) n)))



;; ## Utilities

(defn from-records*
  "Constructs a new bare-bones tablet node."
  [records]
  {:data/type :merkle-db/tablet
   ::records (vec records)})


(defn fields-present
  "Scans the records in a tablet to determine the full set of fields present."
  [tablet]
  (set (mapcat (comp clojure.core/keys second) (::records tablet))))


(defn split
  "Split the tablet into two tablets at the given key. All records less than the
  split key will be contained in the first tablet, all others in the second. "
  [tablet split-key]
  (let [fkey (first-key tablet)
        lkey (last-key tablet)]
    (when-not (and (key/after? split-key fkey)
                   (key/before? split-key lkey))
      (throw (ex-info (format "Cannot split tablet with key %s which falls outside the contained range [%s, %s]"
                              split-key fkey lkey)
                      {:split-key split-key
                       :first-key fkey
                       :last-key lkey}))))
  (let [before-split? #(neg? (key/compare (first %) split-key))]
    [(->>
       (::records tablet)
       (take-while before-split?)
       (from-records*))
     (->>
       (::records tablet)
       (drop-while before-split?)
       (from-records*))]))


(defn join
  "Join two tablets into a single tablet. The tablets key ranges must not
  overlap."
  [left right]
  (let [left-bound (last-key left)
        right-bound (first-key right)]
    (when (key/before? right-bound left-bound)
      (throw (ex-info (format "Cannot join tablets with overlapping key ranges: %s > %s"
                              left-bound right-bound)
                      {:left-bound left-bound
                       :right-bound right-bound}))))
  (update left ::records (comp vec concat) (::records right)))



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



;; ## Update Functions

(defn merge-fields
  "Merge updated fields from the `right` map into the `left` map, dropping any
  fields which are nil-valued."
  [_ left right]
  (->> (merge left right)
       (remove (comp nil? val))
       (into {})
       (not-empty)))


(defn from-records
  "Construct a tablet from a collection of record keys and field data."
  ([records]
   (from-records merge-fields records))
  ([f records]
   (->>
     records
     (map (fn apply-f [[k v]] [k (or (f k nil v) {})]))
     (sort-by first key/compare)
     (from-records*))))


(defn update-records
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
                      (set (map first records))
                      (set (map first (::records tablet))))))
       (sort-by first key/compare)
       (vec)
       (assoc tablet ::records)))



;; ## Deletion Functions

(defn prune-records
  "Update a tablet by removing empty records from the data."
  [tablet]
  (update tablet ::records #(vec (remove (comp empty? second) %))))


(defn remove-batch
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


(defn remove-range
  "Update the tablet by removing a range of record keys from it. Returns nil
  if the resulting tablet is empty."
  [tablet start-key end-key]
  (->
    (into
      []
      (remove (fn in-range?
                [[key-bytes data]]
                (and (or (nil? start-key)
                         (not (key/before? key-bytes start-key)))
                     (or (nil? end-key)
                         (not (key/after? key-bytes end-key))))))
      (::records tablet))
    (as-> records
      (when (seq records)
        (assoc tablet ::records records)))))
