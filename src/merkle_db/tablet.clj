(ns merkle-db.tablet
  "Functions for working with tablet data."
  (:refer-clojure :exclude [read])
  (:require
    [clojure.spec :as s]
    [merkle-db.key :as key]))


(s/def ::record-entry
  (s/tuple key/bytes? map?))

(s/def ::records
  ; TODO: should also be sorted, but that may be better to test separately.
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

(defn read
  "Read a lazy sequence of key/map tuples which contain the field data for all
  the records in the tablet."
  [tablet]
  (seq (::records tablet)))


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
  [record-key left right]
  (->> (merge left right)
       (remove (comp nil? val))
       (into {})))


(defn update-batch
  "Update the tablet by adding certain record data to it. For each record in
  the `records` map, the function `f` will be called with the record key, the
  old data (or nil, if the key is absent), and the new data. The result will be
  used as the new data for that record."
  [tablet f records]
  ; OPTIMIZE: do this in one pass instead of sorting
  (->> (::records tablet)
       (map (fn [[k v]]
              [k (if-let [updates (get records k)]
                   (f k v updates)
                   v)]))
       (concat (map #(vector % (f % nil (get records %)))
                    (clojure.set/difference
                      (set (keys records))
                      (set (map first (::records tablet))))))
       (sort-by first key/compare)
       (assoc tablet ::records)))



;; ## Deletion Functions

(defn remove-batch
  "Update the tablet by removing certain record keys from it. Returns nil if
  the resulting tablet is empty."
  [tablet record-keys]
  ; TODO: record-keys must be a collection of PersistentByte objects
  (->
    (->>
      (::records tablet)
      (remove (comp (set record-keys) first))
      (vec))
    (as-> records
      (when (seq records)
        (assoc tablet ::records records)))))
