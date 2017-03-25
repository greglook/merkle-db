(ns merkle-db.tablet
  "Functions for working with tablet data."
  (:require
    [clojure.spec :as s]))


(s/def ::record-entry
  (s/tuple bytes? map?))

(s/def ::records
  (s/coll-of ::record-entry :kind vector?))

(s/def :merkle-db/tablet
  (s/keys :req [::records]))


(def empty-tablet
  {:data/type :merkle-db.v0/tablet
   ::records []})



;; ## Utility Functions

(defn- entry->record
  "Convert a key-bytes/field-map entry pair into a map with metadata applied."
  [[key-bytes data]]
  (vary-meta data assoc ::record-id key-bytes))



;; ## Read Functions

(defn read-batch
  "Read a lazy sequence of maps which contains the field data for the records
  whose keys are in the given collection."
  [tablet record-keys]
  ; OPTIMIZE: divide up the range by binary-searching for keys in the batch.
  (->> (::records tablet)
       (filter (comp (set record-keys) first))  ; FIXME: won't work with raw byte arrays
       (map entry->record)))


(defn read-range
  "Read a lazy sequence of maps which contains the field data for the records
  whose keys lie in the given range, inclusive. A nil boundary includes all
  records in that range direction."
  [tablet start-key end-key]
  ; OPTIMIZE: binary-search to the starting point and then iterate.
  (->>
    (cond->> (::records tablet)
      start-key
        (drop-while #(not (neg? (compare start-key (first %)))))  ; FIXME: use lexical byte comparator
      end-key
        (take-while #(not (pos? (compare end-key (first %))))))
    (map entry->record)))


(defn read-slice
  "Read a lazy sequence of maps which contain the field data for the records
  whose indices lie in the given range, inclusive. A nil boundary includes all
  records in that range direction."
  [tablet start-index end-index]
  ; OPTIMIZE: input is a vector, so find a way to get a seq starting at the
  ; desired index.
  (->>
    (cond->> (::records tablet)
      start-index
        (drop start-index)
      end-index
        (take (- (inc end-index) (or start-index 0))))
    (map entry->record)))



;; ## Update Functions

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
       (sort-by first compare) ; FIXME: use lexical comparator
       (assoc tablet ::records)))



;; ## Deletion Functions

(defn remove-batch
  "Update the tablet by removing certain record keys from it. Returns nil if
  the resulting tablet is empty."
  [tablet record-keys]
  (->
    (->>
      (::records tablet)
      (remove (comp (set record-keys) first))   ; FIXME: won't work with raw byte arrays
      (vec))
    (as-> records
      (when (seq records)
        (assoc tablet ::records records)))))
