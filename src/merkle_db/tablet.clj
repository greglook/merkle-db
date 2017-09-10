(ns merkle-db.tablet
  "Functions for working with tablet data."
  (:refer-clojure :exclude [keys])
  (:require
    [clojure.spec :as s]
    [clojure.string :as str]
    (merkle-db
      [key :as key]
      [record :as record]
      [validate :as validate])))


;; ## Specs

(def ^:const data-type
  "Value of `:data/type` that indicates a tablet node."
  :merkle-db/tablet)

;; Sorted vector of record entries.
(s/def ::records
  (s/coll-of ::record/entry :kind vector?))

;; Tablet node.
(s/def ::node-data
  (s/and
    (s/keys :req [::records])
    #(= data-type (:data/type %))))


(defn validate
  [params tablet]
  (when (validate/check :data/type
          (= data-type (:data/type tablet))
          "Node data type should be correct")
    (validate/check ::spec
      (s/valid? ::node-data tablet)
      (s/explain-str ::node-data tablet))
    (validate/check ::record/count
      (seq (::records tablet))
      "Tablet should not be empty")
    (when-let [family-keys (get (::record/families params)
                                (::record/family-key params))]
      (let [bad-fields (->> (::records tablet)
                            (mapcat (comp clojure.core/keys second))
                            (remove (set family-keys)))]
        (validate/check ::record/families
          (empty? bad-fields)
          (format "Tablet record data should only contain values for fields in family %s (%s)"
                  (::record/family-key params)
                  family-keys))))
    ; TODO: all records are within partition boundary
    ; TODO: records are sorted by key
    ; TODO: all records are readable?
    ))



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

(defn from-records
  "Constructs a new bare-bones tablet node. Does not ensure that the records
  are sorted."
  [records]
  (when (seq (filter (comp map? second) records))
    {:data/type data-type
     ::records (vec records)}))


(defn fields-present
  "Scans the records in a tablet to determine the full set of fields present."
  [tablet]
  (set (mapcat (comp clojure.core/keys second) (::records tablet))))


#_
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
  (->> (::records tablet)
       (split-with #(key/before? (first %) split-key))
       (mapv from-records)))


(defn join
  "Join two tablets into a single tablet. The tablets key ranges must not
  overlap."
  [left right]
  (let [left-bound (last-key left)
        right-bound (first-key right)]
    (when-not (key/before? left-bound right-bound)
      (throw (ex-info (format "Cannot join tablets with overlapping key ranges: %s > %s"
                              left-bound right-bound)
                      {:left-bound left-bound
                       :right-bound right-bound}))))
  (update left ::records into (::records right)))



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
      (drop-while #(key/before? (first %) start-key))
    end-key
      (take-while #(not (key/after? (first %) end-key)))))



;; ## Update Functions

(defn update-records
  "Update a tablet by inserting the records in `additions` (a collection of
  key/data map entries) and removing the records whose keys are in
  `deletions` (a collection of record keys)."
  [tablet additions deletions]
  ; OPTIMIZE: do this in one pass instead of building maps.
  (when-let [records (-> (sorted-map)
                         (into (::records tablet))
                         (into additions)
                         (cond->
                           (seq deletions)
                             (as-> rs (apply dissoc rs deletions)))
                         (seq))]
    (assoc tablet ::records (vec records))))


(defn prune
  "Update a tablet by removing empty records from the data."
  [tablet]
  (update tablet ::records #(vec (remove (comp empty? second) %))))


#_
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
