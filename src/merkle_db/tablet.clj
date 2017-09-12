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

(defn validate
  [tablet params]
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
                            (remove (set family-keys))
                            (set))]
        (validate/check ::record/families
          (empty? bad-fields)
          (format "Tablet record data should only contain values for fields in family %s (%s)"
                  (::record/family-key params)
                  family-keys))))
    (when-let [boundary (::record/first-key params)]
      (validate/check ::record/first-key
        (not (key/before? (first-key tablet) boundary))
        "First key in partition is within the subtree boundary"))
    (when-let [boundary (::record/last-key params)]
      (validate/check ::record/last-key
        (not (key/after? (last-key tablet) boundary))
        "Last key in partition is within the subtree boundary"))
    ; TODO: records are sorted by key
    ))


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

(defn prune
  "Update a tablet by removing empty records from the data."
  [tablet]
  (update tablet ::records #(vec (remove (comp empty? second) %))))


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
