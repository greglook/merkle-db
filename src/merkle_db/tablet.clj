(ns merkle-db.tablet
  "Functions for working with tablet data."
  (:refer-clojure :exclude [keys])
  (:require
    [clojure.spec :as s]
    [clojure.string :as str]
    [merkle-db.key :as key]
    [merkle-db.record :as record]))


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



;; ## Construction

(defn from-records
  "Constructs a new tablet node. Does not ensure that the records are sorted."
  [records]
  (when (seq (filter (comp map? second) records))
    {:data/type data-type
     ::records (vec records)}))


(defn join
  "Join two tablets into a single tablet. The tablets key ranges must not
  overlap."
  [left right]
  (if (and left right)
    (let [left-bound (last-key left)
          right-bound (first-key right)]
      (when-not (key/before? left-bound right-bound)
        (throw (ex-info (format "Cannot join tablets with overlapping key ranges: %s > %s"
                                left-bound right-bound)
                        {:left-bound left-bound
                         :right-bound right-bound})))
      (update left ::records into (::records right)))
    (or left right)))


(defn prune
  "Update a tablet by removing empty records from the data."
  [tablet]
  (update tablet ::records (partial into [] (remove (comp empty? second)))))



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
