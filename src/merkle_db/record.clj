(ns merkle-db.record
  "Core record specs and functions."
  (:require
    [clojure.future :refer [any? nat-int?]]
    [clojure.spec :as s]
    [clojure.string :as str]
    [merkledag.link :as link]
    [merkle-db.bloom :as bloom]
    [merkle-db.key :as key])
  (:import
    java.time.Instant
    java.time.format.DateTimeFormatter
    merkle_db.bloom.BloomFilter
    merkle_db.key.Key))


;; Count of the records contained under a node.
(s/def ::count nat-int?)

;; Data size in bytes.
(s/def ::size nat-int?)

;; Record key bytes.
(s/def ::key key/key?)

;; Valid field key values.
(s/def ::field-key any?)

;; Map of record field data.
(s/def ::data (s/map-of ::field-key any?))

;; Record key/data tuple.
(s/def ::entry (s/tuple ::key ::data))

;; Valid family keys.
(s/def ::family-key keyword?)

;; Map of family keywords to sets of contained fields.
(s/def ::families
  (s/and
    (s/map-of
      ::family-key
      (s/coll-of ::field-key :kind set?))
    #(= (reduce + (map count (vals %)))
        (count (distinct (apply concat (vals %)))))))



;; ## Codecs

(def codec-types
  "Map of codec type information that can be used with MerkleDAG stores."
  {'inst
   {:description "An instant in time."
    :reader #(Instant/parse ^String %)
    :writers {Instant #(.format DateTimeFormatter/ISO_INSTANT ^Instant %)}}

   'merkle-db/key
   {:description "Record key byte data."
    :reader key/parse
    :writers {Key key/hex}}

   'merkle-db/bloom-filter
   {:description "Probablistic Bloom filter."
    :reader bloom/form->filter
    :writers {BloomFilter bloom/filter->form}}})



;; ## Family Functions

(defn- family-lookup
  "Build a lookup function for field families. Takes a map from family keys to
  collections of field keys in that family. Returns a function which accepts a
  field key and returns either the corresponding family or `:base` if it is not
  assigned one."
  [families]
  (let [lookup (into {}
                     (mapcat #(map vector (second %) (repeat (first %))))
                     families)]
    (fn [field-key] (lookup field-key :base))))


(defn family-groups
  "Build a map from family keys to maps which contain the field data for the
  corresponding family. Fields not grouped in a family will be added to
  `:base`. Families which had no data will have an entry with a `nil` value,
  except `:base` which will be an empty map."
  [families data]
  (let [field->family (family-lookup families)
        init (assoc (zipmap (keys families) (repeat nil))
                    :base {})]
    (reduce-kv
      (fn split-data
        [groups field value]
        (update groups (field->family field) assoc field value))
      init data)))


(defn split-data
  "Split new record values into collections grouped by family. Each configured
  family and `:base` will have an entry in the resulting map, containing a
  sorted map of record keys to new values. The values may be `nil` if the
  record had no data for that family, an empty map if the family is `:base` and
  all the record's data is in other families, or a map of field data belonging
  to that family.

  ```
  {:base {#merkle-db/key \"00\" {:a 123}, ...}
   :bc {#merkle-db/key \"00\" {:b true, :c \"cat\"}, ...}
   ...}
  ```"
  [families records]
  (reduce
    (fn append-updates
      [updates [record-key data]]
      (reduce-kv
        (fn assign-updates
          [updates family fdata]
          (update updates family (fnil assoc (sorted-map)) record-key fdata))
        updates
        (family-groups families data)))
    {} records))



;; ## Utilities

(defn partition-approx
  "Returns a lazy sequence of `n` lists containing the elements of `coll` in
  order, where each list is approximately the same size."
  [n cnt coll]
  (let [n (min n cnt)]
    (when (pos? cnt)
      (->>
        [nil
         (->> (range (inc n))
              (map #(int (* (/ % n) cnt)))
              (partition 2 1))
         coll]
        (iterate
          (fn [[_ [[start end :as split] & splits] xs]]
            (when-let [length (and split (- end start))]
              [(seq (take length xs))
               splits
               (drop length xs)])))
        (drop 1)
        (take-while first)
        (map first)))))


(defn partition-limited
  "Returns a sequence of partitions of the elements of `coll` such that:
  - No partition has more than `limit` elements
  - A minimum number of partitions is returned
  - Partitions are approximately equal in size

  Note that this counts the collection unless it's provided."
  ([limit coll]
   (partition-limited limit (count coll) coll))
  ([limit cnt coll]
   (partition-approx (int (Math/ceil (/ cnt limit))) cnt coll)))
