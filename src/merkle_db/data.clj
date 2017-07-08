(ns merkle-db.data
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

;; Valid field key values.
(s/def ::field-key any?)

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

;; Instant point in time.
(s/def :time/instant #(instance? Instant %))

;; Time an entity was last modified.
(s/def :time/updated-at :time/instant)



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
