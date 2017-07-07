(ns merkle-db.data
  (:require
    [clojure.future :refer [any? nat-int?]]
    [clojure.spec :as s]
    [clojure.string :as str]
    [merkledag.link :as link])
  (:import
    blocks.data.PersistentBytes
    java.time.Instant
    java.time.format.DateTimeFormatter))


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

   'data/bytes
   {:description "Immutable byte sequence."
    :reader #(PersistentBytes/wrap (javax.xml.bind.DatatypeConverter/parseHexBinary %))
    :writers {PersistentBytes #(-> (.toByteArray ^PersistentBytes %)
                                   (javax.xml.bind.DatatypeConverter/printHexBinary)
                                   (str/lower-case))}}})
