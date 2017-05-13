(ns merkle-db.data
  (:require
    [clojure.future :refer [any? nat-int?]]
    [clojure.spec :as s]
    [merkledag.link :as link]))


;; Count of the records contained under a node.
(s/def ::count nat-int?)

;; Data size in bytes.
(s/def ::size nat-int?)

;; Valid field key values.
(s/def ::field-key any?)

;; Valid family keys.
(s/def ::family-key
  (s/and keyword? #(not= % :base)))

;; Map of family keywords to sets of contained fields.
(s/def ::families
  (s/and
    (s/map-of
      ::family-key
      (s/coll-of ::field-key :kind set?))
    #(= (reduce + (map count (vals %)))
        (count (distinct (apply concat (vals %)))))))

;; Link to user-supplied metadata.
(s/def ::metadata link/merkle-link?)

;; Instant point in time.
(s/def :time/instant #(instance? java.time.Instant %))

;; Time an entity was last modified.
(s/def :time/updated-at :time/instant)
