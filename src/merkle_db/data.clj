(ns merkle-db.data
  (:require
    [clojure.future :refer [any? nat-int?]]
    [clojure.spec :as s]))


;; Count of the records contained under a node.
(s/def ::count nat-int?)

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
