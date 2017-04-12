(ns merkle-db.data
  (:require
    [clojure.future :refer [any? nat-int?]]
    [clojure.spec :as s]))


;; Count of the records contained under a node.
(s/def ::count nat-int?)

;; Valid field key values.
(s/def ::field-key any?)

;; Map of family keywords to sets of contained fields.
(s/def ::families
  (s/map-of
    (s/and keyword? #(not= % :base))
    (s/coll-of ::field-key :kind set?)))
