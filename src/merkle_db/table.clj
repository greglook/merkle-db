(ns merkle-db.table
  (:require
    [clojure.future :refer [pos-int?]]
    [clojure.spec :as s]
    [merkledag.link :as link]
    [merkle-db.data :as data]
    [merkle-db.key :as key]
    [merkle-db.partition :as part]))


(s/def ::data link/merkle-link?)
(s/def ::patch link/merkle-link?)
(s/def ::branching-factor (s/and pos-int? #(< 2 %)))

(s/def :merkle-db/table-root
  (s/keys :req [::data/count]
          :opt [::data
                ::patch
                ::branching-factor
                ::data/families
                ::data/metadata
                ::key/lexicoder
                ::part/limit
                :time/updated-at]))
