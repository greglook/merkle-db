(ns merkle-db.table
  (:require
    [clojure.future :refer [pos-int?]]
    [clojure.spec :as s]
    [merkledag.link :as link]
    [merkle-db.data :as data]
    [merkle-db.key :as key]
    [merkle-db.partition :as part]))


;; Link to the root of the data tree.
(s/def ::data link/merkle-link?)

;; Patch tablet containing recent unmerged data.
(s/def ::patch link/merkle-link?)

;; Maximum number of children an index node in the data tree can have.
(s/def ::branching-factor (s/and pos-int? #(< 2 %)))

;; Table root node.
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
