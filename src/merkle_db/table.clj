(ns merkle-db.table
  (:require
    [clojure.future :refer [pos-int?]]
    [clojure.spec :as s]
    [merkledag.link :as link]
    [merkle-db.data :as data]
    [merkle-db.index :as index]
    [merkle-db.key :as key]
    [merkle-db.partition :as part]))


;; Table names
(s/def ::name (s/and string? #(<= 1 (count %) 127)))

;; Link to the root of the data tree.
(s/def ::data link/merkle-link?)

;; Patch tablet containing recent unmerged data.
(s/def ::patch link/merkle-link?)

;; Table root node.
(s/def :merkle-db/table-root
  (s/keys :req [::data/count
                ::index/branching-factor
                ::part/limit]
          :opt [::data
                ::patch
                ::data/families
                ::data/metadata
                ::key/lexicoder
                ::part/limit
                :time/updated-at]))
