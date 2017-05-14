(ns merkle-db.table
  "Tables are named top-level containers of records. Generally, a single table
  corresponds to a certain 'kind' of data. Tables also contain configuration
  determining how keys are encoded and records are stored."
  (:require
    [clojure.future :refer [pos-int?]]
    [clojure.spec :as s]
    [merkledag.link :as link]
    [merkle-db.data :as data]
    [merkle-db.index :as index]
    [merkle-db.key :as key]
    [merkle-db.partition :as part]))


;; ## Specs

;; Table names are non-empty strings.
(s/def ::name (s/and string? #(<= 1 (count %) 127)))

;; Table data is a link to the root of the data tree.
(s/def ::data link/merkle-link?)

;; Tables may have a patch tablet containing recent unmerged data.
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

; TODO: table description


; IDEA: reified table-as-collection type
