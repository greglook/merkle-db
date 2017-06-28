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
    [merkle-db.partition :as part])
  (:import
    java.time.Instant))


;; ## Specs

;; Table names are non-empty strings.
(s/def ::name (s/and string? #(<= 1 (count %) 127)))

;; Table data is a link to the root of the data tree.
(s/def ::data link/merkle-link?)

;; Tables may have a patch tablet containing recent unmerged data.
(s/def ::patch link/merkle-link?)

;; Table root node.
(s/def ::node-data
  (s/keys :req [::data/count
                ::index/branching-factor
                ::part/limit]
          :opt [::data
                ::patch
                ::data/families
                ::key/lexicoder
                :time/updated-at]))

; TODO: table description



;; ## ...

(defn root-data
  "Construct a map for a new table root node."
  [opts]
  (merge {::index/branching-factor 256
          ::part/limit 100000}
          opts
          {::data/count 0
           :time/updated-at (Instant/now)}))
