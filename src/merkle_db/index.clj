(ns merkle-db.index
  "The index tree in a table is a B+ tree which orders the partitions into a
  sorted tree structure."
  (:require
    [clojure.future :refer [pos-int?]]
    [clojure.spec :as s]
    [merkledag.link :as link]
    [merkle-db.data :as data]
    [merkle-db.key :as key]
    [merkle-db.node :as node]
    [merkle-db.partition :as part]))


(s/def ::height pos-int?)
(s/def ::keys (s/coll-of key/bytes? :kind vector?))
(s/def ::children (s/coll-of link/merkle-link? :kind vector?))


(s/def :merkle-db/index-node
  (s/and
    (s/keys :req [::data/count
                  ::height
                  ::keys
                  ::children])
    #(= (count (::children %))
        (inc (count (::keys %))))))
