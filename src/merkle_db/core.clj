(ns merkle-db.core
  (:require
    [merkledag.link :as link]
    [merkledag.refs :as refs]
    [merkledag.refs.memory :refer [memory-ref-tracker]]
    [merkle-db.data :as data]
    [merkle-db.db :as db]
    [merkle-db.node :as node]))



;; Test Implementation

(defn mem-connection
  []
  (map->Connection
    {:store (node/memory-node-store)
     :tracker (memory-ref-tracker)}))
