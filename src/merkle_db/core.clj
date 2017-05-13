(ns merkle-db.core
  (:require
    [merkledag.link :as link]
    [merkledag.refs :as refs]
    [merkledag.refs.memory :refer [memory-ref-tracker]]
    [merkle-db.connection :as conn]
    [merkle-db.data :as data]
    [merkle-db.db :as db]
    [merkle-db.node :as node]))



;; Test Implementation

(defn mem-connection
  []
  (merkle_db.connection.Connection.
    (node/memory-node-store)
    (memory-ref-tracker)))
