(ns merkle-db.node
  "Shim namespace to flesh out MerkleDAG requirements."
  (:require
    [merkledag.link :as link]
    [multihash.core :as multihash]
    [multihash.digest :as digest]))


(defn meta-id
  [data]
  (:merkledag.node/id (meta data)))


(defn meta-links
  [data]
  (:merkledag.node/links (meta data)))


(defprotocol Identifiable
  "Protocol for values which can be resolved to a multihash id."

  (identify
    [value]
    "Return the multihash identifying this value."))


(extend-protocol Identifiable

  multihash.core.Multihash
  (identify [x] x)

  blocks.data.Block
  (identify [x] (:id x))

  merkledag.link.MerkleLink
  (identify [x] (:target x)))


(defprotocol NodeStore
  "..."

  (-get-node
    [store id]
    "...")

  (-store-node!
    [store links data]
    "...")

  (-delete-node!
    [store id]
    "..."))


(defn get-links
  [store id]
  (let [id (identify id)]
    (some->
      (-get-node store id)
      (:links)
      (vary-meta assoc :merkledag.node/id id))))


(defn get-data
  [store id]
  (let [id (identify id)
        node (-get-node store id)]
    (some->
      (:data node)
      (vary-meta assoc
        :merkledag.node/id id
        :merkledag.node/links (:links node)))))


(defn store-node!
  ([store data]
   (store-node! store nil data))
  ([store links data]
   (-store-node! store links data)))


(defn delete-node!
  [store id]
  (-delete-node! store (identify id)))


(defrecord MemoryNodeStore
  [memory]

  NodeStore

  (-get-node
    [this id]
    (get @memory id))

  (-store-node!
    [this links data]
    (let [links (link/collect-table links data)
          id (digest/sha2-256 (pr-str [links data]))
          node {:id id, :links links, :data data}]
      (swap! memory assoc id node)
      node))

  (-delete-node!
    [this id]
    (let [existed? (contains? @memory id)]
      (swap! memory dissoc id)
      existed?)))


(defn memory-node-store
  []
  (->MemoryNodeStore (atom (sorted-map))))
