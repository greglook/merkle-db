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

  nil
  (identify [_] nil)

  multihash.core.Multihash
  (identify [m] m)

  blocks.data.Block
  (identify [b] (:id b))

  merkledag.link.MerkleLink
  (identify [l] (:target l)))


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
  (when-let [id (identify id)]
    (some->
      (-get-node store id)
      (:links)
      (vary-meta assoc :merkledag.node/id id))))


(defn get-data
  [store id]
  (when-let [id (identify id)]
    (let [node (-get-node store id)]
      (some->
        (:data node)
        (vary-meta assoc
          :merkledag.node/id id
          :merkledag.node/links (:links node))))))


(defn store-node!
  ([store data]
   (store-node! store nil data))
  ([store links data]
   (-store-node! store links data)))


(defn delete-node!
  [store id]
  (when-let [id (identify id)]
    (-delete-node! store id)))


(defrecord MemoryNodeStore
  [memory]

  NodeStore

  (-get-node
    [this id]
    (get @memory id))

  (-store-node!
    [this links data]
    (let [links (link/collect-table links data)
          content (pr-str [links data])
          id (digest/sha2-256 content)
          node {:id id, :size (count content), :links links, :data data}]
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
