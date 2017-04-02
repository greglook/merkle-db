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


(defprotocol NodeStore
  "..."

  (get-links
    [store id]
    "...")

  (get-data
    [store id]
    "...")

  (put!
    [store data]
    [store links data]
    "...")

  (delete!
    [store id]
    "..."))


(defrecord MemoryNodeStore
  [memory]

  NodeStore

  (get-links
    [this id]
    (some->
      (get-in @memory [id :links])
      (vary-meta assoc :merkledag.node/id id)))

  (get-data
    [this id]
    (when-let [node (get @memory id)]
      (vary-meta (:data node) assoc
                 :merkledag.node/id id
                 :merkledag.node/links (:links node))))

  (put!
    [this data]
    (put! this nil data))

  (put!
    [this links data]
    (let [id (digest/sha2-256 (pr-str [links data]))
          links (->> (link/find-links data)
                     (concat links)
                     (link/compact-links)
                     (remove (set links))
                     (concat links)
                     (vec))]
      (swap! memory assoc id {:links links, :data data})
      (get-data this id)))

  (delete!
    [this id]
    (let [existed? (contains? @memory id)]
      (swap! memory dissoc id)
      existed?)))


(defn memory-node-store
  []
  (->MemoryNodeStore (atom (sorted-map))))
