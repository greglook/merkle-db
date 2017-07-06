(ns merkle-db.viz
  "Utilities for visualizing merkle-db databases."
  (:require
    [merkledag.link :as link]
    [merkledag.node :as node]
    [multihash.core :as multihash]
    [rhizome.viz :as rhizome]))


;; Thoughts:
;; - Input is database name/version coordinates
;; - Color or shape nodes differently based on type
;; - Draw dashed lines between related versions
;; - Option to label links with names
;; - Option to show just one table between two versions
;; - Option to label nodes with their size


(defn- node-label
  [node]
  (let [short-hash (some-> node ::node/id multihash/base58 (subs 0 8))]
    (if-let [data (::node/data node)]
      (str (vec (remove nil? [short-hash (:data/type data) (:data/title data)])))
      short-hash)))


(defn- node->descriptor
  [node]
  {:label (node-label node)})


(defn- link-label
  [from to]
  (some #(when (= (::node/id to) (::link/target %))
           (::link/name %))
        (::node/links from)))


(defn- edge->descriptor
  [from to]
  ; color links by something?
  {:label (link-label from to)})


(defn visualize-nodes
  "Constructs a graph visualizing the given collection of nodes. Additional
  options are passed on to `rhizome/save-graph`."
  [nodes filename & opts]
  (let [node-map (into {}
                       (comp (filter ::node/id)
                             (map (juxt ::node/id identity)))
                       nodes)]
    (apply rhizome/save-graph
      (vals node-map)
      (fn adjacent [node]
        (keep (comp node-map ::link/target) (::node/links node)))
      :filename filename
      :node->descriptor node->descriptor
      :edge->descriptor edge->descriptor
      opts)))
