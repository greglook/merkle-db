(ns merkle-db.tools.graph
  "Common utilities for working with MerkleDAG graphs."
  (:require
    [merkledag.core :as mdag]
    [merkledag.link :as link]
    [merkledag.node :as node]))


(defn- enqueue-links
  "Helper function for a breadth-first search algorithm which adds all of the
  node's links to the queue in vectors with the node data type."
  [queue node]
  (let [source-type (get-in node [::node/data :data/type])]
    (into queue
          (map (partial vector source-type))
          (::node/links node))))


(defn find-nodes
  "Explore the DAG by following links from the given root node. The `follow?`
  function will be called with the source node `:data/type` and a link, and
  should determine whether to follow the link or not. Returns a map of
  multihash ids to loaded node maps."
  [store visited root follow?]
  (loop [visited (assoc visited (::node/id root) root)
         pending (enqueue-links (clojure.lang.PersistentQueue/EMPTY) root)]
    (if-let [[source-type link] (peek pending)]
      (if (visited (::link/target link))
        ; Already visited the target.
        (recur visited (pop pending))
        ; Determine whether to visit the link and recurse.
        (if-let [child (and (follow? source-type link)
                            (mdag/get-node store link))]
          (recur (assoc visited (::node/id child) child)
                 (enqueue-links (pop pending) child))
          ; Don't visit the link, add a placeholder instead.
          (recur (assoc visited
                        (::link/target link)
                        {::node/id (::link/target link)
                         ::node/size (::link/rsize link)
                         ::node/data {:data/type ::placeholder}})
                 (pop pending))))
      ; No more links to visit, we're done.
      visited)))


(defn find-nodes-2
  "Generate a sequence of values by exploring the DAG structure. Starting with
  the targeted root node, the function `f` will be called with each node, and
  should return a tuple containing two values: the results to emit in the
  sequence and a collection of links to follow for more loads.

  Any node ids in the visited set will be ignored."
  [store visited roots f]
  (when (seq roots)
    (if-let [target (link/identify (peek roots))]
      (if (contains? visited target)
        ; Already visited the target.
        (recur store visited (pop roots) f)
        ; Determine whether to visit the link and recurse.
        (lazy-seq
          (let [node (mdag/get-node store target)
                [results links] (f node)]
            (concat results
                    (find-nodes-2
                      store
                      (conj (set visited) target)
                      (into roots links)
                      f)))))
      ; Not a targetable link somehow?
      (recur store visited (pop roots) f))))
