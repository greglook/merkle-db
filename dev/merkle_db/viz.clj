(ns merkle-db.viz
  "Utilities for visualizing merkle-db databases."
  (:require
    [clojure.string :as str]
    (merkledag
      [core :as mdag]
      [link :as link]
      [node :as node])
    [multihash.core :as multihash]
    [rhizome.viz :as rhizome]))


;; Thoughts:
;; - Input is database name/version coordinates
;; - Color or shape nodes differently based on type
;; - Draw dashed lines between related versions
;; - Option to label links with names
;; - Option to show just one table between two versions
;; - Option to label nodes with their size


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


(defn- node-label
  [node]
  (str
    (when-let [title (get-in node [::node/data :data/title])]
      (str title \newline))
    ;(get-in node [::node/data :data/type] "???") \newline
    (some-> node ::node/id multihash/base58 (subs 0 8))
    (when-let [size (::node/size node)]
      (format " (%d B)" size))))


(defn- node->descriptor
  [node]
  (let [data-type (get-in node [::node/data :data/type])]
    (case data-type
      :merkle-db/database
        {:shape :house
         :label (node-label node)}
      :merkle-db/table
        ; idea: show short record count
        {:shape :ellipse
         :label (str (node-label node)
                     (when-let [rc (:merkle-db.data/count (::node/data node))]
                       (format "\n%d records" rc)))}
      :merkle-db/patch
        {:shape :hexagon
         :label (str (node-label node)
                     (when-let [changes (:merkle-db.patch/changes (::node/data node))]
                       (format "\n%d changes" (count changes))))}
      :merkle-db/index
        ; idea: show start/end keys
        {:shape :trapezium
         :label (node-label node)}
      :merkle-db/partition
        ; idea: show start/end keys
        ; idea: show short record count
        {:shape :rect
         :label (str (node-label node)
                     (when-let [rc (:merkle-db.data/count (::node/data node))]
                       (format "\n%d records" rc)))}
      :merkle-db/tablet
        {:shape :hexagon
         :label (node-label node)}
      ::placeholder
        {:shape :doublecircle
         :label (node-label node)}
      ; else
        {:shape :doubleoctagon
         :color :red
         :label (node-label node)})))


(defn- link-label
  [from to]
  (or
    (::link/name (meta to))
    (some #(when (= (::node/id to) (::link/target %))
             (::link/name %))
          (::node/links from))))


(defn- edge->descriptor
  [from to]
  ; color links by something?
  {:label (link-label from to)})


(defn view-database
  [db]
  (when-not (::node/id db)
    (throw (IllegalArgumentException.
             (str "Cannot vizualize database with no node id: "
                  (pr-str db)))))
  (let [nodes (find-nodes
                (.store db)
                {}
                (mdag/get-node (.store db) (::node/id db))
                (constantly true))]
    (rhizome/view-graph
      (vals nodes)
      (fn adjacent
        [node]
        (keep #(when-let [target (get nodes (::link/target %))]
                 (vary-meta target assoc ::link/name (::link/name %)))
              (::node/links node)))
      :node->descriptor node->descriptor
      :edge->descriptor edge->descriptor
      ; :options
      ; :node->cluster
      ; :cluster->parent
      ; :cluster->descriptor
      )))
