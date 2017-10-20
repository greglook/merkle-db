(ns merkle-db.viz
  "Utilities for visualizing merkle-db databases."
  (:require
    [clojure.string :as str]
    [merkledag.core :as mdag]
    [merkledag.link :as link]
    [merkledag.node :as node]
    [merkle-db.db :as db]
    [merkle-db.graph :as graph]
    [multihash.core :as multihash]
    [rhizome.viz :as rhizome]))


;; Thoughts:
;; - Input is database name/version coordinates
;; - Color or shape nodes differently based on type
;; - Draw dashed lines between related versions
;; - Option to label links with names
;; - Option to show just one table between two versions
;; - Option to label nodes with their size


(defn node-label
  [node]
  (str
    (when-let [title (get-in node [::node/data :data/title])]
      (str title \newline))
    (get-in node [::node/data :data/type] "???") \newline
    (some-> node ::node/id multihash/base58 (subs 0 8))
    (when-let [size (::node/size node)]
      (format " (%d B)" size))))


(defn node->descriptor
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
                     (when-let [rc (:merkle-db.record/count (::node/data node))]
                       (format "\n%d records" rc)))}
      :merkle-db/patch
        {:shape :hexagon
         :label (str (node-label node)
                     (when-let [changes (:merkle-db.patch/changes (::node/data node))]
                       (format "\n%d changes" (count changes))))}
      :merkle-db/index
        {:shape :trapezium
         :label (str (node-label node)
                     (when-let [rc (:merkle-db.record/count (::node/data node))]
                       (format "\n%d records" rc))
                     \newline (:merkle-db.record/first-key (::node/data node))
                     \newline (:merkle-db.record/last-key (::node/data node)))}
      :merkle-db/partition
        {:shape :rect
         :label (str (node-label node)
                     (when-let [rc (:merkle-db.record/count (::node/data node))]
                       (format "\n%d records" rc))
                     \newline (:merkle-db.record/first-key (::node/data node))
                     \newline (:merkle-db.record/last-key (::node/data node)))}
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


(defn link-label
  [from to]
  (or
    (::link/name (meta to))
    (some #(when (= (::node/id to) (::link/target %))
             (::link/name %))
          (::node/links from))))


(defn edge->descriptor
  [from to]
  ; color links by something?
  {:label (link-label from to)})


(defn graph-tree
  [rf store root-id follow? & args]
  (let [nodes (graph/find-nodes store {} (mdag/get-node store root-id) follow?)]
    (apply rf
      (vals nodes)
      (fn adjacent
        [node]
        (keep #(when-let [target (get nodes (::link/target %))]
                 (vary-meta target assoc ::link/name (::link/name %)))
              (::node/links node)))
      :node->descriptor node->descriptor
      :edge->descriptor edge->descriptor
      :options {:dpi 64}
      ; :node->cluster
      ; :cluster->parent
      ; :cluster->descriptor
      args)))


(defn view-tree
  [store root-id follow? & args]
  (apply graph-tree rhizome/view-graph store root-id follow? args))


(defn save-tree
  [store root-id follow? filename & args]
  (apply graph-tree rhizome/save-graph store root-id follow? :filename filename args))


(defn view-table
  ([table]
   (view-table table (constantly true)))
  ([^merkle_db.table.Table table follow?]
   (when-not (::node/id table)
     (throw (IllegalArgumentException.
              (str "Cannot vizualize table with no node id: "
                   (pr-str table)))))
   (view-tree (.store table) (::node/id table) follow?)))


(defn view-database
  ([db]
   (view-database db (constantly true)))
  ([^merkle_db.db.Database db follow?]
   (when-not (::node/id db)
     (throw (IllegalArgumentException.
              (str "Cannot vizualize database with no node id: "
                   (pr-str db)))))
   (view-tree (.store db) (::node/id db) follow?)))
