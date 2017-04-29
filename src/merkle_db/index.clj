(ns merkle-db.index
  "The index tree in a table is a B+ tree which orders the partitions into a
  sorted tree structure.

  An empty data tree is represented by a nil link from the table. A data tree
  with fewer records than the partition limit is represented directly by a
  single partition node."
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



;; ## Information Functions

; TODO: list partitions
; TODO: group partitions into roughly equal sizes



;; ## Read Functions

(defn read-all
  "Read a lazy sequence of key/map tuples which contain the requested field data
  for every record in the subtree. This function works on both index nodes and
  partitions."
  [store node fields]
  (case (:data/type node)
    :merkle-db/index-node
    (mapcat
      (fn read-child
        [child-link]
        (if-let [child (node/get-data store child-link)]
          (read-all store child fields)
          (throw (ex-info (format "Missing child linked from index-node %s: %s"
                                  (:id node) child-link)
                          {:node (:id node)
                           :child child-link}))))
      (::children node))

    :merkle-db/partition
    (part/read-all store node fields)

    (throw (ex-info (str "Unsupported data-tree node type: "
                         (pr-str (:data/type node)))
                    {:data/type (:data/type node)}))))


(defn- assign-keys
  "Assigns record keys to the children of this node which they would belong
  to. Returns a lazy sequence of vectors containing the child index and a
  sequence of record keys."
  [split-keys record-keys]
  (->>
    [nil 0 split-keys record-keys]
    (iterate
      (fn [[_ index splits rks]]
        (when (seq rks)
          (if (seq splits)
            ; Take all records coming before the next split key.
            (let [[in after] (split-with (partial key/before? (first splits)) rks)]
              [[index in] (inc index) (next splits) after])
            ; No more splits, emit one final group with any remaining keys.
            [[(inc index) rks]]))))
    (drop 1)
    (take-while first)
    (map first)))


(defn read-batch
  "Read a lazy sequence of key/map tuples which contain the requested field data
  for the records whose keys are in the given collection. This function works on
  both index nodes and partitions."
  [store node fields record-keys]
  (case (:data/type node)
    :merkle-db/index-node
    (mapcat
      (fn read-child
        [[index child-keys]]
        (let [child-link (nth (::children node) index)]
          (if-let [child (node/get-data store child-link)]
            (read-batch store child fields child-keys)
            (throw (ex-info (format "Missing child linked from index-node %s: %s"
                                    (:id node) child-link)
                            {:node (:id node)
                             :child child-link})))))
      (assign-keys (::keys node) record-keys))

    :merkle-db/partition
    (part/read-batch store node fields record-keys)

    (throw (ex-info (str "Unsupported data-tree node type: "
                         (pr-str (:data/type node)))
                    {:data/type (:data/type node)}))))


; TODO: read-range
; narrow children by which could contain the range, recurse in order


; TODO: read-slice
; narrow children by which could contain the range, recurse in order



;; ## Update Functions

; TODO: update-records!



;; ## Deletion Functions

; TODO: remove-batch


; TODO: remove-range


; TODO: remove-slice
