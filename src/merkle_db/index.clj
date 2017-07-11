(ns merkle-db.index
  "The data tree in a table is a B+ tree which orders the partitions into a
  sorted branching structure.

  The branching factor determines the maximum number of children an index node
  in the data tree can have. Internal (non-root) index nodes with branching
  factor `b` will have between `ceiling(b/2)` and `b` children.

  An empty data tree is represented by a nil link from the table root. A data
  tree with fewer records than the partition limit is represented directly by a
  single partition node."
  (:require
    [clojure.future :refer [pos-int?]]
    [clojure.spec :as s]
    (merkledag
      [core :as mdag]
      [link :as link]
      [node :as node])
    (merkle-db
      [data :as data]
      [key :as key]
      [partition :as part]
      ; TODO: is patch needed?
      [patch :as patch])))


;; ## Specs

(def ^:const data-type
  "Value of `:data/type` that indicates an index tree node."
  :merkle-db/index)

(def default-branching-factor
  "The default number of children to limit each index node to."
  256)

;; The branching factor determines the number of children an index node in the
;; data tree can have.
(s/def ::branching-factor (s/and pos-int? #(< 2 %)))

;; Height of the node in the tree. Partitions are the leaves and have an
;; implicit height of zero, so the first index node has a height of one.
(s/def ::height pos-int?)

;; Sorted vector of index keys.
(s/def ::keys (s/coll-of key/key? :kind vector?))

;; Sorted vector of child links.
(s/def ::children (s/coll-of link/merkle-link? :kind vector?))

;; Data tree index node.
(s/def ::node-data
  (s/and
    (s/keys :req [::data/count
                  ::height
                  ::keys
                  ::children])
    #(= data-type (:data/type %))
    #(= (count (::children %))
        (inc (count (::keys %))))))



;; ## Read Functions

(defn- assign-keys
  "Assigns record keys to the children of this node which they would belong
  to. Returns a lazy sequence of vectors containing the child index and a
  sequence of record keys."
  [split-keys record-keys]
  ; OPTIMIZE: use transducers
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


(defn read-all
  "Read a lazy sequence of key/map tuples which contain the requested field data
  for every record in the subtree. This function works on both index nodes and
  partitions."
  [store node fields]
  (condp = (:data/type node)
    data-type
    (mapcat
      (fn read-child
        [child-link]
        (if-let [child (mdag/get-data store child-link)]
          (read-all store child fields)
          (throw (ex-info (format "Missing child linked from index-node %s: %s"
                                  (:id node) child-link)
                          {:node (:id node)
                           :child child-link}))))
      (::children node))

    part/data-type
    (part/read-all store node fields)

    (throw (ex-info (str "Unsupported index-tree node type: "
                         (pr-str (:data/type node)))
                    {:data/type (:data/type node)}))))


(defn read-batch
  "Read a lazy sequence of key/map tuples which contain the requested field data
  for the records whose keys are in the given collection. This function works on
  both index nodes and partitions."
  [store node fields record-keys]
  (condp = (:data/type node)
    data-type
    (mapcat
      (fn read-child
        [[index child-keys]]
        (let [child-link (nth (::children node) index)]
          (if-let [child (mdag/get-data store child-link)]
            (read-batch store child fields child-keys)
            (throw (ex-info (format "Missing child linked from index-node %s: %s"
                                    (:id node) child-link)
                            {:node (:id node)
                             :child child-link})))))
      (assign-keys (::keys node) record-keys))

    part/data-type
    (part/read-batch store node fields record-keys)

    (throw (ex-info (str "Unsupported index-tree node type: "
                         (pr-str (:data/type node)))
                    {:data/type (:data/type node)}))))


(defn read-range
  "Read a lazy sequence of key/map tuples which contain the field data for the
  records whose keys lie in the given range, inclusive. A nil boundary includes
  all records in that range direction."
  [store node fields start-key end-key]
  (condp = (:data/type node)
    data-type
    (->
      (concat (map vector (::children node) (::keys node))
              [[(last (::children node))]])
      (->>
        (partition 2 1)
        (map (fn [[[_ start-key] [link end-key]]]
               [link [start-key end-key]]))
        (cons [(first (::children node)) nil (first (::key node))]))
      (cond->>
        start-key
          (drop-while #(key/before? (nth % 2) start-key))
        end-key
          (take-while #(key/after? end-key (nth % 1))))
      ,,,  ; FIXME
      #_
      (read-tablets store part fields tablet/read-range start-key end-key))

    part/data-type
    (part/read-range store node fields start-key end-key)

    (throw (ex-info (str "Unsupported index-tree node type: "
                         (pr-str (:data/type node)))
                    {:data/type (:data/type node)}))))



;; ## Update Functions

;; Insertion
;;
;; Perform a search to determine what bucket the new record should go into.
;;
;;     If the bucket is not full (at most b − 1 {\displaystyle b-1} b-1 entries after the insertion), add the record.
;;     Otherwise, split the bucket.
;;         Allocate new leaf and move half the bucket's elements to the new bucket.
;;         Insert the new leaf's smallest key and address into the parent.
;;         If the parent is full, split it too.
;;             Add the middle key to the parent node.
;;         Repeat until a parent is found that need not split.
;;     If the root splits, create a new root which has one key and two pointers. (That is, the value that gets pushed to the new root gets removed from the original node)
;;
;; B-trees grow at the root and not at the leaves.[1]


;; Deletion
;;
;;     Start at root, find leaf L where entry belongs.
;;     Remove the entry.
;;         If L is at least half-full, done!
;;         If L has fewer entries than it should,
;;             If sibling (adjacent node with same parent as L) is more than half-full, re-distribute, borrowing an entry from it.
;;             Otherwise, sibling is exactly half-full, so we can merge L and sibling.
;;     If merge occurred, must delete entry (pointing to L or sibling) from parent of L.
;;     Merge could propagate to root, decreasing height.


(defn build-index
  "Given a sequence of partitions, builds an index tree with the given
  parameters incorporating the partitions."
  [parameters partitions]
  ; TODO: implement
  (throw (UnsupportedOperationException. "NYI: build-index over partition sequence")))


(defn update-tree
  "Apply a set of changes to the index tree rooted in the given node. The
  changes should be a sequence of record ids to either data maps or patch
  tombstones. Parameters may include `:merkle-db.partition/limit` and
  `:merkle-db.data/families`. Returns an updated persisted root node if any
  records remain in the tree."
  [store parameters root changes]
  (if (nil? root)
    ; If root is nil, divide up added records into a sequence of partitions and
    ; build an index over them.
    (->> (patch/remove-tombstones changes)
         (part/from-records store parameters)
         (build-index parameters))
    ; Check root node type.
    (condp = (get-in root [::node/data :data/type])
      ; If root is a partition, apply changes to the partition. If the new
      ; partition is empty, return nil. If the partition exceeds the limit, split
      ; into smaller partitions and build an index over them.
      part/data-type
        (throw (UnsupportedOperationException. "NYI: update data partition"))
      ; If root is an index node, divide up changes to match children and
      ; recurse for each child:
      ; - Apply changes by calling `update-tree*` which may return multiple nodes;
      ; - If result has one node, check that it is at least half-full.
      ;   - If so, replace child link and continue.
      ;   - If not, borrow from a sibling.
      ; - If multiple nodes, there is a _split_, so update the current root node.
      ; - If empty or nil, subtree became empty so remove child and key from node.
      data-type
        (throw (UnsupportedOperationException. "NYI: update data index tree"))

      (throw (ex-info (str "Unsupported index-tree node type: "
                           (pr-str (:data/type (::node/data root))))
                      {:data/type (:data/type (::node/data root))})))))



;; ## Deletion Functions

; TODO: remove-batch
; TODO: remove-range
