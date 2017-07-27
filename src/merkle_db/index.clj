(ns merkle-db.index
  "The data tree in a table is a B+ tree variant which orders the partitions
  into a sorted branching structure.

  The branching factor determines the maximum number of children an index node
  in the data tree can have. Internal (non-root) index nodes with branching
  factor `b` will have between `ceil(b/2)` and `b` children.

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
      [key :as key]
      [partition :as part]
      [record :as record])))


;; ## Specs

(def ^:const data-type
  "Value of `:data/type` that indicates an index tree node."
  :merkle-db/index)

(def default-branching-factor
  "The default number of children to limit each index node to."
  256)

;; The branching factor determines the number of children an index node in the
;; data tree can have.
(s/def ::branching-factor (s/and pos-int? #(<= 4 %)))

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
    (s/keys :req [::height
                  ::keys
                  ::children
                  ::record/count])
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

;; Updating an index tree starts with the root node and a batch of changes to
;; apply to it. The _leaves_ of the tree are partitions, and all other nodes
;; are index nodes. In a tree with branching factor `b`, every index node
;; except the root must have between `ceiling(b/2)` and `b` children. The root
;; is allowed to have between 2 and `b` children. If the tree has more than
;; `:merkle-db.partition/limit` records, then each partition in the tree will
;; be at least half full.
;;
;; The root node may be:
;; - nil, indicating an empty tree.
;; - A partition, indicating that the tree has only a single node and fewer
;;   than `::partition/limit` records.
;; - An index, which is treated as the root node of the tree.
;;
;; At each node, the update proceeds in a number of phases. The algorithm
;; described below uses three terms for nodes:
;; - The "parent" is the current top level node we're recursing on.
;; - The "children" are the direct descendants of the parent node.
;; - The "candidates" are changed but unstored descendant nodes.
;;
;; The batch-update algorithm has the following goals:
;; - **Log scaling:** visit `O(log_b(n))` nodes for `n` nodes updated.
;; - **Minimize garbage:** avoid storing nodes which are not part of the final
;;   tree.
;; - **Deduplication:** reuse existing stored nodes where possible.
;;
;; In the following diagrams, these symbols are used:
;; - `O` - unchanged index node
;; - `*` - candidate index node
;; - `+` - persisted index node
;; - `#` - unchanged partition leaf
;; - `@` - persisted partition leaf
;; - `U` - underflowing partition leaf
;; - `x` - node which has had all children deleted

;; ### Divide Changes
;;
;; In the first **downward** phase, changes at the parent are grouped by the
;; child which links to the subtree containing the referenced keys.
;;
;;     >        .[O].         [????|?????]
;;             /     \
;;            /       \
;;           O        .O.
;;          / \      / | \
;;         O   O    O  O  O
;;        /|\  |\  / \/|\/ \
;;       # # # # # # ##### #
;;
;;              ..O..              |?????]
;;             /     \
;;            /       \
;;     >    [O]       .O.     [??|???]
;;          / \      / | \
;;         O   O    O  O  O
;;        /|\  |\  / \/|\/ \
;;       # # # # # # ##### #
;;
;;              ..O..              |?????]
;;             /     \
;;            /       \
;;           O        .O.        |???]
;;          / \      / | \
;;     >  [O]  O    O  O  O   [?|?]
;;        /|\  |\  / \/|\/ \
;;       # # # # # # ##### #
;;
;;              ..O..              |?????]
;;             /     \
;;            /       \
;;           O        .O.        |???]
;;          / \      / | \
;;         O   O    O  O  O     |?]
;;        /|\  |\  / \/|\/ \
;;     >[#]# # # # # ##### #  [?]

,,,


;; ### Apply Changes
;;
;; In the **apply** phase, changes are applied to the _children_ to produce
;; new child nodes. This call returns an unpersisted candidate, or nil if the
;; resulting node would be empty. The resulting candidate node may have any
;; number of children, meaning it can either underflow, be valid, or overflow,
;; depending on the changes applied.
;;
;;              ..O..
;;             /     \
;;            /       \
;;           O        .O.
;;          / \      / | \
;;         O   O    O  O  O
;;        /|\  |\  /|\/|\/ \
;;     > x x[U]# # ####### #
;;
;; When a node has only a single child, it is 'pulled' up the tree recursively
;; so it can be passed down the next branch for merging into the next branch.
;;
;;              ..O..
;;             /     \
;;            /       \
;;           O        .O.
;;          / \      / | \
;;     >  [U]  O    O  O  O
;;             |\  /|\/|\/ \
;;             # # ####### #
;;
;;              ..O..
;;             /     \
;;            /       \
;;     >    [O](U)    .O.
;;           |       / | \
;;           O      O  O  O
;;          / \    /|\/|\/ \
;;         #   #   ####### #
;;
;; When a subtree is being passed to the next branch, and the current node's
;; height is one more than the subtree root, insert it as a child of the
;; current node:
;;
;;              ..O..
;;             /     \
;;            /       \
;;           O        .O.
;;           |       / | \
;;     >    [*]     O  O  O
;;          /|\    /|\/|\/ \
;;        (U)# #   ####### #
;;
;;              ..O..
;;             /     \
;;            /       \
;;           O        .O.
;;           |       / | \
;;           *      O  O  O
;;          /|\    /|\/|\/ \
;;     >   U x[U]  ####### #
;;
;; ### Redistribute Children
;;
;; In the **distribution** phase, any candidate children which have over or
;; overflowed must split, merge with a neighbor, or borrow some elements from
;; one. Afterwards, all children should be at least half full and under the
;; size limit, unless there is only a single child left.
;;
;; Consider consecutive runs of invalid candidate nodes; if the total number of
;; children is at least half the limit, repartition the children into a number
;; of valid nodes. Otherwise, resolve the last link before the run and add it
;; to the pool to redistribute. Use the link after if the run includes the
;; first child.
;;
;; Two underflowing partitions need to be merged, but still form an
;; underflowing partition:
;;
;;              ..O..
;;             /     \
;;            /       \
;;           O        .O.
;;           |       / | \
;;           *      O  O  O
;;           |     /|\/|\/ \
;;     >  [->U<-]  ####### #
;;
;;              ..O..
;;             /     \
;;            /       \
;;           O        .O.
;;           |       / | \
;;     >    [U]     O  O  O
;;                 /|\/|\/ \
;;                 ####### #
;;
;;              ..O..
;;             /     \
;;            /       \
;;     >    [U]       .O.
;;                   / | \
;;                  O  O  O
;;                 /|\/|\/ \
;;                 ####### #
;;
;;     >       [O](U)
;;              |
;;            ..O..
;;           /  |  \
;;          O   O   O
;;         /|\ /|\ / \
;;         ### ### # #
;;
;;              O
;;              |
;;     >      .[O]. (U)
;;           /  |  \
;;          O   O   O
;;         /|\ /|\ / \
;;         ### ### # #
;;
;;              O
;;              |
;;            ..O..
;;           /  |  \
;;     >   [O]  O   O
;;        // \\/|\ / \
;;        U# U@### # #
;;
;; Again, redistribute the records in the underflowing partitions.
;;
;;              O
;;              |
;;            ..O..
;;           /  |  \
;;          O   O   O
;;         /|\ /|\ / \
;;     >  [@@@]### # #
;;
;;              O
;;              |
;;            ..O..
;;           /  |  \
;;     >   [*]  O   O
;;         /|\ /|\ / \
;;         @@@ ### # #
;;
;;              O
;;              |
;;     >      .[*].
;;           /  |  \
;;          *   O   O
;;         /|\ /|\ / \
;;         @@@ ### # #
;;
;; Fast forward, deleting a subtree...
;;
;;              O
;;              |
;;            ..*..
;;           /  |  \
;;     >    *  [x]  O
;;         /|\ /|\ / \
;;     >   @@@[xxx]# #
;;
;; Insert nodes into right subtree, resulting in splitting multiple partitions.
;;
;;              O
;;              |
;;            ..*..
;;           /     \
;;          *       *
;;         /|\    //|\\
;;     >  @ @ @  [#@@@@]
;;
;; Back to distribution phase, fix overflow by splitting the node:
;;              O
;;              |
;;            ..*..
;;           /  |  \
;;     >    *  [*   *]
;;         /|\ / \ /|\
;;         @@@ # @ @@@
;;
;;              *
;;              |
;;     >      .[*].
;;           /  |  \
;;          *   *   *
;;         /|\ / \ /|\
;;         @@@ # @ @@@
;;
;;     >       [*]
;;              |
;;            ..*..
;;           /  |  \
;;          *   *   *
;;         /|\ / \ /|\
;;         @@@ # @ @@@
;;
;; Now that we're back at the root, we can serialize out the resulting tree.
;;
;;              *
;;              |
;;     >      .[+].
;;           /  |  \
;;          +   +   +
;;         /|\ / \ /|\
;;         @@@ # @ @@@
;;
;; If the upward recursion reaches a branch with a single child, we're on a path
;; up to the root, so return the subtree directly, decreasing the height of the
;; tree.
;;
;;     >       [*]
;;              |
;;            ..+..
;;           /  |  \
;;          +   +   +
;;         /|\ / \ /|\
;;         @@@ # @ @@@
;;
;; Resulting balanced tree:
;;
;;            ..+..
;;           /  |  \
;;          +   +   +
;;         /|\ / \ /|\
;;         @@@ # @ @@@


;; ### References
;;
;; https://pdfs.semanticscholar.org/85eb/4cf8dfd51708418881e2b5356d6778645a1a.pdf
;; Insight: instead of flushing all the updates, select a related subgroup that
;; minimizes repeated changes to the same node path.


(defn build-index
  "Given a sequence of partitions, builds an index tree with the given
  parameters incorporating the partitions. Returns ???"
  ; TODO: document return type
  [store parameters partitions]
  (cond
    (empty? partitions) nil
    (= 1 (count partitions)) (first partitions)
    :else
    ; TODO: implement
    (throw (UnsupportedOperationException. "NYI: build-index over partition sequence"))))


(defn- update-empty
  "Apply changes to an empty tree, returning an updated root node data."
  [store parameters changes]
  ; Divide up added records into a sequence of partitions and build an index
  ; over them.
  (->> (part/from-records store parameters)
       (build-index store parameters)))


(defn- update-partition
  "Apply changes to a partition root, returning an updated root node data."
  [store parameters part changes]
  (when-let [parts (seq (part/apply-patch! store (merge part parameters) changes))]
    (build-index store parameters parts)))


(defn- update-index
  "Apply changes to an index subtree, returning a ..."
  ; TODO: document return type
  [store parameters index changes]
  ; TODO: implement
  (throw (UnsupportedOperationException. "NYI: update data index tree")))


(defn update-tree
  "Apply a set of changes to the index tree rooted in the given node. The
  changes should be a sequence of record ids to either data maps or patch
  tombstones. Parameters may include `:merkle-db.partition/limit` and
  `:merkle-db.data/families`. Returns an updated persisted root node if any
  records remain in the tree."
  [store parameters root changes]
  (if (nil? root)
    ; Empty tree.
    (update-empty store parameters changes)
    ; Check root node type.
    (condp = (get-in root [::node/data :data/type])
      part/data-type
        (update-partition store parameters root changes)
      data-type
        (update-index store parameters root changes)
      (throw (ex-info (str "Unsupported index-tree node type: "
                           (pr-str (:data/type (::node/data root))))
                      {:data/type (:data/type (::node/data root))})))))



;; ## Deletion Functions

; TODO: remove-range?
; TODO: drop-partition?
