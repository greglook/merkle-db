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
    [clojure.spec.alpha :as s]
    [merkle-db.graph :as graph]
    [merkle-db.key :as key]
    [merkle-db.partition :as part]
    [merkle-db.record :as record]
    [merkledag.core :as mdag]
    [merkledag.node :as node]))


;; The fan-out determines the number of children an index node in the data tree
;; can have.
(s/def ::fan-out (s/and pos-int? #(<= 4 %)))

;; Height of the node in the tree. Partitions are the leaves and have an
;; implicit height of zero, so the first index node has a height of one.
(s/def ::height pos-int?)

;; Sorted vector of index keys.
(s/def ::keys (s/coll-of key/key? :kind vector?))

;; Sorted vector of child links.
(s/def ::children (s/coll-of mdag/link? :kind vector?))

;; Data tree index node.
(s/def ::node-data
  (s/and
    (s/keys :req [::height
                  ::keys
                  ::children
                  ::record/count
                  ::record/first-key
                  ::record/last-key])
    #(= :merkle-db/index (:data/type %))
    #(= (count (::children %))
        (inc (count (::keys %))))))



;; ## Branching Factor

(def default-fan-out
  "The default number of children to limit each index node to."
  256)


(defn max-branches
  "Return the maximum number of children a valid index node can have given the
  parameters."
  [params]
  (::fan-out params default-fan-out))


(defn min-branches
  "Return the minimum number of children a valid index node must have given the
  parameters."
  [params]
  (int (Math/ceil (/ (max-branches params) 2))))



;; ## Construction

(defn- numbered-child-links
  "Write a vector of childen into numbered links."
  [children]
  (let [width (count (pr-str (dec (count children))))
        fmt (str "%0" width "d")]
    (into []
          (map-indexed #(mdag/link (format fmt %1) %2))
          children)))


(defn- store-index!
  "Aggregates index node values from a sequence of child data. Returns the
  persisted node data map for the constructed index."
  [store height children]
  ; OPTIMIZE: truncate separator keys to the shortest value that correctly splits the adjacent children
  (let [ckeys (into [] (map ::record/first-key) (rest children))
        links (numbered-child-links children)
        record-count (reduce + 0 (map ::record/count children))]
    (->>
      {:data/type :merkle-db/index
       ::height height
       ::keys ckeys
       ::children links
       ::record/count record-count
       ::record/first-key (::record/first-key (first children))
       ::record/last-key (::record/last-key (last children))}
      (mdag/store-node! store nil)
      (::node/data))))


(defn- split-limited
  "Returns a sequence of groups of the elements of `coll` such that:
  - No group has more than `limit` elements
  - The number of groups is minimized
  - Groups are approximately equal in size

  This method eagerly realizes the input collection."
  [limit coll]
  (let [cnt (count coll)
        n (min (int (Math/ceil (/ cnt limit))) cnt)]
    (when (pos? cnt)
      (loop [i 0
             mark 0
             groups []
             xs coll]
        (if (seq xs)
          (let [mark' (int (* (/ (inc i) n) cnt))
                [head tail] (split-at (- mark' mark) xs)]
            (recur (inc i) (int mark') (conj groups head) tail))
          groups)))))


(defn- build-tree*
  "Build an index tree from a sequence of child nodes, using the given
  parameters. Returns a result vector with the height and final sequence of
  index nodes which can no longer make a valid higher root node. The height of
  the result will be at most `ceiling`, if a positive value is given."
  [store params ceiling children]
  (when (seq children)
    (let [heights (distinct (map #(::height % 0) children))]
      (when (< 1 (count heights))
        (throw (IllegalArgumentException.
                 (str "Cannot build tree from nodes at differing heights: "
                      (pr-str heights)))))
      (loop [height (first heights)
             layer children]
        (if (and (<= (min-branches params) (count layer))
                 (or (nil? ceiling) (< height ceiling)))
          ; Build the next layer.
          (let [groups (split-limited (max-branches params) layer)]
            (recur (inc height)
                   (mapv (partial store-index! store (inc height)) groups)))
          ; Hit ceiling, or insufficient children.
          [height layer])))))


(defn build-tree
  "Build an index tree from a sequence of child nodes, using the given
  parameters. Returns the final, persisted root node data."
  [store params children]
  (when-let [[height nodes] (build-tree* store params nil children)]
    (if (= 1 (count nodes))
      (first nodes)
      (store-index! store (inc height) nodes))))



;; ## Read Functions

(defn- assign-records
  "Assigns records to the children of this node which they would belong
  to. Returns a sequence of vectors containing each child link and a collection
  of the record tuples, if any."
  [index records]
  (loop [assignments []
         children (::children index)
         split-keys (::keys index)
         pending (sort-by first records)]
    (if (seq pending)
      (if (seq split-keys)
        ; Take next batch of keys.
        (let [split (first split-keys)
              [in after] (split-with #(key/before? (first %) split) pending)]
          (recur (conj assignments [(first children) (not-empty in)])
                 (next children)
                 (next split-keys)
                 after))
        ; No more splits, emit one final group with remaining keys.
        (conj assignments [(first children) pending]))
      ; No more record keys to assign; add remaining children.
      (into assignments (map #(vector % nil)) children))))


(defn- child-boundaries
  "Return a sequence of tuples of a child link, its leading boundary, and its
  trailing boundary, corresponding to the split keys on either side of that
  child."
  [index]
  (loop [boundaries [[(first (::children index)) nil (first (::keys index))]]
         children (next (::children index))
         split-keys (::keys index)]
    (if (seq children)
      (recur (conj boundaries [(first children) (first split-keys) (second split-keys)])
             (next children)
             (next split-keys))
      boundaries)))


(defn- children-in-range
  "Generate a sequence of links to the children of `node` which may have data
  in the range `min-k` to `max-k`. A nil boundary is treated as an open range."
  [node min-k max-k]
  (cond->> (child-boundaries node)
    min-k
      (drop-while #(if-let [last-key (nth % 2)]
                     (key/before? last-key min-k)
                     false))
    max-k
      (take-while #(if-let [first-key (nth % 1)]
                     (not (key/after? first-key max-k))
                     true))
    true
      (mapv first)))


(defn find-partitions
  "Find all of the partitions in the index subtree by recursively traversing
  the tree nodes, calling `f` on each index node and context to return a list
  of child links to traverse, plus some child context. Returns a lazy sequence
  of tuples with the partitions in order, plus the final child context for each
  partition."
  [store f node ctx]
  ; TODO: figure out how to make this reducible
  (when node
    (case (:data/type node)
      :merkle-db/index
        (mapcat
          (fn read-child
            [[child-link child-ctx]]
            (let [child (graph/get-link! store node child-link)]
              (find-partitions store f child child-ctx)))
          (f node ctx))

      :merkle-db/partition
        [[node ctx]]

      (throw (ex-info (str "Unsupported index-tree node type: "
                           (pr-str (:data/type node)))
                      {:data/type (:data/type node)
                       :cxt ctx})))))


(defn read-all
  "Read a lazy sequence of key/map tuples which contain the requested field data
  for every record in the subtree. This function works on both index nodes and
  partitions."
  [store node fields]
  (when node
    (mapcat
      (fn read-part
        [[part _]]
        (part/read-all store part fields))
      (find-partitions
        store
        (fn search-children
          [index _]
          (map vector (::children index)))
        node nil))))


(defn read-batch
  "Read a lazy sequence of key/map tuples which contain the requested field data
  for the records whose keys are in the given collection. This function works on
  both index nodes and partitions."
  [store node fields record-keys]
  (when node
    (mapcat
      (fn read-part
        [[part record-keys]]
        (part/read-batch store part fields record-keys))
      (find-partitions
        store
        (fn search-children
          [index record-keys]
          (->> (map vector record-keys)
               (assign-records index)
               (map (fn unwrap [[cl rkv]] [cl (mapv first rkv)]))))
        node record-keys))))


(defn read-range
  "Read a lazy sequence of key/map tuples which contain the field data for the
  records whose keys lie in the given range, inclusive. A nil boundary includes
  all records in that range direction."
  [store node fields min-k max-k]
  (when node
    (mapcat
      (fn read-part
        [[part _]]
        (part/read-range store part fields min-k max-k))
      (find-partitions
        store
        (fn search-children
          [index _]
          (map vector (children-in-range index min-k max-k)))
        node nil))))



;; ## Update Functions

(defn- carry-back
  [store params height nodes carry]
  {:pre [(number? height) (vector? nodes)]}
  (cond
    ; No remaining nodes - return carry value.
    (empty? nodes)
      carry

    ; Carry matches node height, concat result.
    (= height (first carry))
      [height (into nodes (second carry))]

    ; Otherwise recursively fold last element.
    :else
      (let [nodes' (pop nodes)
            children (mapv #(graph/get-link! store (peek nodes) %)
                           (::children (peek nodes)))
            result (if (= 1 height)
                     (part/carry-back store params children carry)
                     (carry-back store params (dec height) children carry))
            carry' (build-tree* store params height (second result))]
        (recur store params height nodes' carry'))))


(declare update-index-node!)


(defn- update-index-children!
  "Apply changes to a sequence of intermediate index nodes and redistribute the
  results to create a new sequence of valid indexes. Each input tuple should
  have the form `[index changes]`, and `carry` may be a forward-carried result
  vector.

  Returns a tuple containing the resulting height and a sequence of updated and
  valid index nodes at that height."
  [store params height carry child-inputs]
  (loop [outputs []
         carry carry
         inputs child-inputs]
    (if (seq inputs)
      ; Process next input node
      (let [[child changes] (first inputs)
            [rheight elements :as result] (update-index-node!
                                            store params carry child changes)]
        (cond
          ; Result is empty.
          (nil? result)
            (recur outputs nil (next inputs))
          ; Result elements are output-level nodes.
          (= rheight height)
            (recur (into outputs elements) nil (next inputs))
          ; Result elements are a carry.
          :else
            (recur outputs result (next inputs))))
      ; No more input nodes.
      (if (seq outputs)
        (if carry
          (carry-back store params height outputs carry)
          [height outputs])
        ; No outputs, so return nil or direct carry.
        carry))))


(defn- update-index-node!
  "Updates the given index node by applying the changes. Returns a tuple
  containing the resulting height and a sequence of updated and valid (but
  unpersisted) index nodes at that height."
  [store params carry index changes]
  (if (and (nil? carry) (empty? changes))
    ; No carry or changes to apply, pass-through node.
    [(::height index) [index]]

    ; Divide up changes and apply to children.
    (let [child-inputs (map (fn [[child child-changes]]
                              [(graph/get-link! store index child)
                               child-changes])
                            (assign-records index changes))
          child-height (dec (::height index))]
      (->
        (cond
          ; Update children as partitions.
          (zero? child-height)
            (part/update-partitions! store params carry child-inputs)
          ; Adopt carried subnodes.
          (and carry (= (first carry) child-height))
            (update-index-children!
              store params child-height
              nil (concat (map vector (second carry)) child-inputs))
          ; Carry forward into child updates.
          :else
            (update-index-children!
              store params child-height
              carry child-inputs))
        (as-> result
          (when result
            (if (and (= child-height (first result))
                     (= (map first child-inputs) (second result)))
              ; Children remained unchanged after updates, so return original
              ; index node.
              [(::height index) [index]]
              (if (neg? (first result))
                ; Negative height means directly-carried records
                result
                ; Build until the layer is too small or we've reached the original
                ; index node height.
                (build-tree* store params (::height index) (second result))))))))))


(defn update-tree
  "Apply a set of changes to the index tree rooted in the given node. The
  changes should be a sequence of record ids to either data maps or patch
  tombstones. Parameters may include `:merkle-db.partition/limit` and
  `:merkle-db.data/families`. Returns an updated persisted root node if any
  records remain in the tree."
  [store params root changes]
  (cond
    ; No changes, return root as-is.
    (empty? changes)
      root

    ; Empty tree.
    (nil? root)
      (->> changes
           (part/partition-records store params)
           (build-tree store params))

    ; Root is an index or partition node.
    (contains? #{:merkle-db/index :merkle-db/partition} (:data/type root))
      (let [[height nodes] (if (= :merkle-db/partition (:data/type root))
                             (part/update-partitions! store params nil [[root changes]])
                             (update-index-node! store params nil root changes))]
        (if (and height (neg? height))
          (part/from-records store params nodes)
          (build-tree store params nodes)))

    :else
      (throw (ex-info (str "Unsupported index-tree node type: "
                           (pr-str (:data/type root)))
                      {:data/type (:data/type root)}))))
