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
    [merkledag.core :as mdag]
    [merkledag.node :as node]
    [merkle-db.graph :as graph]
    [merkle-db.key :as key]
    [merkle-db.partition :as part]
    [merkle-db.record :as record]))


(def ^:const data-type
  "Value of `:data/type` that indicates an index tree node."
  :merkle-db/index)


;; The branching factor determines the number of children an index node in the
;; data tree can have.
(s/def ::branching-factor (s/and pos-int? #(<= 4 %)))

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
    #(= data-type (:data/type %))
    #(= (count (::children %))
        (inc (count (::keys %))))))



;; ## Branching Factor

(def default-branching-factor
  "The default number of children to limit each index node to."
  256)


(defn max-branches
  "Return the maximum number of children a valid index node can have given the
  parameters."
  [params]
  (::branching-factor params default-branching-factor))


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
  (let [ckeys (into [] (map ::record/first-key) (rest children))
        links (numbered-child-links children)
        record-count (reduce + 0 (map ::record/count children))]
    (->>
      {:data/type data-type
       ::height height
       ::keys ckeys
       ::children links
       ::record/count record-count
       ::record/first-key (::record/first-key (first children))
       ::record/last-key (::record/last-key (last children))}
      (mdag/store-node! store nil)
      (::node/data))))


(defn build-tree
  "Build an index tree from a sequence of child nodes, using the given
  parameters. Returns the final, persisted root node data."
  [store params children]
  (when (seq children)
    (let [heights (distinct (map #(::height % 0) children))]
      (when (< 1 (count heights))
        (throw (IllegalArgumentException.
                 (str "Cannot build tree from nodes at differing heights: "
                      (pr-str heights)))))
      (loop [layer children
             height (inc (first heights))]
        (if (<= (count layer) 1)
          (first layer)
          (let [groups (part/split-limited (max-branches params) layer)]
            (recur (mapv (partial store-index! store height) groups)
                   (inc height))))))))


(defn from-records
  "Build an index tree from a sequence of records. Not performant for large
  numbers of records!"
  [store params records]
  (->>
    (sort-by first records)
    (part/partition-records store params)
    (build-tree store params)))



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


(defn read-all
  "Read a lazy sequence of key/map tuples which contain the requested field data
  for every record in the subtree. This function works on both index nodes and
  partitions."
  [store node fields]
  (cond
    (nil? node)
      nil

    (= data-type (:data/type node))
      (mapcat
        (fn read-child
          [child-link]
          (let [child (graph/get-link! store node child-link)]
            (read-all store child fields)))
        (::children node))

    (= part/data-type (:data/type node))
      (part/read-all store node fields)

    :else
      (throw (ex-info (str "Unsupported index-tree node type: "
                           (pr-str (:data/type node)))
                      {:data/type (:data/type node)}))))


(defn read-batch
  "Read a lazy sequence of key/map tuples which contain the requested field data
  for the records whose keys are in the given collection. This function works on
  both index nodes and partitions."
  [store node fields record-keys]
  (cond
    (nil? node)
      nil

    (= data-type (:data/type node))
      (mapcat
        (fn read-child
          [[child-link record-keys]]
          (when (seq record-keys)
            (let [child (graph/get-link! store node child-link)]
              (read-batch store child fields (map first record-keys)))))
        (assign-records node (map vector record-keys)))

    (= part/data-type (:data/type node))
      (part/read-batch store node fields record-keys)

    :else
      (throw (ex-info (str "Unsupported index-tree node type: "
                           (pr-str (:data/type node)))
                      {:data/type (:data/type node)}))))


(defn read-range
  "Read a lazy sequence of key/map tuples which contain the field data for the
  records whose keys lie in the given range, inclusive. A nil boundary includes
  all records in that range direction."
  [store node fields start-key end-key]
  (cond
    (nil? node)
      nil

    (= data-type (:data/type node))
      (->
        (child-boundaries node)
        (cond->>
          start-key
            (drop-while #(key/before? (nth % 2) start-key))
          end-key
            (take-while #(not (key/after? (nth % 1) end-key))))
        (->>
          (mapcat
            (fn read-child
              [[child-link _ _]]
              (let [child (graph/get-link! store node child-link)]
                (read-range store child fields start-key end-key))))))

    (= part/data-type (:data/type node))
      (part/read-range store node fields start-key end-key)

    :else
      (throw (ex-info (str "Unsupported index-tree node type: "
                           (pr-str (:data/type node)))
                      {:data/type (:data/type node)}))))



;; ## Update Functions

(defn- carry-back
  [store params nodes carry]
  ; FIXME: implement
  (throw (RuntimeException. "NYI: index/carry-back")))


(declare update-index-node!)


(defn- update-index-children!
  "Apply changes to a sequence of intermediate index nodes and redistribute the
  results to create a new sequence of valid indexes. Each input tuple should
  have the form `[index changes]`, and `carry` may be a forward-carried result
  vector.

  Returns a tuple containing the resulting height and a sequence of updated and
  valid (but unpersisted) index nodes at that height."
  [store params height carry inputs]
  (loop [outputs []
         carry carry
         inputs inputs]
    (if (seq inputs)
      ; Process next input node
      (let [[child changes] (first inputs)
            [rheight elements :as result]
              (update-index-node! store params carry child changes)]
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
        (let [[oheight outputs :as result]
                (if carry
                  (carry-back store params outputs carry)
                  [(dec height) outputs])]
          (if (neg? oheight)
            ; Got bare records from carry-back
            result
            ; Try to build intermediate index layers.
            ; TODO: build until (= oheight (dec height)) ?
            (if (<= (min-branches params) (count outputs))
              ; Build one or more valid index nodes from the outputs.
              [height (mapv (partial store-index! store (inc oheight))
                            (part/split-limited (max-branches params)
                                                outputs))]
              ; Not enough outputs to make a valid node - return for carrying.
              result)))
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
            carry child-inputs)))))


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
    (contains? #{data-type part/data-type} (:data/type root))
      (let [[height nodes] (if (= part/data-type (:data/type root))
                             (part/update-partitions! store params nil [[root changes]])
                             (update-index-node! store params nil root changes))]
        (if (and height (neg? height))
          (part/from-records store params nodes)
          (build-tree store params nodes)))

    :else
      (throw (ex-info (str "Unsupported index-tree node type: "
                           (pr-str (:data/type root)))
                      {:data/type (:data/type root)}))))
