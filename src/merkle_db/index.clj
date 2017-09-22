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
      [node :as node])
    (merkle-db
      [graph :as graph]
      [key :as key]
      [partition :as part]
      [record :as record])))


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



;; ## Construction

(defn- store-index!
  "Recursively store index nodes from the given sequence of children. Loads
  each child to calculate the aggregate first-key, last-key, and count
  attributes. Returns persisted index node data."
  [store height children]
  (loop [record-count 0
         new-children []
         child-keys []
         last-last-key nil
         children children]
    (if (seq children)
      ; Load and update aggregate stats from next child.
      (let [child (first children)
            link-name (format "%03d" (count new-children))
            data (if (mdag/link? child)
                   (graph/get-link! store child)
                   child)
            target (cond
                     (mdag/link? child) child
                     (= 1 height) (meta child)
                     :else (meta (store-index! store (dec height) (::children data))))
            link (mdag/link link-name target)]
        (recur (int (+ record-count (::record/count data)))
               (conj new-children link)
               (conj child-keys (::record/first-key data))
               (::record/last-key data)
               (next children)))
      ; No more children, serialize node.
      (->>
        {:data/type data-type
         ::height height
         ::keys (vec (rest child-keys))
         ::children new-children
         ::record/count record-count
         ::record/first-key (first child-keys)
         ::record/last-key last-last-key}
        (mdag/store-node! store nil)
        (::node/data)))))


(defn from-partitions
  "Build an index tree from a sequence of partitions, using the given
  parameters. Returns the final, persisted root node data."
  [store params partitions]
  (loop [layer partitions
         height 1]
    (if (<= (count layer) 1)
      (first layer)
      (let [index-groups (part/partition-limited
                           (::branching-factor params default-branching-factor)
                           layer)]
        (recur (mapv (partial store-index! store height) index-groups)
               (inc height))))))



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
      ; No more record keys to assign.
      assignments)))


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
  (condp = (:data/type node)
    data-type
      (mapcat
        (fn read-child
          [child-link]
          (let [child (graph/get-link! store node child-link)]
            (read-all store child fields)))
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
          [[child-link record-keys]]
          (when (seq record-keys)
            (let [child (graph/get-link! store node child-link)]
              (read-batch store child fields (map first record-keys)))))
        (assign-records node (map vector record-keys)))

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

    part/data-type
      (part/read-range store node fields start-key end-key)

    (throw (ex-info (str "Unsupported index-tree node type: "
                         (pr-str (:data/type node)))
                    {:data/type (:data/type node)}))))



;; ## Update Functions

(defn- numbered-child-links
  "Write a vector of childen into numbered links."
  [children]
  (into []
        (map-indexed #(mdag/link (format "%03d" %1) %2))
        children))


(defn- index-children
  "Aggregates index node values from a sequence of child data."
  [children]
  (let [ckeys (into [] (map ::record/first-key) (rest children))
        links (numbered-child-links children)
        record-count (reduce + 0 (map ::record/count children))]
    {::keys ckeys
     ::children links
     ::record/count record-count
     ::record/first-key (::record/first-key (first children))
     ::record/last-key (::record/last-key (last children))}))


(defn- update-index-children!
  "Apply changes to a sequence of intermediate index nodes and redistribute the
  results to create a new sequence of valid indexes. Each input tuple should
  have the form `[index changes]`, and `carry` may be a forward-carried index
  subtree or partition that is **not** a direct descendant of the parent node.

  Returns a sequence of updated and valid (but unpersisted) index nodes, or a
  direct map of node data to carry forward."
  [store params carry inputs]
  ,,,)


(defn- update-index-node
  [store params carry [index changes]]
  (if (seq changes)
    (let [child-changes (map (juxt #(graph/get-link! store index (first %)) second)
                             (assign-records index changes))
          children (if (= 1 (::height index))
                     ; Update children as partitions.
                     (part/update-partitions! store params carry child-changes)
                     ; Update child index nodes.
                     (if (and (= data-type (:data/type carry))
                              (= (dec (::height index)) (::height carry)))
                       ; Adopt the carried index subtree by prepending it to the results.
                       (update-index-children! store params nil (cons [carry nil] child-changes))
                       ; No carry, or carry is not a direct child.
                       (update-index-children! store params carry child-changes)))]
      (if (sequential? children)
        (if (= 1 (count children))
          ; Orphaned child, return directly for carrying.
          (first children)
          ; Construct index node from partitions.
          ; FIXME: needs to break into multiple nodes on overflow
          [(merge index (index-children children))])
        ; Carried node, return for further carrying.
        children))
    ; No changes, return unchanged node.
    [index]))


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
           (from-partitions store params))

    ; Root is a partition node.
    (= part/data-type (:data/type root))
      (some->>
        (part/update-root! store params root changes)
        (from-partitions store params))

    ; Root is an index node.
    (= data-type (:data/type root))
      (letfn [(store!  ; TODO: very similar to `store-index!`
                [node]
                (if (mdag/link? node)
                  node
                  (->> (::children node)
                       (map store!)
                       (numbered-child-links)
                       (assoc node ::children)
                       (mdag/store-node! store nil)
                       (::node/data))))]
        (store! (update-index-node store params nil [root changes])))

    :else
      (throw (ex-info (str "Unsupported index-tree node type: "
                           (pr-str (:data/type root)))
                      {:data/type (:data/type root)}))))
