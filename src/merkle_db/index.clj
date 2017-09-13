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
         ::keys (vec (drop 1 child-keys))
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
                           (::branching-factor params)
                           layer)]
        (recur (mapv (partial store-index! store height) index-groups)
               (inc height))))))



;; ## Read Functions

(defn- assign-keys
  "Assigns record keys to the children of this node which they would belong
  to. Returns a sequence of vectors containing the child index and a sequence
  of record keys, only including children which had keys assigned."
  [index record-keys]
  (loop [assignments []
         children (::children index)
         split-keys (::keys index)
         pending-keys (sort (set record-keys))]
    (if (seq pending-keys)
      (if (seq split-keys)
        ; Take next batch of keys.
        (let [split (first split-keys)
              [in after] (split-with #(key/before? % split) pending-keys)
              assignments' (if (seq in)
                             (conj assignments [(first children) in])
                             assignments)]
          (recur assignments' (next children) (next split-keys) after))
        ; No more splits, emit one final group with remaining keys.
        (conj assignments [(first children) pending-keys]))
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
          [[child-link child-keys]]
          (let [child (graph/get-link! store node child-link)]
            (read-batch store child fields child-keys)))
        (assign-keys node record-keys))

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

(defn- group-changes
  "Divide up a sorted sequence of changes into a lazy sequence of tuples of
  the first key in the subtree, the child link, and the associated changes, if
  any."
  [child-keys child-links changes]
  (lazy-seq
    (when (seq child-links)
      (if (seq changes)
        (let [next-key (first child-keys)
              [group more-changes]
                (if next-key
                  (split-with #(key/before? (first %) next-key) changes)
                  [changes nil])]
          (cons
            [(first child-links) (seq group)]
            (group-changes (next child-keys)
                           (next child-links)
                           more-changes)))
        ; No more changes
        (map vector child-keys)))))


(defn- redistribute-children
  "... accumulates child nodes and outputs valid nodes as necessary. Input
  values should be a vector of `[first-key link-or-candidate]`."
  [store limit count-children merge-nodes]
  (let [half-full (int (Math/ceil (/ limit 2)))
        valid? #(or (mdag/link? %) (<= half-full (count-children %) limit))]
    (fn [xf]
      (fn
        ([]
         [(xf) nil nil])
        ([[result prev curr]]
         (cond
           ; Current node is first in sequence.
           (nil? prev)
             (if curr
               ; Output only child node.
               (xf (xf (unreduced result) curr))
               ; No further children to process.
               (xf result))

           ; Two valid children, output directly.
           (valid? curr)
             (-> (unreduced result)
                 (xf prev)
                 (xf curr)
                 (xf))

           ; Final child is underflowing, merge with prev.
           :else
             (reduce
               xf
               (unreduced result)
               (merge-nodes
                 (if (mdag/link? prev)
                   (graph/get-link! store prev)
                   prev)
                 curr))))
        ([[result prev curr] [first-key child]]
         ; case [prev curr] input
         ; [nil nil]
         ; Input becomes current node unless it's an overflow, in which case it
         ; is split into multiple conforming nodes.
         ; TODO: might be more optimal to keep an overflowing node in curr,
         ; but would require changes to flush above.
         ;
         ; [nil link]
         ; - link => [link link]
         ; - underflow => [link underflow]
         ; - candidate => [link candidate]
         ; - overflow => split nodes, output all but last two candidates
         ;
         ; [nil underflow]
         ; - link => load link, merge with underflowing node
         ; - underflow => merge with current
         ; - candidate => redistribute with current
         ; - overflow => redistribute with current
         ;
         ; [nil candidate]
         ; - link
         ; - underflow
         ; - candidate
         ; - overflow
         ;
         ; [link link]
         ; - link
         ; - underflow
         ; - candidate
         ; - overflow
         ;
         ; [link underflow]
         ; - link
         ; - underflow
         ; - candidate
         ; - overflow
         ,,,)))))


(defn- update-index-node
  [store params carry [node-link changes]]
  (if (seq changes)
    (let [index (graph/get-link! store node-link)
          child-changes (group-changes
                          (::keys index)
                          (::children index)
                          changes)]
      (if (= 1 (::height index))
        ; Update children as partitions.
        (let [children (part/update-partitions! store params child-changes)]
          ; if carry is a virtual tablet, merge with children
          ; if carry is a partition, add it to the front (or back?) of changes list
          ; otherwise, not valid to have a carry here
          (prn :update-index-partitions children)
          (if (sequential? children)
            (if (= 1 (count children))
              ; Single partition to carry up the tree.
              (first children)
              (let [child-keys (into [] (comp (drop 1) (map ::record/first-key)) children)
                    child-links (into [] (map #(mdag/link "part" (meta %))) children)
                    record-count (reduce + 0 (map ::record/count children))]
                (assoc index
                       ::keys child-keys
                       ::children child-links
                       ::record/count record-count
                       ::record/first-key (::record/first-key (first children))
                       ::record/last-key (::record/last-key (peek children)))))
            ; Virtual tablet, return for carrying.
            children))
        ; Update child index nodes.
        (let [; if carry is an index subtree and the height is one less than
              ; ours, adopt it as a child with no changes. Otherwise, carry it
              ; into the update for the relevant child.
              children (mapv #(update-index-node store params nil %) child-changes)
              ; TODO: redistribute children
              child-keys (into [] (comp (drop 1) (map ::record/first-key)) children)
              record-count (reduce + 0 (map ::record/count children))]
          (prn :update-index-children children)
          (assoc index
                 ::keys child-keys
                 ::children children
                 ::record/count record-count
                 ::record/first-key (::record/first-key (first children))
                 ::record/last-key (::record/last-key (peek children))))))
    ; No changes, return unchanged node.
    (graph/get-link! store node-link)))


(defn- relink-children
  "Rewrite a vector of childen into numbered links."
  [children]
  (into []
        (map-indexed
          #(mdag/link
             (format "%03d" %1)
             (if (mdag/link? %2) %2 (meta %2))))
        children))


(defn update-tree
  "Apply a set of changes to the index tree rooted in the given node. The
  changes should be a sequence of record ids to either data maps or patch
  tombstones. Parameters may include `:merkle-db.partition/limit` and
  `:merkle-db.data/families`. Returns an updated persisted root node if any
  records remain in the tree."
  [store params root changes]
  (if (nil? root)
    ; Empty tree.
    (->> changes
         (part/partition-records store params)
         (from-partitions store params))
    ; Check root node type.
    (condp = (:data/type root)
      part/data-type
        (if (seq changes)
          (some->>
            (part/update-root! store params root changes)
            (from-partitions store params))
          root)
      data-type
        (letfn [(store!  ; TODO: factor out?
                  [node]
                  (if (mdag/link? node)
                    node
                    (->> (::children node)
                         (map store!)
                         (relink-children)
                         (assoc node ::children)
                         (mdag/store-node! store nil)
                         (::node/data))))]
          (store! (update-index-node store params nil [root changes])))
      (throw (ex-info (str "Unsupported index-tree node type: "
                           (pr-str (:data/type root)))
                      {:data/type (:data/type root)})))))
