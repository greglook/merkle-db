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
      [key :as key]
      [partition :as part]
      [record :as record]
      [tablet :as tablet] ; would be nice if this weren't needed
      [validate :as validate])))


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


(defn validate
  [store params index]
  (when (validate/check :data/type
          (= data-type (:data/type index))
          (str "Index should have :data/type of " data-type))
    (validate/check ::node-data
      (s/valid? ::node-data index)
      (s/explain-str ::node-data index))
    (validate/check ::keys
      (= (dec (count (::children index)))
         (count (::keys index)))
      "Index nodes have one fewer key than child links")
    (if (::root? params)
      (validate/check ::branching-factor
        (<= 2 (count (::children index)) (::branching-factor params))
        "Root index node has at between [2, b] children")
      (validate/check ::branching-factor
        (<= (int (Math/ceil (/ (::branching-factor params) 2)))
            (count (::children index))
            (::branching-factor params))
        "Internal index node has between [ceil(b/2), b] children"))
    (validate/check ::height
      (= (::height params) (::height index))
      "Index node has expected height")
    (let [result (reduce
                   (fn test-child
                     [result [first-key child-link last-key]]
                     (let [params' (assoc params
                                          ::root? false
                                          ::height (dec (::height index))
                                          ::record/first-key first-key
                                          ::record/last-key last-key)
                           child-result (validate/check-link store child-link
                                          (if (zero? (::height params'))
                                            #(part/validate store params' %)
                                            #(validate store params' %)))]
                       (update result ::record/count + (::record/count child-result 0))))
                   {::record/count 0}
                   (map vector
                        (cons (::record/first-key params) (::keys index))
                        (::children index)
                        (conj (::keys index) (::record/last-key params))))]
      (validate/check ::record/count
        (= (::record/count result) (::record/count index))
        "Aggregate record count matches actual subtree count"))))


(defn validate-tree
  [store params root]
  (cond
    (zero? (::record/count params))
      (validate/check ::empty
        (nil? root)
        "Empty tree has nil root ")
    (<= (::record/count params) (::part/limit params))
      (part/validate store params root)
    :else
      (validate store
                (assoc params
                       ::root? true
                       ::height (::height root))
                root)))



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

(defn build-index
  "Given a sequence of partitions, builds an index tree with the given
  parameters incorporating the partitions. Returns the final, persisted root
  node data."
  [store params partitions]
  (letfn [(store-child
            [height children]
            (->>
              {:data/type data-type
               ::height height
               ::keys (vec (drop 1 (map ::record/first-key children)))
               ::children (->>
                            children
                            (map-indexed
                              #(mdag/link (format "%03d" %1) (meta %2)))
                            (vec))
               ::record/count (reduce + 0 (map ::record/count children))
               ::record/first-key (::record/first-key (first children))
               ::record/last-key (::record/last-key (last children))}
              (mdag/store-node! store nil)
              (::node/data)))]
    (loop [layer partitions
           height 1]
      (if (<= (count layer) 1)
        (first layer)
        (let [index-groups (record/partition-limited
                             (::branching-factor params)
                             layer)]
          (recur (mapv (partial store-child height) index-groups)
                 (inc height)))))))


(defn- update-empty
  "Apply changes to an empty tree, returning an updated root node data."
  [store params changes]
  ; Divide up added records into a sequence of partitions and build an index
  ; over them.
  (let [parts (part/from-records store params changes)]
     ; TODO: if result is a virtual tablet, need to turn it into a partition
     (build-index store params parts)))


(defn group-changes
  "Divides up a sorted sequence of changes into a lazy sequence of tuples of
  the first key in the subtree, the child link, and the associated changes, if
  any."
  [child-keys child-links changes]
  ; all arguments must be sorted
  ; return lazy sequence of ([nil link-0 child-changes] [key-0 link-1 changes] ...)
  (letfn [(next-group
            [nnext-key changes]
            (split-with #(key/before? (first %) nnext-key) changes))
          (group-seq
            [child-keys child-links changes]
            (when (and (seq child-keys) (seq child-links))
              (if (seq changes)
                (let [our-key (first child-keys)
                      next-key (first (next child-keys))
                      [group more-changes]
                        (if next-key
                          (next-group next-key changes)
                          [changes nil])]
                  (cons
                    [our-key (first child-links) (seq group)]
                    (lazy-seq
                      (group-seq (next child-keys)
                                 (next child-links)
                                 more-changes))))
                ; No more changes
                (map vector child-keys child-links))))]
    (lazy-seq (group-seq (cons nil child-keys) child-links changes))))


(defn- redistribute-children
  "Transducer which accumulates child nodes and outputs valid nodes as
  necessary. Input values should be a vector of
  `[first-key link-or-candidate]`."
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
                   (mdag/get-data store prev)
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
  [store params carry [first-key node-link changes]]
  (if (seq changes)
    (let [index (mdag/get-data store node-link)
          child-changes (group-changes
                          (::keys index)
                          (::children index)
                          changes)]
      (if (= 1 (::height index))
        ; Update children as partitions.
        (let [result (part/update-partitions! store params child-changes)]
          (if (sequential? result)
            (if (= 1 (count result))
              ; Single partition to carry up the tree.
              (first result)
              (let [child-keys (into [] (comp (drop 1) (map first)) result)
                    child-links (into [] (map second) result)
                    ; TODO: this feels inefficient
                    record-count (reduce + 0 (map (comp ::record/count (partial mdag/get-data store))
                                                  child-links))
                    first-key (ffirst result)
                    last-key (if (= (:target (peek (::children index)))
                                    (:target (peek child-links)))
                               (::record/last-key index)
                               (::record/last-key (mdag/get-data store (peek child-links))))]
                [first-key
                 (assoc index
                        ::keys child-keys
                        ::children child-links
                        ::record/count record-count
                        ::record/first-key first-key
                        ::record/last-key last-key)]))
            ; Virtual tablet, return for carrying.
            [(tablet/first-key result) result]))
        ; Update child index nodes.
        (let [result (mapcat #(update-index-node store params %) child-changes)
              ; TODO: redistribute children
              child-keys (into [] (comp (drop 1) (map first)) result)
              children (into [] (map second) result)
              record-count 0 ; FIXME
              first-key (if (map? (first children))
                          (::record/first-key (first children))
                          (::record/first-key index))
              last-key (if (map? (peek children))
                         (::record/last-key (peek children))
                         (::record/last-key index))]
          [first-key
           (assoc index
                  ::keys child-keys
                  ::children children
                  ::record/count record-count
                  ::record/first-key first-key
                  ::record/last-key last-key)])))
    ; No changes, return key and link unchanged.
    [[first-key node-link]]))


(defn- update-index
  "Apply changes to an index subtree, returning data for the updated root node."
  [store params index changes]
  (prn :update-index index changes)
  ; FIXME: no way this is right
  (let [root' (second (update-index-node store params nil [nil index changes]))]
    (::node/data (mdag/store-node! store nil root'))))


(defn update-tree
  "Apply a set of changes to the index tree rooted in the given node. The
  changes should be a sequence of record ids to either data maps or patch
  tombstones. Parameters may include `:merkle-db.partition/limit` and
  `:merkle-db.data/families`. Returns an updated persisted root node if any
  records remain in the tree."
  [store params root changes]
  (if (nil? root)
    ; Empty tree.
    (update-empty store params changes)
    ; Check root node type.
    (condp = (:data/type root)
      part/data-type
        (if (seq changes)
          (some->>
            (update-partition store params root changes)
            (build-index store params))
          root)
      data-type
        (update-index store params root changes)
      (throw (ex-info (str "Unsupported index-tree node type: "
                           (pr-str (:data/type root)))
                      {:data/type (:data/type root)})))))
