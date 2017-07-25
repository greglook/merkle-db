(ns merkle-db.index-test
  (:require
    [clojure.set :as set]
    [clojure.spec :as s]
    [clojure.test :refer :all]
    [clojure.test.check.generators :as gen]
    [com.gfredericks.test.chuck.clojure-test :refer [checking]]
    [merkledag.core :as mdag]
    (merkle-db
      [generators :as mdgen]
      [index :as index]
      [key :as key]
      [partition :as part]
      [record :as record])))


(defn validate-partition-node
  [store params part]
  (is (valid? ::part/node-data part)
      "partition node data spec is valid")
  ; TODO: warn when partition limit or families don't match params
  (when (<= (::part/limit part) (::record/count params))
    (is (<= (Math/ceil (/ (::part/limit part) 2)) (::record/count part))
            "partition is at least half full if tree has at least :merkle-db.partition/limit records"))
  (when (::first-key params)
    (is (not (key/before? (::part/first-key part) (::first-key params)))
        "first key in partition is not before the subtree boundary"))
  (when (::last-key params)
    (is (not (key/after? (::part/last-key part) (::last-key params)))
        "last key in partition is not after the subtree boundary"))
  ; TODO: check tablets for validity
  ,,,)


(defn validate-index-node
  [store params index]
  (is (valid? ::index/node-data index)
      "index node data spec is valid")
  (if (::root? params)
    (is (<= 2
            (count (::index/children index))
            (::index/branching-factor params))
        "root index node has at between [2, b] children")
    (is (<= (int (Math/ceil (/ (::index/branching-factor params) 2)))
            (count (::index/children index)))
        "internal index node has between [ceil(b/2), b] children"))
  (is (= (dec (count (::index/children index)))
         (count (::index/keys index)))
      "index nodes have one fewer key than child links")
  (is (= (::height params) (::index/height index))
      "index node has expected height")
  (let [result (reduce
                 (fn test-child
                   [result [first-key child-link last-key]]
                   (let [params' (assoc params
                                        ::root? false
                                        ::height (dec (::index/height index))
                                        ::first-key first-key
                                        ::last-key last-key)
                         child (mdag/get-data store child-link)
                         child-result (if (zero? (::height params'))
                                        (validate-partition-node store params' child)
                                        (validate-index-node store params' child))]
                     (update result ::record/count + (::record/count child-result))))
                 {::record/count 0}
                 (map vector
                      (cons (::first-key params) (::index/keys index))
                      (::index/children index)
                      (conj (::index/keys index) (::last-key params))))]
    (is (= (::record/count result) (::record/count index))
        "index ::record/count matches actual subtree count")))


(defn validate-tree
  [store params root-id]
  (let [root (mdag/get-data store root-id)]
    (cond
      (zero? (::record/count params))
        (is (nil? root-id) "root node is nil if tree is empty")
      (<= (::record/count params) (::part/limit params))
        (do (is (= part/data-type (:data/type root))
                "root node is a partition if tree has fewer than :merkle-db.partition/limit records")
            (validate-partition-node store params root))
      :else
        (do (is (= index/data-type (:data/type root))
                "root node is an index node if tree has more than one partition")
            (validate-index-node
              store
              (assoc params
                     ::root? true
                     ::height (::index/height root))
              root)))))
