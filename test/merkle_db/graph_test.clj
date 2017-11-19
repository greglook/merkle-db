(ns merkle-db.graph-test
  (:require
    [clojure.test :refer :all]
    [merkle-db.graph :as graph]
    [merkledag.core :as mdag]
    [merkledag.node :as node]
    [multihash.digest :as digest]))


(deftest link-loading
  (let [store (mdag/init-store :types graph/codec-types)
        n0 (mdag/store-node! store nil {:x 123})
        n1 (mdag/store-node! store [(mdag/link "n0" n0)] nil)
        n2-data [(mdag/link "n0" n0) (mdag/link "n1" n1)]
        n2 (mdag/store-node! store nil n2-data)]
    (is (= n2-data (graph/get-link! store (mdag/link "n2" n2))))
    (is (= {:x 123} (graph/get-link! store n2 (first n2-data))))
    (is (nil? (graph/get-link! store n2 (second n2-data))))
    (is (thrown? Exception
          (graph/get-link! store (mdag/link "foo" (digest/sha1 "foo")))))
    (is (thrown? Exception
          (graph/get-link! store n0 (mdag/link "bar" (digest/sha1 "bar")))))))
