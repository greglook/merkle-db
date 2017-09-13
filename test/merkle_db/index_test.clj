(ns merkle-db.index-test
  (:require
    [clojure.set :as set]
    [clojure.spec :as s]
    [clojure.string :as str]
    [clojure.test :refer :all]
    [clojure.test.check.generators :as gen]
    [com.gfredericks.test.chuck.clojure-test :refer [checking]]
    [merkledag.core :as mdag]
    [merkledag.link :as link]
    [merkledag.node :as node]
    (merkle-db
      [generators :as mdgen]
      [graph :as graph]
      [index :as index]
      [key :as key]
      [partition :as part]
      [record :as record]
      #_[validate :as validate]
      [test-utils :as tu])))


(deftest construction-reading
  (let [store (mdag/init-store :types graph/codec-types)
        k0 (key/create [0])
        k1 (key/create [1])
        k2 (key/create [2])
        k3 (key/create [3])
        k4 (key/create [4])
        r0 {:x 0, :y 0, :a 0, :c 0}
        r1 {:x 1, :c 1, :d 1}
        r2 {:b 2, :c 2}
        r3 {:x 3, :y 3, :a 3, :b 3}
        r4 {:z 4, :d 4}
        params {::index/branching-factor 4
                ::part/limit 3
                ::record/families {:ab #{:a :b}, :cd #{:c :d}}}
        part0 (part/from-records store params {k0 r0, k1 r1, k2 r2})
        part1 (part/from-records store params {k3 r3, k4 r4})
        root (index/from-partitions store params [part0 part1])]
    (testing "root properties"
      (is (= index/data-type (:data/type root)))
      (is (= 1 (::index/height root)))
      (is (= [k3] (::index/keys root)))
      (is (= 2 (count (::index/children root))))
      (is (= 5 (::record/count root)))
      (is (= k0 (::record/first-key root)))
      (is (= k4 (::record/last-key root))))
    (testing "read-all"
      (is (= [[k0 r0] [k1 r1] [k2 r2] [k3 r3] [k4 r4]]
             (index/read-all store root nil)))
      (is (= [[k1 {:d 1}] [k4 {:d 4}]]
             (index/read-all store root #{:d}))))
    (testing "read-batch"
      (is (= [[k1 r1] [k2 r2] [k4 r4]]
             (index/read-batch store root nil #{k1 k2 k4 (key/create [7])})))
      (is (= [[k1 {:x 1}] [k3 {:x 3}]]
             (index/read-batch store root #{:x} #{k1 k3 k4}))))
    (testing "read-range"
      (is (= [[k0 r0] [k1 r1] [k2 r2] [k3 r3] [k4 r4]]
             (index/read-range store root nil nil nil)))
      (is (= [[k0 {:y 0}] [k3 {:y 3}]]
             (index/read-range store root #{:y} nil nil)))
      (is (= [[k0 r0] [k1 r1]]
             (index/read-range store root nil nil k1)))
      (is (= [[k2 r2] [k3 r3] [k4 r4]]
             (index/read-range store root nil k2 nil)))
      (is (= [[k2 {:c 2}]]
             (index/read-range store root #{:c} k2 k3))))))


#_
(deftest tree-validation
  (let [store (mdag/init-store :types graph/codec-types)]
    (testing "empty tree"
      (tu/check-asserts
        (validate/run!
          store
          nil
          validate/validate-data-tree
          {::record/count 0})))
    (testing "partition tree"
      (let [part (part/from-records
                   store
                   {::record/families {:ab #{:a :b}, :cd #{:c :d}}}
                   {(key/create [0 1 2]) {:x 0, :y 0, :a 0, :c 0}
                    (key/create [1 2 3]) {:x 1, :c 1, :d 1, }
                    (key/create [2 3 4]) {:b 2, :c 2}
                    (key/create [3 4 5]) {:x 3, :y 3, :a 3, :b 3}
                    (key/create [4 5 6]) {:z 4, :d 4}})]
        (tu/check-asserts
          (validate/run!
            store
            (::node/id (meta part))
            validate/validate-data-tree
            {::record/count 5
             ::part/limit 10}))))))


#_
(deftest ^:generative index-construction
  (checking "valid properties" 20
    [[field-keys families records] mdgen/data-context
     part-limit (gen/large-integer* {:min 4})
     branch-fac (gen/large-integer* {:min 4})]
    (is (valid? ::record/families families))
    (let [store (mdag/init-store :types graph/codec-types)
          params {::record/families families
                  ::record/count (count records)
                  ::index/branching-factor branch-fac
                  ::part/limit part-limit}
          parts (part/partition-records store params records)
          root (index/from-partitions store params parts)]
      (tu/check-asserts
        (validate/run!
          store
          (::node/id (meta root))
          validate/validate-data-tree
          params)))))
