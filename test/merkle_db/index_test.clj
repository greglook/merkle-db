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
      [patch :as patch]
      [record :as record]
      [validate :as validate]
      [test-utils :as tu])))


(def k0 (key/create [0]))
(def k1 (key/create [1]))
(def k2 (key/create [2]))
(def k3 (key/create [3]))
(def k4 (key/create [4]))

(def r0 {:x 0, :y 0, :a 0, :c 0})
(def r1 {:x 1, :c 1, :d 1})
(def r2 {:b 2, :c 2})
(def r3 {:x 3, :y 3, :a 3, :b 3})
(def r4 {:z 4, :d 4})

(def params
  {::index/branching-factor 4
   ::part/limit 3
   ::record/families {:ab #{:a :b}, :cd #{:c :d}}})


(deftest record-assigment
  (let [assign-records @#'index/assign-records]
    (is (= [[:a [[k0 r0] [k1 ::patch/tombstone]]]
            [:b [[k2 r2] [k3 ::patch/tombstone]]]]
           (assign-records
             {::index/keys [k2]
              ::index/children [:a :b]}
             [[k0 r0] [k1 ::patch/tombstone]
              [k2 r2] [k3 ::patch/tombstone]])))
    (is (= [[:a nil]
            [:b [[k1 ::patch/tombstone]]]
            [:c [[k2 r2] [k3 ::patch/tombstone]]]]
           (assign-records
             {::index/keys [k0 k2]
              ::index/children [:a :b :c]}
             [[k1 ::patch/tombstone] [k2 r2] [k3 ::patch/tombstone]])))
    (is (= [[:a [[k0 r0] [k1 ::patch/tombstone]]]
            [:b [[k2 r2] [k3 ::patch/tombstone]]]
            [:c nil]]
           (assign-records
             {::index/keys [k2 k4]
              ::index/children [:a :b :c]}
             [[k0 r0] [k1 ::patch/tombstone]
              [k2 r2] [k3 ::patch/tombstone]])))))


(deftest index-reading
  (let [store (mdag/init-store :types graph/codec-types)
        params {::index/branching-factor 4
                ::part/limit 3
                ::record/families {:ab #{:a :b}, :cd #{:c :d}}}
        part0 (part/from-records store params {k0 r0, k1 r1, k2 r2})
        part1 (part/from-records store params {k3 r3, k4 r4})
        root (index/build-tree store params [part0 part1])]
    (testing "root properties"
      (is (= index/data-type (:data/type root)))
      (is (= 1 (::index/height root)))
      (is (= [k3] (::index/keys root)))
      (is (= 2 (count (::index/children root))))
      (is (= 5 (::record/count root)))
      (is (= k0 (::record/first-key root)))
      (is (= k4 (::record/last-key root))))
    (testing "read-all"
      (is (thrown? Exception
            (index/read-all store {:data/type :foo} nil)))
      (is (= [[k0 r0] [k1 r1] [k2 r2] [k3 r3] [k4 r4]]
             (index/read-all store root nil)))
      (is (= [[k1 {:d 1}] [k4 {:d 4}]]
             (index/read-all store root #{:d}))))
    (testing "read-batch"
      (is (thrown? Exception
            (index/read-batch store {:data/type :foo} nil nil)))
      (is (= [[k1 r1] [k2 r2] [k4 r4]]
             (index/read-batch store root nil #{k1 k2 k4 (key/create [7])})))
      (is (= [[k1 {:x 1}] [k3 {:x 3}]]
             (index/read-batch store root #{:x} #{k1 k3 k4}))))
    (testing "read-range"
      (is (thrown? Exception
            (index/read-range store {:data/type :foo} nil nil nil)))
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


(deftest index-update-empty-root
  (let [store (mdag/init-store :types graph/codec-types)
        root (index/update-tree store params nil [[k0 r0] [k1 ::patch/tombstone]])]
    (is (nil? (index/update-tree store params nil nil)))
    (is (= part/data-type (:data/type root)))
    (is (= 1 (::record/count root)))
    (is (= [[k0 r0]] (index/read-all store root nil)))))


(deftest index-updates-partition-root
  (let [store (mdag/init-store :types graph/codec-types)
        part0 (part/from-records store params {k0 r0, k1 r1, k2 r2})
        root (index/update-tree store params part0 [[k3 r3] [k4 r4]])]
    (is (= part0 (index/update-tree store params part0 [])))
    (is (= index/data-type (:data/type root)))
    (is (= 5 (::record/count root)))
    (is (= 2 (count (::index/children root))))
    (is (= [[k0 r0] [k1 r1] [k2 r2] [k3 r3] [k4 r4]]
           (index/read-all store root nil)))))


(deftest index-updates-index-root
  (let [store (mdag/init-store :types graph/codec-types)
        k5 (key/create [5])
        k6 (key/create [6])
        k7 (key/create [7])
        k8 (key/create [8])
        k9 (key/create [9])
        r5 {:a 5, :x 5}
        r6 {:b 6, :y 6}
        r7 {:b 7, :y 7}
        r8 {:b 8, :y 8}
        r9 {:b 9, :y 9}
        part0 (part/from-records store params {k0 r0, k1 r1})
        part1 (part/from-records store params {k3 r3, k4 r4})
        part2 (part/from-records store params {k5 r5, k7 r7})
        part3 (part/from-records store params {k8 r8, k9 r9})
        idx00 (index/build-tree store params [part0 part1])
        idx01 (index/build-tree store params [part2 part3])
        idx10 (index/build-tree store params [idx00 idx01])
        root (index/update-tree store params idx10 [[k2 r2] [k6 r6] [k7 ::patch/tombstone]])]
    ;(prn ::root root)
    (is (= index/data-type (:data/type root)))
    (is (= 9 (::record/count root)))
    (is (= 4 (count (::index/children root))))
    (is (= k0 (::record/first-key root)))
    (is (= k9 (::record/last-key root)))
    (is (= [[k0 r0] [k1 r1] [k2 r2] [k3 r3] [k4 r4]
            [k5 r5] [k6 r6] [k8 r8] [k9 r9]]
           (index/read-all store root nil)))))


(deftest ^:generative index-updating
  (checking "tree updates" 20
    [[field-keys families records] mdgen/data-context
     ; TODO: pick extant, inserts, and deletes
     part-limit (gen/large-integer* {:min 4})
     branch-fac (gen/large-integer* {:min 4})]
    (is (valid? ::record/families families))
    (let [store (mdag/init-store :types graph/codec-types)
          params {::record/families families
                  ::record/count (count records)
                  ::index/branching-factor branch-fac
                  ::part/limit part-limit}
          root (index/from-records store params records)]
      (tu/check-asserts
        (validate/run!
          store
          (::node/id (meta root))
          validate/validate-data-tree
          params)))))
