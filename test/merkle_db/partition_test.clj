(ns merkle-db.partition-test
  (:require
    [clojure.set :as set]
    [clojure.spec :as s]
    [clojure.test :refer :all]
    [clojure.test.check.generators :as gen]
    [com.gfredericks.test.chuck.clojure-test :refer [checking]]
    [merkledag.core :as mdag]
    (merkle-db
      [generators :as mdgen]
      [key :as key]
      [partition :as part]
      [patch :as patch]
      [record :as record]
      [tablet :as tablet])))


;; ## Unit Tests

(deftest partitioning-utils
  (testing "partition-limited"
    (is (nil? (part/partition-limited 3 [])))
    (is (= [[:a]]
           (part/partition-limited 3 [:a])))
    (is (= [[:a :b :c]]
           (part/partition-limited 3 [:a :b :c])))
    (is (= [[:a :b] [:c :d :e]]
           (part/partition-limited 3 [:a :b :c :d :e])))
    (is (= [100 100 101 100 101]
           (->>
             (range 502)
             (part/partition-limited 120)
             (map count))))))


(deftest tablet-selection
  (let [choose @#'part/choose-tablets]
    (is (= #{:base} (choose {} nil))
        "nil selection includes :base, even if it's not mapped")
    (is (= #{:base} (choose {:base #{"a"}} #{"b"}))
        "base fields are ignored")
    (is (= #{:base :foo} (choose {:foo #{"a"}} #{"a" "b"})))
    (is (= #{:foo} (choose {:foo #{"a"}} #{"a"})))
    (is (= #{:a :b :c} (choose {:a #{"a1" "a2" "a3"}
                                :b #{"b1" "b2"}
                                :c #{"c1" "c2" "c3"}}
                               #{"a1" "a2" "b2" "c3"})))))


(deftest record-seqing
  (let [record-seq @#'part/record-seq
        k0 (key/create [0 1 2])
        k1 (key/create [1 2 3])
        k2 (key/create [2 3 4])
        k3 (key/create [3 4 5])
        k4 (key/create [4 5 6])
        k5 (key/create [5 6 7])
        k6 (key/create [6 7 8])
        k7 (key/create [7 8 9])
        seq-a [[k0 {:a 0}] [k1 {:a 1}] [k3 {:a 3}] [k4 {:a 4}] [k7 {:a 7}]]
        seq-b [[k0 {:b 0}] [k2 {:b 2}] [k3 {:b 3}] [k5 {:b 5}] [k7 {:b 7}]]
        seq-c [[k1 {:c 1}] [k2 {:c 2}] [k4 {:c 4}] [k7 {:c 7}]]]
    (is (= [[k0 {:a 0}]] (record-seq [[[k0 {:a 0}]]]))
        "simplest example")
    (is (= [[k0 {:a 0, :b 0}]
            [k1 {:a 1, :c 1}]
            [k2 {:b 2, :c 2}]
            [k3 {:a 3, :b 3}]
            [k4 {:a 4, :c 4}]
            [k5 {:b 5}]
            [k7 {:a 7, :b 7, :c 7}]]
           (record-seq [seq-a seq-b seq-c])))))


(deftest partition-construction
  (let [store (mdag/init-store :types record/codec-types)
        k0 (key/create [0 1 2])
        k1 (key/create [1 2 3])
        k2 (key/create [2 3 4])
        k3 (key/create [3 4 5])
        k4 (key/create [4 5 6])
        part (part/from-records
               store
               {::record/families {:ab #{:a :b}, :cd #{:c :d}}}
               {k0 {:x 0, :y 0, :a 0, :c 0}
                k1 {:x 1, :c 1, :d 1, }
                k2 {:b 2, :c 2}
                k3 {:x 3, :y 3, :a 3, :b 3}
                k4 {:z 4, :d 4}})]
    (testing "bad partition"
      (is (thrown? Exception
            (part/from-records
              store
              {::part/limit 3}
              {k0 {:x 0, :y 0, :a 0, :c 0}
               k1 {:x 1, :c 1, :d 1, }
               k2 {:b 2, :c 2}
               k3 {:x 3, :y 3, :a 3, :b 3}}))))
    (testing "properties"
      (is (= 5 (::record/count part)))
      (is (= k0 (::record/first-key part)))
      (is (= k4 (::record/last-key part)))
      (is (= #{:base :ab :cd} (set (keys (::part/tablets part)))))
      (is (= #{:a :b} (get-in part [::record/families :ab])))
      (is (= #{:c :d} (get-in part [::record/families :cd]))))))


(deftest partition-reading
  (let [store (mdag/init-store :types record/codec-types)
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
        part (part/from-records
               store
               {::record/families {:ab #{:a :b}, :cd #{:c :d}}}
               {k0 r0, k1 r1, k2 r2, k3 r3, k4 r4})]
    (testing "read-all"
      (is (= [[k0 r0] [k1 r1] [k2 r2] [k3 r3] [k4 r4]]
             (part/read-all store part nil)))
      (is (= [[k0 {:x 0, :a 0}]
              [k1 {:x 1, :d 1}]
              [k2 {}]
              [k3 {:x 3, :a 3}]
              [k4 {:d 4}]]
             (part/read-all store part #{:x :a :d}))))
    (testing "read-batch"
      (is (= [[k1 r1] [k3 r3]]
             (part/read-batch store part nil #{k1 k3})))
      (is (= [[k0 {:c 0}] [k2 {:c 2}] [k4 {}]]
             (part/read-batch store part #{:c} #{k0 k2 k4 (key/create [7])}))))
    (testing "read-range"
      (is (= [[k0 r0] [k1 r1] [k2 r2] [k3 r3] [k4 r4]]
             (part/read-range store part nil nil nil)))
      (is (= [[k0 {:c 0, :x 0}] [k1 {:c 1, :x 1}] [k2 {:c 2}]]
             (part/read-range store part #{:c :x} nil k2)))
      (is (= [[k2 {}] [k3 {:x 3}] [k4 {:z 4}]]
             (part/read-range store part #{:x :z} k2 nil)))
      (is (= [[k2 r2] [k3 r3]]
             (part/read-range store part nil k2 k3))))))


(deftest partition-updating
  (let [store (mdag/init-store :types record/codec-types)
        k0 (key/create [0])
        k1 (key/create [1])
        k2 (key/create [2])
        k3 (key/create [3])
        k4 (key/create [4])
        k5 (key/create [5])
        k6 (key/create [6])
        k7 (key/create [7])
        params {::part/limit 4
                ::record/families {:ab #{:a :b}, :cd #{:c :d}}}
        p0 (part/from-records
             store
             params
             {k0 {:x 0, :y 0, :a 0, :c 0}
              k2 {:x 1, :c 1, :d 1, }
              k4 {:z 4, :d 4}})
        p1 (part/from-records
             store
             params
             {k5 {:x 5, :d 5}
              k6 {:a 6, :c 6}})]
    (testing "full removals"
      (is (nil? (part/update-partitions!
                  store params
                  [[(mdag/link "B" p1) [[k5 ::patch/tombstone]
                                        [k6 ::patch/tombstone]]]])))
      (is (nil? (part/update-partitions!
                  store params
                  [[(tablet/from-records [[k5 {}] [k6 {}]])
                    [[k5 ::patch/tombstone]
                     [k6 ::patch/tombstone]]]]))))
    (testing "underflow to virtual tablet"
      (let [vt (part/update-partitions!
                 store params
                 [[(mdag/link "B" p1) [[k6 ::patch/tombstone]]]])]
        (is (= tablet/data-type (:data/type vt)))
        (is (= [[k5 {:x 5, :d 5}]] (tablet/read-all vt)))))
    (testing "pass-through logic"
      (is (= [p0] (part/update-partitions!
                    store params
                    [[(mdag/link "A" p0) []]])))
      (let [vt (tablet/from-records [[k5 {}]])]
        (is (= vt (part/update-partitions!
                    store params
                    [[vt []]])))))
    (testing "unchanged data"
      (is (= [p0] (part/update-partitions!
                    store params
                    [[(mdag/link "A" p0) [[k1 ::patch/tombstone]]]])))
      (let [vt (tablet/from-records [[k5 {}]])]
        (is (= vt (part/update-partitions!
                    store params
                    [[vt [[k5 {}]]]])))))
    (testing "pending overflow"
      (let [[a b :as parts]
              (part/update-partitions!
                store params
                [[(mdag/link "A" p0)
                  [[k1 {}]
                   [k3 {}]
                   [k5 {}]
                   [k6 {}]
                   [k7 {}]]]])]
        (is (= 2 (count parts)))
        (is (= [[k0 {:c 0, :a 0, :x 0, :y 0}]
                [k1 {}]
                [k2 {:c 1, :d 1, :x 1}]
                [k3 {}]]
               (part/read-all store a nil)))
        (is (= [[k4 {:z 4, :d 4}]
                [k5 {}]
                [k6 {}]
                [k7 {}]]
               (part/read-all store b nil)))))
    (testing "final underflow"
      (let [[a :as parts]
              (part/update-partitions!
                store params
                [[(mdag/link "A" p0) []]
                 [(mdag/link "B" p1) [[k5 ::patch/tombstone]]]])]
        (is (= 1 (count parts)))
        (is (= [[k0 {:c 0, :a 0, :x 0, :y 0}]
                [k2 {:c 1, :d 1, :x 1}]
                [k4 {:d 4, :z 4}]
                [k6 {:a 6, :c 6}]]
               (part/read-all store a nil)))))))



;; ## Property Tests

(deftest ^:generative partition-behavior
  (checking "valid properties" 20
    [[field-keys families records] mdgen/data-context]
    (is (valid? ::record/families families))
    (let [store (mdag/init-store :types record/codec-types)
          part (part/from-records store {::record/families families} records)
          tablets (into {}
                        (map (juxt key #(mdag/get-data store (val %))))
                        (::part/tablets part))]
      (is (valid? ::part/node-data part)
          "partition data should match schema")
      (doseq [[family-key tablet] tablets]
        (is (valid? ::tablet/node-data tablet)
            (str "partition tablet " family-key " should match schema")))
      (let [all-keys (set (map first (mapcat tablet/read-all (vals tablets))))]
        (is (contains? tablets :base)
            "partition contains a base tablet")
        (is (= all-keys (set (tablet/keys (:base tablets))))
            "base tablet contains every record key"))
      (is (empty? (set/intersection
                    (tablet/fields-present (:base tablets))
                    (set (mapcat val families))))
          "base tablet record data does not contain any fields in families")
      (doseq [[family-key tablet] (dissoc tablets :base)]
        (is (empty? (set/difference
                      (tablet/fields-present tablet)
                      (get families family-key)))
            "family tablet should only contain field data for that family")
        (is (zero? (count (filter empty? (map second (tablet/read-all tablet)))))
            "family tablet should not contain empty data values"))
      (is (= (count (part/read-all store part nil)) (::record/count part))
          "::count attribute is accurate")
      (is (= (first (tablet/keys (:base tablets)))
             (::record/first-key part))
          "::first-key is first key in the base tablet")
      (is (= (last (tablet/keys (:base tablets)))
             (::record/last-key part))
          "::last-key is last key in the base tablet")
      (is (every? (::part/membership part) (tablet/keys (:base tablets)))
          "every record key tests true against the ::membership filter"))))
