(ns merkle-db.partition-test
  (:require
    [clojure.set :as set]
    [clojure.spec :as s]
    [clojure.test :refer :all]
    [clojure.test.check.generators :as gen]
    [com.gfredericks.test.chuck.clojure-test :refer [checking]]
    [merkle-db.data :as data]
    [merkle-db.generators :as mdgen]
    [merkle-db.key :as key]
    [merkle-db.key-test]
    [merkle-db.node :as node]
    [merkle-db.partition :as part]
    [merkle-db.tablet :as tablet]))


;; ## Unit Tests

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


(deftest partition-logic
  (let [store (node/memory-node-store)
        k0 (key/create [0 1 2])
        k1 (key/create [1 2 3])
        k2 (key/create [2 3 4])
        k3 (key/create [3 4 5])
        k4 (key/create [4 5 6])
        [part] (part/from-records
                 store
                 {::data/families {:ab #{:a :b}, :cd #{:c :d}}}
                 tablet/merge-fields
                 {k0 {:x 0, :y 0, :a 0, :c 0}
                  k1 {:x 1, :c 1, :d 1, }
                  k2 {:b 2, :c 2}
                  k3 {:x 3, :y 3, :a 3, :b 3}
                  k4 {:z 4, :d 4}})]
    (testing "partition construction"
      (is (= 5 (::data/count part)))
      (is (= k0 (::part/first-key part)))
      (is (= k4 (::part/last-key part)))
      (is (= #{:base :ab :cd} (set (keys (::part/tablets part)))))
      (is (= #{:a :b} (get-in part [::data/families :ab])))
      (is (= #{:c :d} (get-in part [::data/families :cd]))))
    (testing "record reading"
      (is (= [[k0 {:x 0, :a 0}]
              [k1 {:x 1, :d 1}]
              [k2 {}]
              [k3 {:x 3, :a 3}]
              [k4 {:d 4}]]
             (part/read-all store part #{:x :a :d}))))))



;; ## Property Tests

(deftest partition-behavior
  (checking "valid properties" 20
    [[field-keys families records] mdgen/data-context]
    (is (valid? ::data/families families))
    (let [store (node/memory-node-store)
          [part] (part/from-records store {::data/families families} tablet/merge-fields records)
          tablets (into {}
                        (map (juxt key #(node/get-data store (val %))))
                        (::part/tablets part))]
      (is (valid? :merkle-db/partition part)
          "partition data should match schema")
      (doseq [[family-key tablet] tablets]
        (is (valid? :merkle-db/tablet tablet)
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
      (is (= (count (part/read-all store part nil)) (::data/count part))
          "::count attribute is accurate")
      (is (= (first (tablet/keys (:base tablets)))
             (::part/first-key part))
          "::first-key is first key in the base tablet")
      (is (= (last (tablet/keys (:base tablets)))
             (::part/last-key part))
          "::last-key is last key in the base tablet")
      (is (every? (::part/membership part) (tablet/keys (:base tablets)))
          "every record key tests true against the ::membership filter"))))
