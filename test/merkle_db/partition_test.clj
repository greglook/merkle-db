(ns merkle-db.partition-test
  (:require
    [clojure.test :refer :all]
    [merkle-db.key :as key]
    [merkle-db.key-test :refer [->key]]
    [merkle-db.partition :as part]))


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
        k0 (->key 0 1 2)
        k1 (->key 1 2 3)
        k2 (->key 2 3 4)
        k3 (->key 3 4 5)
        k4 (->key 4 5 6)
        k5 (->key 5 6 7)
        k6 (->key 6 7 8)
        k7 (->key 7 8 9)
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
