(ns merkle-db.partition-test
  (:require
    [clojure.test :refer :all]
    [merkle-db.key :as key]
    [merkle-db.key-test]
    [merkle-db.node :as node]
    [merkle-db.partition :as part]
    [merkle-db.tablet :as tablet]))


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
        base (->>
               {k0 {:x 0, :y 0}
                k1 {:x 1}
                k2 {}
                k3 {:x 3, :y 3}
                k4 {:z 4}}
               (tablet/from-records)
               (node/store-node! store))
        ab-tab (->>
                 {k0 {:a 0}
                  k2 {:b 2}
                  k3 {:a 3, :b 3}}
                 (tablet/from-records)
                 (node/store-node! store))
        cd-tab (->>
                 {k0 {:c 0}
                  k1 {:c 1, :d 1}
                  k2 {:c 2}
                  k4 {:d 4}}
                 (tablet/from-records)
                 (node/store-node! store))
        part (->>
               {:base (:id base)
                :ab (:id ab-tab)
                :cd (:id cd-tab)}
               (part/from-tablets store))]
    (testing "partition construction"
      (is (= 5 (:merkle-db.data/count part)))
      (is (= k0 (:merkle-db.partition/first-key part)))
      (is (= k4 (:merkle-db.partition/last-key part)))
      (is (= (:id base) (get-in part [:merkle-db.partition/tablets :base])))
      (is (= #{:a :b} (get-in part [:merkle-db.data/families :ab])))
      (is (= #{:c :d} (get-in part [:merkle-db.data/families :cd]))))
    (testing "record reading"
      (is (= [[k0 {:x 0, :a 0}]
              [k1 {:x 1, :d 1}]
              [k2 {}]
              [k3 {:x 3, :a 3}]
              [k4 {:d 4}]]
             (part/read-tablets
               store
               part
               #{:x :a :d}
               tablet/read-all)))
      )))


; TODO: property tests
; - partition data matches schema
; - base tablet contains every record key
; - base tablet does not contain any fields in families
; - family tablets only contain field data for that family
; - family tablets contain no empty values
; - ::count attribute is accurate
; - ::first-key is contained in the base tablet
; - no record key is less than ::first-key
; - ::last-key is contained in the base tablet
; - no record key is greater than ::last-key
; - every record key tests true against the ::membership filter
