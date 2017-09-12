(ns merkle-db.record-test
  (:require
    [clojure.test :refer :all]
    (merkle-db
      [key :as key]
      [record :as record])))


(deftest family-grouping
  (let [k0 (key/create [0])
        r0 {}
        k1 (key/create [1])
        r1 {:a 5, :x true}
        k2 (key/create [2])
        r2 {:b 7, :y false}]
    (is (= {} (record/split-data {} [])))
    (is (= {:base {k0 {}}}
           (record/split-data {} [[k0 {}]])))
    (is (= {:base {k0 {}
                   k1 {:x true}
                   k2 {:y false}}
            :ab   {k0 nil
                   k1 {:a 5}
                   k2 {:b 7}}}
           (record/split-data {:ab #{:a :b}} [[k0 r0] [k1 r1] [k2 r2]])))))
