(ns merkle-db.record-test
  (:require
    [clojure.spec :as s]
    [clojure.test :refer :all]
    [merkle-db.key :as key]
    [merkle-db.record :as record]
    [merkle-db.test-utils]))


(deftest record-specs
  (testing "record data"
    (is (invalid? ::record/data nil))
    (is (invalid? ::record/data []))
    (is (valid? ::record/data {}))
    (is (valid? ::record/data {:foo true, :bar "---", :baz 123})))
  (testing "family maps"
    (is (invalid? ::record/families nil))
    (is (invalid? ::record/families []))
    (is (valid? ::record/families {}))
    (is (valid? ::record/families {:bc #{:b :c}}))
    (is (valid? ::record/families {:bc #{:b :c}, :ad #{:a :d}}))
    (is (invalid? ::record/families {"foo" #{:a}}))
    (is (invalid? ::record/families {:qualified/family #{:a}}))
    (is (invalid? ::record/families {:not-a-set [:a]}))
    (is (invalid? ::record/families {:bc #{:b :c}, :cd #{:c :d}}))))


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
