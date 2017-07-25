(ns merkle-db.record-test
  (:require
    [clojure.test :refer :all]
    (merkle-db
      [key :as key]
      [record :as record])))


(deftest partitioning-utils
  (testing "partition-approx"
    (is (nil? (record/partition-approx 1 [])))
    (is (nil? (record/partition-approx 5 [])))
    (is (= [[:a :b :c]]
           (record/partition-approx 1 [:a :b :c])))
    (is (= [[:a] [:b :c]]
           (record/partition-approx 2 [:a :b :c])))
    (is (= [[:a] [:b] [:c]]
           (record/partition-approx 3 [:a :b :c])))
    (is (= [[:a] [:b] [:c]]
           (record/partition-approx 5 [:a :b :c])))
    (is (= [[:a :b] [:c :d] [:e :f :g]]
           (record/partition-approx 3 [:a :b :c :d :e :f :g]))))
  (testing "partition-limited"
    (is (nil? (record/partition-limited 3 [])))
    (is (= [[:a]]
           (record/partition-limited 3 [:a])))
    (is (= [[:a :b :c]]
           (record/partition-limited 3 [:a :b :c])))
    (is (= [[:a :b] [:c :d :e]]
           (record/partition-limited 3 [:a :b :c :d :e])))
    (is (= [100 100 101 100 101]
           (->>
             (range 502)
             (record/partition-limited 120)
             (map count))))))
