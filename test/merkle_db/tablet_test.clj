(ns merkle-db.tablet-test
  (:require
    [clojure.spec :as s]
    [clojure.test :refer :all]
    [merkle-db.key :as key]
    [merkle-db.key-test]
    [merkle-db.tablet :as tablet]))


(deftest constants
  (is (s/valid? :merkle-db/tablet tablet/empty-tablet))
  (is (empty? (tablet/read-all tablet/empty-tablet))))


(deftest batch-reads
  (let [k1 (key/create 1 2 3)
        r1 {:foo 123}
        k2 (key/create 4 5 6)
        r2 {:foo 456}
        tablet (tablet/from-records {k1 r1, k2 r2})]
    (is (empty? (tablet/read-batch tablet/empty-tablet nil)))
    (is (= [[k1 r1]]
           (tablet/read-batch tablet #{k1})))
    (is (= [[k1 r1] [k2 r2]]
           (tablet/read-batch tablet #{k1 k2})))))


(deftest range-reads
  (let [k1 (key/create 1 2 3)
        r1 {:foo 123}
        k2 (key/create 4 5 6)
        r2 {:foo 456}
        tablet (tablet/from-records {k1 r1, k2 r2})]
    (is (empty? (tablet/read-range tablet/empty-tablet nil nil)))
    (is (= [[k1 r1] [k2 r2]]
           (tablet/read-range tablet nil nil)))
    (is (= [[k1 r1]]
           (tablet/read-range tablet nil (key/create 2))))
    (is (= [[k2 r2]]
           (tablet/read-range tablet (key/create 2) nil)))
    (is (empty?
          (tablet/read-range tablet (key/create 2) (key/create 3))))))


#_
(deftest slice-reads
  (let [k1 (key/create 1 2 3)
        r1 {:foo 123}
        k2 (key/create 4 5 6)
        r2 {:foo 456}
        tablet (tablet/from-records {k1 r1, k2 r2})]
    (is (empty? (tablet/read-slice tablet/empty-tablet nil nil)))
    (is (= [[k1 r1] [k2 r2]]
           (tablet/read-slice tablet nil nil)))
    (is (= [[k1 r1] [k2 r2]]
           (tablet/read-slice tablet 0 1)))
    (is (= [[k1 r1]]
           (tablet/read-slice tablet nil 0)))
    (is (= [[k1 r1]]
           (tablet/read-slice tablet 0 0)))
    (is (= [[k2 r2]]
           (tablet/read-slice tablet 1 nil)))
    (is (= [[k2 r2]]
           (tablet/read-slice tablet 1 3)))
    (is (empty?
          (tablet/read-slice tablet 3 nil)))))


(deftest record-addition
  (let [k1 (key/create 1 2 3)
        r1 {:foo 123}
        k2 (key/create 4 5 6)
        r2 {:foo 456}
        tablet (tablet/from-records {k1 r1})]
    (is (= [[k1 {:foo 124}]]
           (tablet/read-all
             (tablet/merge-records
               tablet
               tablet/merge-fields
               {k1 {:foo 124}}))))
    (is (= [[k1 {:foo 123, :bar true}]]
           (tablet/read-all
             (tablet/merge-records
               tablet
               tablet/merge-fields
               {k1 {:bar true}}))))
    (is (= [[k1 r1] [k2 r2]]
           (tablet/read-all
             (tablet/merge-records
               tablet
               tablet/merge-fields
               {k2 r2}))))))


(deftest record-removal
  (let [k1 (key/create 1 2 3)
        r1 {:foo 123}
        k2 (key/create 4 5 6)
        r2 {:foo 456}
        tablet (tablet/from-records {k1 r1, k2 r2})]
    (is (nil? (tablet/remove-records tablet/empty-tablet #{})))
    (is (= [[k1 r1] [k2 r2]]
           (tablet/read-all
             (tablet/remove-records tablet nil))))
    (is (= [[k2 r2]]
           (tablet/read-all
             (tablet/remove-records tablet #{k1}))))
    (is (= [[k1 r1]]
           (tablet/read-all
             (tablet/remove-records tablet #{k2}))))
    (is (nil? (tablet/remove-records tablet #{k1 k2})))))


; TODO: property tests
; - tablet data matches schema
; - tablet records are sorted by key
