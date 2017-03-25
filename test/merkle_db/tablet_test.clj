(ns merkle-db.tablet-test
  (:require
    [clojure.spec :as s]
    [clojure.test :refer :all]
    [merkle-db.key-test :refer [->key]]
    [merkle-db.tablet :as tablet]))


(deftest constants
  (is (s/valid? :merkle-db/tablet tablet/empty-tablet))
  (is (empty? (tablet/read tablet/empty-tablet))))


(deftest batch-reads
  (let [k1 (->key 1 2 3)
        r1 {:foo 123}
        k2 (->key 4 5 6)
        r2 {:foo 456}
        tablet (tablet/from-records {k1 r1, k2 r2})]
    (is (empty? (tablet/read-batch tablet/empty-tablet nil)))
    (is (= [[k1 r1]]
           (tablet/read-batch tablet #{k1})))
    (is (= [[k1 r1] [k2 r2]]
           (tablet/read-batch tablet #{k1 k2})))))


(deftest range-reads
  (let [k1 (->key 1 2 3)
        r1 {:foo 123}
        k2 (->key 4 5 6)
        r2 {:foo 456}
        tablet (tablet/from-records {k1 r1, k2 r2})]
    (is (empty? (tablet/read-range tablet/empty-tablet nil nil)))
    (is (= [[k1 r1] [k2 r2]]
           (tablet/read-range tablet nil nil)))
    (is (= [[k1 r1]]
           (tablet/read-range tablet nil (->key 2))))
    (is (= [[k2 r2]]
           (tablet/read-range tablet (->key 2) nil)))
    (is (empty?
          (tablet/read-range tablet (->key 2) (->key 3))))))


(deftest slice-reads
  (let [k1 (->key 1 2 3)
        r1 {:foo 123}
        k2 (->key 4 5 6)
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


(deftest batch-updates
  (let [k1 (->key 1 2 3)
        r1 {:foo 123}
        k2 (->key 4 5 6)
        r2 {:foo 456}
        tablet (tablet/from-records {k1 r1})]
    (is (= [[k1 {:foo 124}]]
           (tablet/read
             (tablet/update-batch
               tablet
               tablet/merge-fields
               {k1 {:foo 124}}))))
    (is (= [[k1 {:foo 123, :bar true}]]
           (tablet/read
             (tablet/update-batch
               tablet
               tablet/merge-fields
               {k1 {:bar true}}))))
    (is (= [[k1 r1] [k2 r2]]
           (tablet/read
             (tablet/update-batch
               tablet
               tablet/merge-fields
               {k2 r2}))))))


(deftest batch-deletes
  (let [k1 (->key 1 2 3)
        r1 {:foo 123}
        k2 (->key 4 5 6)
        r2 {:foo 456}
        tablet (tablet/from-records {k1 r1, k2 r2})]
    (is (nil? (tablet/remove-batch tablet/empty-tablet #{})))
    (is (= [[k1 r1] [k2 r2]]
           (tablet/read
             (tablet/remove-batch tablet nil))))
    (is (= [[k2 r2]]
           (tablet/read
             (tablet/remove-batch tablet #{k1}))))
    (is (= [[k1 r1]]
           (tablet/read
             (tablet/remove-batch tablet #{k2}))))
    (is (nil? (tablet/remove-batch tablet #{k1 k2})))))
