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


(deftest key-reads
  (let [k1 (key/create [1 2 3])
        r1 {:foo 123}
        k2 (key/create [4 5 6])
        r2 {:foo 456}
        tablet (tablet/from-records tablet/merge-fields {k1 r1, k2 r2})]
    (is (= k2 (tablet/nth-key tablet 1)))
    (is (thrown? Exception
          (tablet/nth-key tablet -1)))
    (is (thrown? Exception
          (tablet/nth-key tablet 3)))
    (is (= [k1 k2] (tablet/keys tablet)))
    (is (= [[k1 r1]]
           (tablet/read-batch tablet #{k1})))
    (is (= [[k1 r1] [k2 r2]]
           (tablet/read-batch tablet #{k1 k2})))))


(deftest batch-reads
  (let [k1 (key/create [1 2 3])
        r1 {:foo 123}
        k2 (key/create [4 5 6])
        r2 {:foo 456}
        tablet (tablet/from-records tablet/merge-fields {k1 r1, k2 r2})]
    (is (empty? (tablet/read-batch tablet/empty-tablet nil)))
    (is (= [[k1 r1]]
           (tablet/read-batch tablet #{k1})))
    (is (= [[k1 r1] [k2 r2]]
           (tablet/read-batch tablet #{k1 k2})))))


(deftest range-reads
  (let [k1 (key/create [1 2 3])
        r1 {:foo 123}
        k2 (key/create [4 5 6])
        r2 {:foo 456}
        tablet (tablet/from-records {k1 r1, k2 r2})]
    (is (empty? (tablet/read-range tablet/empty-tablet nil nil)))
    (is (= [[k1 r1] [k2 r2]]
           (tablet/read-range tablet nil nil)))
    (is (= [[k1 r1]]
           (tablet/read-range tablet nil (key/create [2]))))
    (is (= [[k2 r2]]
           (tablet/read-range tablet (key/create [2]) nil)))
    (is (empty?
          (tablet/read-range tablet (key/create [2]) (key/create [3]))))))


(deftest record-addition
  (let [k1 (key/create [1 2 3])
        r1 {:foo 123}
        k2 (key/create [4 5 6])
        r2 {:foo 456}
        tablet (tablet/from-records {k1 r1})]
    (is (= [[k1 {:foo 124}]]
           (tablet/read-all
             (tablet/update-records
               tablet
               tablet/merge-fields
               {k1 {:foo 124}}))))
    (is (= [[k1 {:foo 123, :bar true}]]
           (tablet/read-all
             (tablet/update-records
               tablet
               tablet/merge-fields
               {k1 {:bar true}}))))
    (is (= [[k1 r1] [k2 r2]]
           (tablet/read-all
             (tablet/update-records
               tablet
               tablet/merge-fields
               {k2 r2}))))))


(deftest record-removal
  (let [k1 (key/create [1 2 3])
        r1 {:foo 123}
        k2 (key/create [4 5 6])
        r2 {:foo 456}
        tablet (tablet/from-records {k1 r1, k2 r2})]
    (is (nil? (tablet/remove-batch tablet/empty-tablet #{})))
    (is (= [[k1 r1] [k2 r2]]
           (tablet/read-all
             (tablet/remove-batch tablet nil))))
    (is (= [[k2 r2]]
           (tablet/read-all
             (tablet/remove-batch tablet #{k1}))))
    (is (= [[k1 r1]]
           (tablet/read-all
             (tablet/remove-batch tablet #{k2}))))
    (is (nil? (tablet/remove-batch tablet #{k1 k2})))
    (is (= [[k1 r1]]
           (tablet/read-all
             (tablet/prune-records
               (tablet/from-records {k1 r1, k2 {}})))))))


(deftest tablet-utilities
  (let [k1 (key/create [1 2 3])
        r1 {:foo 123}
        k2 (key/create [2 3 4])
        r2 {:bar "baz"}
        tablet (tablet/from-records {k1 r1, k2 r2})]
    (is (= #{:foo :bar} (tablet/fields-present tablet)))
    (is (= k1 (tablet/first-key tablet)))
    (is (= k2 (tablet/last-key tablet)))))


(deftest tablet-splitting
  (let [k1 (key/create [1 2 3])
        r1 {:foo 123}
        k2 (key/create [2 3 4])
        r2 {:bar "baz"}
        tablet (tablet/from-records {k1 r1, k2 r2})]
    (is (thrown? Exception
          (tablet/split tablet (key/create [1 2 0]))))
    (is (thrown? Exception
          (tablet/split tablet (key/create [2 4 0]))))
    (let [[t1 t2] (tablet/split tablet (key/create [2 0]))]
      (is (= [[k1 r1]] (tablet/read-all t1)))
      (is (= [[k2 r2]] (tablet/read-all t2))))))


(deftest tablet-joining
  (let [k1 (key/create [1 2 3])
        r1 {:foo 123}
        t1 (tablet/from-records {k1 r1})
        k2 (key/create [2 3 4])
        r2 {:bar "baz"}
        t2 (tablet/from-records {k2 r2})]
    (is (thrown? Exception
          (tablet/join t2 t1)))
    (let [tablet (tablet/join t1 t2)]
      (is (= [[k1 r1] [k2 r2]] (tablet/read-all tablet))))))


; TODO: property tests
; - tablet data matches schema
; - tablet records are sorted by key
