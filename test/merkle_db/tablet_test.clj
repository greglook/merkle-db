(ns merkle-db.tablet-test
  (:require
    [clojure.spec :as s]
    [clojure.test :refer :all]
    [merkle-db.key :as key]
    [merkle-db.tablet :as tablet]))


(def empty-tablet
  (tablet/from-records []))


(deftest key-reads
  (let [k1 (key/create [1 2 3])
        r1 {:foo 123}
        k2 (key/create [4 5 6])
        r2 {:foo 456}
        tablet (tablet/from-records {k1 r1, k2 r2})]
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
        tablet (tablet/from-records {k1 r1, k2 r2})]
    (is (empty? (tablet/read-batch empty-tablet nil)))
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
    (is (empty? (tablet/read-range empty-tablet nil nil)))
    (is (= [[k1 r1] [k2 r2]]
           (tablet/read-range tablet nil nil)))
    (is (= [[k1 r1]]
           (tablet/read-range tablet nil (key/create [2]))))
    (is (= [[k2 r2]]
           (tablet/read-range tablet (key/create [2]) nil)))
    (is (empty?
          (tablet/read-range tablet (key/create [2]) (key/create [3]))))))


(deftest tablet-utilities
  (let [k1 (key/create [1 2 3])
        r1 {:foo 123}
        k2 (key/create [2 3 4])
        r2 {:bar "baz"}
        tablet (tablet/from-records {k1 r1, k2 r2})]
    (is (= #{:foo :bar} (tablet/fields-present tablet)))
    (is (= k1 (tablet/first-key tablet)))
    (is (= k2 (tablet/last-key tablet)))))


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
