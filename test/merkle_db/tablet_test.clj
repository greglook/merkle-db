(ns merkle-db.tablet-test
  (:require
    [clojure.spec :as s]
    [clojure.test :refer :all]
    [merkle-db.key :as key]
    [merkle-db.tablet :as tablet]))


(def empty-tablet
  (tablet/from-records []))


(deftest tablet-predicate
  (is (not (tablet/tablet? "foo")))
  (is (not (tablet/tablet? [123 456])))
  (is (not (tablet/tablet? {})))
  (is (not (tablet/tablet? {:data/type :foo/bar})))
  (is (tablet/tablet? {:data/type tablet/data-type})))


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
  (let [k0 (key/create [0])
        k1 (key/create [1])
        k2 (key/create [2])
        r0 {:foo 123}
        r1 {:foo 456}
        r2 {:foo 789}
        tablet (tablet/from-records {k0 r0, k1 r1, k2 r2})]
    (is (empty? (tablet/read-range empty-tablet nil nil)))
    (is (= [[k0 r0] [k1 r1] [k2 r2]]
           (tablet/read-range tablet nil nil)))
    (is (= [[k0 r0] [k1 r1]]
           (tablet/read-range tablet nil k1)))
    (is (= [[k2 r2]]
           (tablet/read-range tablet k2 nil)))
    (is (= [[k1 r1]]
           (tablet/read-range tablet (key/create [0 0]) (key/create [1 5]))))
    (is (empty? (tablet/read-range tablet (key/create [3]) (key/create [7]))))))


(deftest tablet-utilities
  (let [k1 (key/create [1 2 3])
        r1 {:foo 123}
        k2 (key/create [2 3 4])
        r2 {:bar "baz"}
        tablet (tablet/from-records {k1 r1, k2 r2})]
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
      (is (= [[k1 r1] [k2 r2]] (tablet/read-all tablet))))
    (is (identical? t1 (tablet/join t1 nil)))
    (is (identical? t2 (tablet/join nil t2)))))


(deftest record-pruning
  (let [k1 (key/create [1 2 3])
        r1 {:foo 123}
        k2 (key/create [4 5 6])
        r2 {:foo 456}
        k3 (key/create [7 8 9])
        r3 {:foo 789}
        tablet (tablet/from-records {k1 r1, k2 r2, k3 r3})]
    (is (= [[k1 r1]]
           (tablet/read-all
             (tablet/prune
               (tablet/from-records {k1 r1, k2 {}})))))))
