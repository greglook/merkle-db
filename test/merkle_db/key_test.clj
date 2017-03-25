(ns merkle-db.key-test
  (:require
    [clojure.test :refer :all]
    [merkle-db.key :as key])
  (:import
    blocks.data.PersistentBytes))


(defn ->key
  "Construct a new `PersistentBytes` value containing the given byte data."
  [& data]
  (PersistentBytes/wrap (byte-array data)))


(deftest lexicographic-ordering
  (testing "equal arrays"
    (is (zero? (key/compare (byte-array 0) (byte-array 0))))
    (is (zero? (key/compare (byte-array [1 2 3]) (byte-array [1 2 3])))))
  (testing "equal prefixes"
    (is (neg? (key/compare (byte-array [1 2 3])
                           (byte-array [1 2 3 4]))))
    (is (pos? (key/compare (byte-array [1 2 3 4])
                           (byte-array [1 2 3])))))
  (testing "order-before"
    (is (neg? (key/compare (byte-array [1 2 3])
                           (byte-array [1 2 4]))))
    (is (neg? (key/compare (byte-array [1 2 3])
                           (byte-array [1 3 2]))))
    (is (neg? (key/compare (byte-array [0 2 3 4])
                           (byte-array [1 3 2 1]))))
  (testing "order-after"
    (is (pos? (key/compare (byte-array [1 2 4])
                           (byte-array [1 2 3]))))
    (is (pos? (key/compare (byte-array [1 3 2])
                           (byte-array [1 2 3]))))
    (is (pos? (key/compare (byte-array [1 3 2 1])
                           (byte-array [0 2 3 4])))))))
