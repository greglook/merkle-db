(ns merkle-db.key-test
  (:require
    [clojure.test :refer :all]
    [clojure.test.check.generators :as gen]
    [com.gfredericks.test.chuck.clojure-test :refer [checking]]
    [merkle-db.key :as key])
  (:import
    blocks.data.PersistentBytes))


(defmethod print-method PersistentBytes
  [x w]
  (print-method
    (tagged-literal
      'data/bytes
      (apply str (map (partial format "%02x") (seq x))))
    w))


(deftest key-predicate
  (is (false? (key/bytes? nil)))
  (is (false? (key/bytes? :foo)))
  (is (false? (key/bytes? (byte-array [0 1 2]))))
  (is (true? (key/bytes? (key/create [0]))))
  (is (true? (key/bytes? (key/create [0 1 2 3 4])))))


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


(deftest min-max-util
  (let [a (key/create [0])
        b (key/create [5])
        c (key/create [7])
        d (key/create [8])]
    (testing "minimum keys"
      (is (= a (key/min a)))
      (is (= b (key/min b c)))
      (is (= b (key/min c d b)))
      (is (= a (key/min c d a b))))
    (testing "maximum keys"
      (is (= a (key/max a)))
      (is (= c (key/max b c)))
      (is (= d (key/max c d b)))
      (is (= d (key/max c d a b))))))


(defn- check-lexicoder
  [coder generator]
  (checking "reflexive coding" 100
    [x generator]
    (is (= x (key/decode coder (key/encode coder x)))))
  (checking "sort order" 200
    [a generator
     b generator]
    (let [ka (key/encode coder a)
          kb (key/encode coder b)]
      (cond
        (zero? (compare a b))
          (is (zero? (key/compare ka kb)))
        (pos? (compare a b))
          (is (pos? (key/compare ka kb)))
        :else
          (is (neg? (key/compare ka kb)))))))


(deftest string-lexicoder
  (check-lexicoder (key/string-lexicoder) (gen/not-empty gen/string)))

(deftest long-lexicoder
  (check-lexicoder (key/long-lexicoder) (gen/fmap #(long %) (gen/large-integer* {:min Long/MIN_VALUE, :max Long/MAX_VALUE}))))
