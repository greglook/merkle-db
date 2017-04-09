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
    (is (zero? (key/compare (key/create []) (key/create []))))
    (is (zero? (key/compare (key/create [1 2 3]) (key/create [1 2 3]))))
    (is (false? (key/before? (key/create [1]) (key/create [1]))))
    (is (false? (key/after? (key/create [1]) (key/create [1])))))
  (testing "equal prefixes"
    (is (neg? (key/compare (key/create [1 2 3])
                           (key/create [1 2 3 4]))))
    (is (pos? (key/compare (key/create [1 2 3 4])
                           (key/create [1 2 3])))))
  (testing "order-before"
    (is (neg? (key/compare (key/create [1 2 3])
                           (key/create [1 2 4]))))
    (is (neg? (key/compare (key/create [1 2 3])
                           (key/create [1 3 2]))))
    (is (neg? (key/compare (key/create [0 2 3 4])
                           (key/create [1 3 2 1]))))
    (is (true? (key/before? (key/create [0 1])
                            (key/create [0 2])))))
  (testing "order-after"
    (is (pos? (key/compare (key/create [1 2 4])
                           (key/create [1 2 3]))))
    (is (pos? (key/compare (key/create [1 3 2])
                           (key/create [1 2 3]))))
    (is (pos? (key/compare (key/create [1 3 2 1])
                           (key/create [0 2 3 4]))))
    (is (true? (key/after? (key/create [0 1 2])
                           (key/create [0 1 0]))))))


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
  [generator]
  (checking "reflexive coding" 100
    [[coder arg-gen] generator
     x arg-gen]
    (is (= x (key/decode coder (key/encode coder x)))))
  (checking "sort order" 200
    [[coder arg-gen] generator
     a arg-gen
     b arg-gen]
    (let [ka (key/encode coder a)
          kb (key/encode coder b)]
      (cond
        (zero? (compare a b))
          (is (zero? (key/compare ka kb)))
        (pos? (compare a b))
          (is (pos? (key/compare ka kb)))
        :else
          (is (neg? (key/compare ka kb)))))))


(def lexicoder-generators
  "Map of lexicoder types to generators that return a lexicoder instance and
  a generator for values matching that lexicoder."
  {:string
   (gen/return
     [key/string-lexicoder
      (gen/such-that not-empty gen/string)])

   :long
   (gen/return
     [key/long-lexicoder
      gen/large-integer])

   :double
   (gen/return
     [key/double-lexicoder
      (gen/double* {:NaN? false})])

   :instant
   (gen/return
     [key/instant-lexicoder
      (gen/fmap #(java.time.Instant/ofEpochMilli %) gen/large-integer)])})


(deftest string-lexicoder
  (check-lexicoder (:string lexicoder-generators)))


(deftest long-lexicoder
  (is (thrown? Exception (key/decode key/long-lexicoder (byte-array 7)))
      "should require 8 bytes")
  (check-lexicoder (:long lexicoder-generators)))


(deftest double-lexicoder
  (check-lexicoder (:double lexicoder-generators)))


(deftest instant-lexicoder
  (check-lexicoder (:instant lexicoder-generators)))


(deftest sequence-lexicoder
  (check-lexicoder
    (gen/fmap
      (fn [[[coder arg-gen] n]]
        [(key/sequence-lexicoder coder)
         (gen/vector arg-gen n)])
      (gen/tuple
        (gen/one-of (vals lexicoder-generators))
        gen/nat))))
