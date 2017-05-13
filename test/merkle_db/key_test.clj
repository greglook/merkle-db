(ns merkle-db.key-test
  (:require
    [clojure.test :refer :all]
    [clojure.test.check.generators :as gen]
    [com.gfredericks.test.chuck.clojure-test :refer [checking]]
    [merkle-db.key :as key]
    [merkle-db.test-utils]))


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
  ([generator]
   (check-lexicoder generator compare))
  ([generator cmp]
   (checking "reflexive coding" 50
     [[coder arg-gen] generator
      x arg-gen]
     (is (= x (key/decode coder (key/encode coder x)))))
   (checking "sort order" 100
     [[coder arg-gen] generator
      a arg-gen
      b arg-gen]
     (let [rrank (cmp a b)
           ka (key/encode coder a)
           kb (key/encode coder b)]
       (cond
         (zero? rrank)
           (is (zero? (key/compare ka kb)))
         (pos? rrank)
           (is (pos? (key/compare ka kb)))
         :else
           (is (neg? (key/compare ka kb))))))))


(defn- lexi-comparator
  "Wrap a comparator for elements of type x to build a lexical comparator
  over sequences of xs."
  [cmp]
  (fn lex-cmp
    [as bs]
    (let [prefix-len (min (count as) (count bs))]
      (loop [as as, bs bs]
        (if (and (seq as) (seq bs))
          (let [rrank (cmp (first as) (first bs))]
            (if (zero? rrank)
              (recur (rest as) (rest bs))
              rrank))
          (- (count as) (count bs)))))))


(def lexicoder-generators
  "Map of lexicoder types to tuples of a lexicoder instance and a generator for
  values matching that lexicoder."
  {:string
   [key/string-lexicoder
    (gen/such-that not-empty gen/string)]

   :long
   [key/long-lexicoder
    gen/large-integer]

   :double
   [key/double-lexicoder
    (gen/double* {:NaN? false})]

   :instant
   [key/instant-lexicoder
    (gen/fmap #(java.time.Instant/ofEpochMilli %) gen/large-integer)]})


(deftest lexicoder-configs
  (is (thrown? Exception (key/lexicoder "not a keyword or vector")))
  (is (thrown? Exception (key/lexicoder [123 "needs a keyword first"]))))


(deftest string-lexicoder
  (is (identical? key/string-lexicoder (key/lexicoder :string)))
  (is (satisfies? key/Lexicoder (key/lexicoder [:string "UTF-8"])))
  (is (thrown? Exception
        (key/lexicoder [:string "UTF-8" :bar])))
  (is (thrown? IllegalArgumentException
        (key/encode key/string-lexicoder ""))
      "should not encode empty strings")
  (is (thrown? IllegalArgumentException
        (key/decode key/string-lexicoder (byte-array 0)))
      "should not decode empty bytes")
  (check-lexicoder (gen/return (:string lexicoder-generators))))


(deftest long-lexicoder
  (is (identical? key/long-lexicoder (key/lexicoder :long)))
  (is (thrown? Exception
        (key/lexicoder [:long :bar])))
  (is (thrown? IllegalArgumentException
        (key/decode key/long-lexicoder (byte-array 7)))
      "should require 8 bytes")
  (check-lexicoder (gen/return (:long lexicoder-generators))))


(deftest double-lexicoder
  (is (identical? key/double-lexicoder (key/lexicoder :double)))
  (is (thrown? Exception
        (key/lexicoder [:double :bar])))
  (check-lexicoder (gen/return (:double lexicoder-generators))))


(deftest instant-lexicoder
  (is (identical? key/instant-lexicoder (key/lexicoder :instant)))
  (is (thrown? Exception
        (key/lexicoder [:instant :bar])))
  (is (thrown? IllegalArgumentException
        (key/encode key/instant-lexicoder ""))
      "should not encode non-instant value")
  (check-lexicoder (gen/return (:instant lexicoder-generators))))


(deftest sequence-lexicoder
  (is (satisfies? key/Lexicoder (key/lexicoder [:seq :long])))
  (is (thrown? Exception
        (key/lexicoder :seq)))
  (is (thrown? Exception
        (key/lexicoder [:seq :string :foo])))
  (check-lexicoder
    (gen/fmap
      (fn [[coder arg-gen]]
        [(key/sequence-lexicoder coder)
         (gen/vector arg-gen)])
      (gen/elements (vals lexicoder-generators)))
    (lexi-comparator compare)))


(deftest tuple-lexicoder
  (is (satisfies? key/Lexicoder (key/lexicoder [:tuple :string])))
  (is (thrown? Exception
        (key/lexicoder [:tuple])))
  (is (thrown? IllegalArgumentException
        (key/encode (key/tuple-lexicoder key/long-lexicoder) [0 0]))
      "should not encode tuples larger than coders")
  (is (thrown? IllegalArgumentException
        (key/encode (key/lexicoder [:tuple :long :string])
                    [0]))
      "should not encode tuples smaller than coders")
  (is (thrown? IllegalArgumentException
        (key/decode (key/tuple-lexicoder key/string-lexicoder)
                    (key/encode (key/tuple-lexicoder key/string-lexicoder
                                                     key/long-lexicoder)
                                ["foo" 123])))
      "should not decode tuple longer than coders")
  (check-lexicoder
    (gen/fmap
      (fn [generators]
        [(apply key/tuple-lexicoder (map first generators))
         (apply gen/tuple (map second generators))])
      (gen/not-empty (gen/vector (gen/elements (vals lexicoder-generators)))))))


(deftest reverse-lexicoder
  (is (satisfies? key/Lexicoder (key/lexicoder [:reverse :instant])))
  (is (thrown? Exception
        (key/lexicoder [:reverse])))
  (is (thrown? Exception
        (key/lexicoder [:reverse :long :string])))
  (check-lexicoder
    (gen/fmap
      (fn [[coder arg-gen]]
        [(key/reverse-lexicoder coder) arg-gen])
      (gen/elements (vals lexicoder-generators)))
    (comp - compare)))
