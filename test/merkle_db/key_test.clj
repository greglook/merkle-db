(ns merkle-db.key-test
  (:require
    [clojure.future :refer [bytes?]]
    [clojure.test :refer :all]
    [clojure.test.check.generators :as gen]
    [com.gfredericks.test.chuck.clojure-test :refer [checking]]
    [merkle-db.key :as key]))


(deftest printing
  (is (= "#merkle-db/key \"012345\"" (pr-str (key/create [0x01 0x23 0x45])))))


(deftest key-predicate
  (is (false? (key/key? nil)))
  (is (false? (key/key? :foo)))
  (is (false? (key/key? (byte-array [0 1 2]))))
  (is (true? (key/key? (key/create [0]))))
  (is (true? (key/key? (key/create [0 1 2 3 4])))))


(deftest lexicographic-ordering
  (testing "equal arrays"
    (is (zero? (compare (key/create []) (key/create []))))
    (is (zero? (compare (key/create [1 2 3]) (key/create [1 2 3]))))
    (is (false? (key/before? (key/create [1]) (key/create [1]))))
    (is (false? (key/after? (key/create [1]) (key/create [1])))))
  (testing "equal prefixes"
    (is (neg? (compare (key/create [1 2 3])
                       (key/create [1 2 3 4]))))
    (is (pos? (compare (key/create [1 2 3 4])
                       (key/create [1 2 3])))))
  (testing "order-before"
    (is (neg? (compare (key/create [1 2 3])
                       (key/create [1 2 4]))))
    (is (neg? (compare (key/create [1 2 3])
                       (key/create [1 3 2]))))
    (is (neg? (compare (key/create [0 2 3 4])
                       (key/create [1 3 2 1]))))
    (is (true? (key/before? (key/create [0 1])
                            (key/create [0 2])))))
  (testing "order-after"
    (is (pos? (compare (key/create [1 2 4])
                       (key/create [1 2 3]))))
    (is (pos? (compare (key/create [1 3 2])
                       (key/create [1 2 3]))))
    (is (pos? (compare (key/create [1 3 2 1])
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
     (let [decoded (key/decode coder (key/encode coder x))]
       (if (bytes? x)
         (is (zero? (cmp x decoded)))
         (is (= x decoded)))))
   (checking "sort order" 100
     [[coder arg-gen] generator
      a arg-gen
      b arg-gen]
     (let [rrank (cmp a b)
           ka (key/encode coder a)
           kb (key/encode coder b)]
       (cond
         (zero? rrank)
           (is (zero? (compare ka kb)))
         (pos? rrank)
           (is (pos? (compare ka kb)))
         :else
           (is (neg? (compare ka kb))))))))


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


(defmacro ^:private is-reflexive
  [coder value hex]
  `(let [coder# ~coder
         key# (key/parse ~hex)]
     (is (= key# (key/encode coder# ~value)))
     (is (= ~value (key/decode coder# key#)))))


(def lexicoder-generators
  "Map of lexicoder types to tuples of a lexicoder instance and a generator for
  values matching that lexicoder."
  {:string
   [key/string-lexicoder
    (gen/such-that not-empty gen/string)]

   :integer
   [key/integer-lexicoder
    gen/large-integer]

   :float
   [key/float-lexicoder
    (gen/double* {:NaN? false})]

   :instant
   [key/instant-lexicoder
    (gen/fmap #(java.time.Instant/ofEpochMilli %) gen/large-integer)]})


(deftest lexicoder-configs
  (is (thrown? Exception (key/lexicoder "not a keyword or vector")))
  (is (thrown? Exception (key/lexicoder [123 "needs a keyword first"]))))


(deftest bytes-lexicoder
  (testing "construction"
    (is (identical? key/bytes-lexicoder (key/lexicoder :bytes)))
    (is (satisfies? key/Lexicoder key/bytes-lexicoder))
    (is (= :bytes (key/lexicoder-config key/bytes-lexicoder)))
    (is (thrown? Exception
          (key/lexicoder [:bytes :foo]))
        "should not accept any config parameters"))
  (testing "bad values"
    (is (thrown? IllegalArgumentException
          (key/encode key/bytes-lexicoder nil))
        "should not encode nil")
    (is (thrown? IllegalArgumentException
          (key/encode key/bytes-lexicoder "foo"))
        "should not encode non-byte-arrays")
    (is (thrown? IllegalArgumentException
          (key/encode key/bytes-lexicoder (byte-array 0)))
        "should not encode empty bytes")
    (is (thrown? IllegalArgumentException
          (key/decode key/bytes-lexicoder (byte-array 0)))
        "should not decode empty bytes"))
  (testing "basic values"
    (is (= (key/parse "00")
           (key/encode key/bytes-lexicoder (byte-array [0]))))
    (is (= (key/parse "012345")
           (key/encode key/bytes-lexicoder (byte-array [1 35 69]))))))


(deftest ^:generative bytes-lexicoding
  (check-lexicoder
    (gen/return [key/bytes-lexicoder
                 (gen/such-that not-empty gen/bytes)])
    @#'key/compare-bytes))


(deftest string-lexicoder
  (testing "construction"
    (is (identical? key/string-lexicoder (key/lexicoder :string)))
    (is (satisfies? key/Lexicoder (key/lexicoder [:string "UTF-8"])))
    (is (= :string (key/lexicoder-config key/string-lexicoder)))
    (is (= [:string "US-ASCII"] (key/lexicoder-config (key/string-lexicoder* "US-ASCII"))))
    (is (thrown? Exception
          (key/lexicoder [:string "UTF-8" :bar]))
        "should only accept one config parameter"))
  (testing "bad values"
    (is (thrown? IllegalArgumentException
          (key/encode key/string-lexicoder nil))
        "should not encode nil")
    (is (thrown? IllegalArgumentException
          (key/encode key/string-lexicoder 123))
        "should not encode non-strings")
    (is (thrown? IllegalArgumentException
          (key/encode key/string-lexicoder ""))
        "should not encode empty strings")
    (is (thrown? IllegalArgumentException
          (key/decode key/string-lexicoder (byte-array 0)))
        "should not decode empty bytes"))
  (testing "basic values"
    (is-reflexive key/string-lexicoder "foo" "666F6F")
    (is-reflexive key/string-lexicoder "bar" "626172")))


(deftest ^:generative string-lexicoding
  (check-lexicoder
    (gen/return (:string lexicoder-generators))))


(deftest integer-lexicoder
  (testing "construction"
    (is (identical? key/integer-lexicoder (key/lexicoder :integer)))
    (is (= :integer (key/lexicoder-config key/integer-lexicoder)))
    (is (thrown? Exception
          (key/lexicoder [:integer :bar]))
        "should not accept any config parameters"))
  (testing "bad values"
    (is (thrown? IllegalArgumentException
          (key/encode key/integer-lexicoder nil))
        "should not encode nil")
    (is (thrown? IllegalArgumentException
          (key/encode key/integer-lexicoder 0.0))
        "should not encode non-integers")
    (is (thrown? IllegalArgumentException
          (key/decode key/integer-lexicoder (key/parse "80")))
        "should not encode too-short keys"))
  (testing "basic values"
    (is-reflexive key/integer-lexicoder  0 "8000")
    (is-reflexive key/integer-lexicoder -1 "7FFF")
    (is-reflexive key/integer-lexicoder -0x7FFFFFFFFF000000 "788000000001000000")))


(deftest ^:generative integer-lexicoding
  (check-lexicoder
    (gen/return (:integer lexicoder-generators))))


(deftest float-lexicoder
  (testing "construction"
    (is (identical? key/float-lexicoder (key/lexicoder :float)))
    (is (= :float (key/lexicoder-config key/float-lexicoder)))
    (is (thrown? Exception
          (key/lexicoder [:float :bar]))
        "should not accept any config parameters"))
  (testing "bad values"
    (is (thrown? IllegalArgumentException
          (key/encode key/float-lexicoder nil))
        "should not encode nil")
    (is (thrown? IllegalArgumentException
          (key/encode key/float-lexicoder 123))
        "should not encode non-floats")
    (is (thrown? IllegalArgumentException
          (key/encode key/float-lexicoder Double/NaN))
        "should not encode NaN values"))
  (testing "basic values"
    (is-reflexive key/float-lexicoder 0.0 "8000")
    (is-reflexive key/float-lexicoder 1.0 "873FF0000000000000")
    (is-reflexive key/float-lexicoder -1.0 "78C00FFFFFFFFFFFFF")))


(deftest ^:generative float-lexicoding
  (check-lexicoder
    (gen/return (:float lexicoder-generators))))


(deftest instant-lexicoder
  (testing "construction"
    (is (identical? key/instant-lexicoder (key/lexicoder :instant)))
    (is (= :instant (key/lexicoder-config key/instant-lexicoder)))
    (is (thrown? Exception
          (key/lexicoder [:instant :bar]))
        "should not accept any config parameters"))
  (testing "bad values"
    (is (thrown? IllegalArgumentException
          (key/encode key/instant-lexicoder nil))
        "should not encode nil")
    (is (thrown? IllegalArgumentException
          (key/encode key/instant-lexicoder ""))
        "should not encode non-instant value")))


(deftest ^:generative instant-lexicoding
  (check-lexicoder
    (gen/return (:instant lexicoder-generators))))


(deftest sequence-lexicoder
  (testing "construction"
    (is (satisfies? key/Lexicoder (key/lexicoder [:seq :integer])))
    (is (= [:seq :integer] (key/lexicoder-config (key/sequence-lexicoder key/integer-lexicoder))))
    (is (thrown? Exception
          (key/lexicoder :seq))
        "should require at least one config parameter")
    (is (thrown? Exception
          (key/lexicoder [:seq :string :foo]))
        "should only accept one config parameter"))
  (testing "bad values"
    (is (thrown? IllegalArgumentException
          (key/encode (key/sequence-lexicoder key/integer-lexicoder) nil))
        "should not encode nil")
    (is (thrown? IllegalArgumentException
          (key/encode (key/sequence-lexicoder key/integer-lexicoder) #{123}))
        "should not encode non-sequential values")))


(deftest ^:generative sequence-lexicoding
  (check-lexicoder
    (gen/fmap
      (fn [[coder arg-gen]]
        [(key/sequence-lexicoder coder)
         (gen/vector arg-gen)])
      (gen/elements (vals lexicoder-generators)))
    (lexi-comparator compare)))


(deftest tuple-lexicoder
  (testing "construction"
    (is (satisfies? key/Lexicoder (key/lexicoder [:tuple :string])))
    (is (= [:tuple :integer :string] (key/lexicoder-config (key/tuple-lexicoder
                                                             key/integer-lexicoder
                                                             key/string-lexicoder))))
    (is (thrown? Exception
          (key/lexicoder [:tuple]))
        "should require at least one config parameter"))
  (testing "bad values"
    (is (thrown? IllegalArgumentException
          (key/encode (key/tuple-lexicoder key/integer-lexicoder) nil))
        "should not encode nil")
    (is (thrown? IllegalArgumentException
          (key/encode (key/tuple-lexicoder key/integer-lexicoder) #{123}))
        "should not encode non-sequential values")
    (is (thrown? IllegalArgumentException
          (key/encode (key/tuple-lexicoder key/integer-lexicoder) [0 0]))
        "should not encode tuples larger than coders")
    (is (thrown? IllegalArgumentException
          (key/encode (key/lexicoder [:tuple :integer :string])
                      [0]))
        "should not encode tuples smaller than coders")
    (is (thrown? IllegalArgumentException
          (key/decode (key/tuple-lexicoder key/string-lexicoder)
                      (key/encode (key/tuple-lexicoder key/string-lexicoder
                                                       key/integer-lexicoder)
                                  ["foo" 123])))
        "should not decode tuple longer than coders")))


(deftest ^:generative tuple-lexicoding
  (check-lexicoder
    (gen/fmap
      (fn [generators]
        [(apply key/tuple-lexicoder (map first generators))
         (apply gen/tuple (map second generators))])
      (gen/not-empty (gen/vector (gen/elements (vals lexicoder-generators)))))))


(deftest reverse-lexicoder
  (testing "construction"
    (is (satisfies? key/Lexicoder (key/lexicoder [:reverse :instant])))
    (is (= [:reverse :bytes] (key/lexicoder-config (key/reverse-lexicoder key/bytes-lexicoder))))
    (is (thrown? Exception
          (key/lexicoder [:reverse])))
    (is (thrown? Exception
          (key/lexicoder [:reverse :integer :string])))))


(deftest ^:generative reverse-lexicoding
  (check-lexicoder
    (gen/fmap
      (fn [[coder arg-gen]]
        [(key/reverse-lexicoder coder) arg-gen])
      (gen/elements (vals lexicoder-generators)))
    (comp - compare)))
