(ns merkle-db.record-test
  (:require
    [clojure.spec :as s]
    [clojure.test :refer :all]
    [merkle-db.key :as key]
    [merkle-db.record :as record]
    [merkle-db.test-utils]))


(deftest record-specs
  (testing "id fields"
    (is (invalid? ::record/id-field nil))
    (is (invalid? ::record/id-field #{:a}))
    (is (invalid? ::record/id-field []))
    (is (valid? ::record/id-field :id))
    (is (valid? ::record/id-field [:a]))
    (is (valid? ::record/id-field [:a :b :c]))
    (is (invalid? ::record/id-field [:a :a])
        "no duplicates")
    (is (invalid? ::record/id-field [#{:foo} :bar nil])
        "elements must be valid field keys"))
  (testing "record data"
    (is (invalid? ::record/data nil))
    (is (invalid? ::record/data []))
    (is (valid? ::record/data {}))
    (is (valid? ::record/data {:foo true, :bar "---", :baz 123})))
  (testing "family maps"
    (is (invalid? ::record/families nil))
    (is (invalid? ::record/families []))
    (is (valid? ::record/families {}))
    (is (valid? ::record/families {:bc #{:b :c}}))
    (is (valid? ::record/families {:bc #{:b :c}, :ad #{:a :d}}))
    (is (invalid? ::record/families {"foo" #{:a}}))
    (is (invalid? ::record/families {:qualified/family #{:a}}))
    (is (invalid? ::record/families {:not-a-set [:a]}))
    (is (invalid? ::record/families {:bc #{:b :c}, :cd #{:c :d}}))))


(deftest entry-encoding
  (testing "id projection"
    (is (= 123 (record/project-id nil {::record/id 123}))
        "field defaults to ::record/id")
    (is (= "xyz" (record/project-id :id {:id "xyz"})))
    (is (= [123 "abc"] (record/project-id [:a :b] {:a 123, :b "abc"}))))
  (testing "encoding"
    (is (= [(key/parse "807B")
            {:a "abc", :b true}]
           (record/encode-entry
             key/integer-lexicoder
             nil
             {::record/id 123
              :a "abc"
              :b true})))
    (is (= [(key/parse "807B00810102C8")
            {:z :foo}]
           (record/encode-entry
             (key/tuple-lexicoder
               key/integer-lexicoder
               key/integer-lexicoder)
             [:x :y]
             {:x 123
              :y 456
              :z :foo}))))
  (testing "decoding"
    (is (= {::record/id 123
            :a "abc"
            :b true}
           (record/decode-entry
             key/integer-lexicoder
             nil
             [(key/parse "807B")
              {:a "abc", :b true}])))
    (is (= {:x 123
            :y 456
            :z :foo}
           (record/decode-entry
             (key/tuple-lexicoder
               key/integer-lexicoder
               key/integer-lexicoder)
             [:x :y]
             [(key/parse "807B00810102C8")
              {:z :foo}])))))


(deftest family-grouping
  (let [k0 (key/create [0])
        r0 {}
        k1 (key/create [1])
        r1 {:a 5, :x true}
        k2 (key/create [2])
        r2 {:b 7, :y false}]
    (is (= {} (record/split-data {} [])))
    (is (= {:base [[k0 {}]]}
           (record/split-data {} [[k0 {}]])))
    (is (= {:base [[k0 {}]
                   [k1 {:x true}]
                   [k2 {:y false}]]
            :ab   [[k0 nil]
                   [k1 {:a 5}]
                   [k2 {:b 7}]]}
           (record/split-data {:ab #{:a :b}} [[k0 r0] [k1 r1] [k2 r2]])))))
