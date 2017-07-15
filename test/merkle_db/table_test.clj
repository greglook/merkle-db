(ns merkle-db.table-test
  (:require
    [clojure.test :refer :all]
    [merkle-db.key :as key]
    [merkle-db.table :as table]))


(deftest record-updating-fn
  (let [record-updater @#'table/record-updater]
    (testing "default behavior"
      (let [merge-simple (record-updater {})]
        (is (= {:a "abc", :b 4}
               (merge-simple nil {:a "abc", :c true} {:b 4, :c nil})))))
    (testing "explicit merge-record"
      (let [merge-record (fn custom-merge [rk l r] r)]
        (is (identical? merge-record (record-updater {:merge-record merge-record}))
            "function should be used directly"))
      (is (thrown? IllegalArgumentException
            (record-updater {:merge-record "not a function"})
            "non-function should be illegal")))
    (testing "merge-field"
      (testing "with map of update functions"
        (let [merge-fields (record-updater {:merge-field {:a +, :c conj}})]
          (is (= {:a 3} (merge-fields nil {:a 1} {:a 2})))
          (is (= {:c #{"foo" "bar"}}
                 (merge-fields nil {:c #{"bar"}} {:c "foo"})))
          (is (= {:b 3} (merge-fields nil {:d 1} {:b 3, :d nil})))
          (is (= {:a 468, :b 4, :c ["abc"]}
                 (merge-fields nil
                               {:a 123, :b 1, :c []}
                               {:a 345, :b 4, :c "abc"})))))
      (testing "with custom function"
        (let [merge-fields (record-updater
                             {:merge-field (fn [fk l r]
                                             (case fk
                                               :a (+ l r)
                                               :c (conj l r)
                                               :d (when-not (= r "foo") r)
                                               (* r 2)))})]
          (is (= {:a 3} (merge-fields nil {:a 1} {:a 2})))
          (is (= {:d "bar"} (merge-fields nil {} {:d "bar"})))
          (is (= {:b 6} (merge-fields nil {:d "abc"} {:b 3, :d nil})))
          (is (= {:a 468, :b 8, :c ["abc"]}
                 (merge-fields nil
                               {:a 123, :b 1, :c []}
                               {:a 345, :b 4, :c "abc", :d "foo"})))))
      (is (thrown? IllegalArgumentException
            (record-updater {:merge-field "not a map or function"})
            "non-function should be illegal")))
    (testing "specifying both merge-record and merge-field"
      (is (thrown? IllegalArgumentException
            (record-updater {:merge-record (constantly nil)
                             :merge-field (constantly nil)}))
          "should be illegal"))))
