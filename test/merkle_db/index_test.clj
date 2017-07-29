(ns merkle-db.index-test
  (:require
    [clojure.set :as set]
    [clojure.spec :as s]
    [clojure.string :as str]
    [clojure.test :refer :all]
    [clojure.test.check.generators :as gen]
    [com.gfredericks.test.chuck.clojure-test :refer [checking]]
    [merkledag.core :as mdag]
    [merkledag.link :as link]
    [merkledag.node :as node]
    (merkle-db
      [generators :as mdgen]
      [index :as index]
      [key :as key]
      [partition :as part]
      [record :as record]
      [validate :as validate])))


(defmacro check-asserts
  [& body]
  `(doseq [result# (validate/collect-results ~@body)]
     (do-report
       {:type (::validate/state result#)
        :message (::validate/message result#)
        :expected [(::validate/type result#)
                   (str/join \/ (::validate/path result#))
                   (::node/id result#)]
        :actual (::validate/state result#)})))


(deftest tree-validation
  (let [store (mdag/init-store :types record/codec-types)]
    (testing "empty tree"
      (check-asserts
        (index/validate-tree store {::record/count 0} nil)))
    (testing "partition tree"
      (let [[part] (part/from-records
                     store
                     {::record/families {:ab #{:a :b}, :cd #{:c :d}}}
                     {(key/create [0 1 2]) {:x 0, :y 0, :a 0, :c 0}
                      (key/create [1 2 3]) {:x 1, :c 1, :d 1, }
                      (key/create [2 3 4]) {:b 2, :c 2}
                      (key/create [3 4 5]) {:x 3, :y 3, :a 3, :b 3}
                      (key/create [4 5 6]) {:z 4, :d 4}})]
        (check-asserts
          (index/validate-tree store
                               {::record/count 5
                                ::part/limit 10}
                               part))))))


(deftest ^:generative index-construction
  (checking "valid properties" 20
    [[field-keys families records] mdgen/data-context
     part-limit (gen/large-integer* {:min 4})
     branch-fac (gen/large-integer* {:min 4})]
    (is (valid? ::record/families families))
    (let [store (mdag/init-store :types record/codec-types)
          params {::record/families families
                  ::record/count (count records)
                  ::index/branching-factor branch-fac
                  ::part/limit part-limit}
          parts (part/from-records store params records)
          root (index/build-index store params parts)]
      (check-asserts
        (index/validate-tree store params root)))))
