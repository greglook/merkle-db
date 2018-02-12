(ns merkle-db.patch-test
  (:require
    [clojure.spec.alpha :as s]
    [clojure.test :refer :all]
    [merkle-db.key :as key]
    [merkle-db.patch :as patch]
    [merkle-db.test-utils]))


(deftest patch-spec
  (is (invalid? ::patch/node-data nil))
  (is (invalid? ::patch/node-data []))
  (is (invalid? ::patch/node-data
        {:data/type :foo}))
  (is (invalid? ::patch/node-data
        {:data/type :merkle-db/patch}))
  (is (invalid? ::patch/node-data
        {:data/type :merkle-db/patch
         ::patch/changes "wat"}))
  (is (valid? ::patch/node-data
        (patch/from-changes [[(key/create [0]) {:a 5}]])))
  (is (valid? ::patch/node-data
        (patch/from-changes [[(key/create [1]) ::patch/tombstone]]))))


(deftest tombstone-removal
  (is (= [[:a 1] [:c 3] [:e 5]]
         (patch/remove-tombstones
           [[:a 1]
            [:b ::patch/tombstone]
            [:c 3]
            [:d ::patch/tombstone]
            [:e 5]]))))


(deftest sequence-patching
  (let [changes [[(key/create [0 0]) {:a 10, :b 11, :c 12}]
                 [(key/create [1 0]) ::patch/tombstone]
                 [(key/create [1 1]) {:b 30, :c 32}]
                 [(key/create [2 0]) ::patch/tombstone]
                 [(key/create [3 0]) {:a 50, :b 51}]]
        records [[(key/create [0 5]) {:b 22, :c 31}]
                 [(key/create [1 0]) {:a 25, :b 42}]
                 [(key/create [1 1]) {:b 30, :c 30}]
                 [(key/create [2 5]) {}]]]
    (is (= [[(key/create [0 0]) {:a 10, :b 11, :c 12}]
            [(key/create [0 5]) {:b 22, :c 31}]
            [(key/create [1 1]) {:b 30, :c 32}]
            [(key/create [2 5]) {}]
            [(key/create [3 0]) {:a 50, :b 51}]]
           (patch/patch-seq changes records)))
    (is (= [[(key/create [0 0]) {:a 10, :b 11, :c 12}]
            [(key/create [0 5]) {:b 22, :c 31}]
            [(key/create [1 1]) {:b 30, :c 32}]
            [(key/create [2 5]) {}]
            [(key/create [3 0]) {:a 50, :b 51}]
            [(key/create [3 1]) {}]]
           (patch/patch-seq changes (conj records [(key/create [3 1]) {}]))))))
