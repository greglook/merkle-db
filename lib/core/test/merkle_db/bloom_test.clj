(ns merkle-db.bloom-test
  (:require
    [clojure.test :refer :all]
    [merkle-db.bloom :as bloom]))


(deftest basic-ops
  (let [bf (bloom/create 1000)]
    (is (= bf bf))
    (is (not= bf (bloom/create 1500)))
    (is (string? (pr-str bf)))
    (is (bloom/filter? bf))
    (is (false? (bf :x)))
    (is (false? (bf :y)))
    (is (true? ((bloom/insert bf :x) :x)))
    (is (false? (bloom/contains? (bloom/insert bf :x) :y)))
    (is (false? (bf :x)))))


(deftest merging
  (let [a (bloom/insert (bloom/create 1000) :x :y)
        b (bloom/insert (bloom/create 1000) 1 2)
        ab (bloom/merge a b)]
    (is (bloom/filter? ab))
    (is (true? (ab :x)))
    (is (true? (ab :y)))
    (is (true? (ab 1)))
    (is (true? (ab 2)))
    (is (false? (ab :z)))
    (is (false? (ab 3))))
  (is (thrown? IllegalArgumentException
        (bloom/merge (bloom/create 1000) (bloom/create 10000)))))


(deftest forming
  (let [bf (bloom/into (bloom/create 1000) (range 100))
        form (bloom/filter->form bf)]
    (is (vector? form))
    (is (= 3 (count form)))
    (is (= bf (bloom/form->filter form)))))
