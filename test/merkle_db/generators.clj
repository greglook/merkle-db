(ns merkle-db.generators
  (:require
    [clojure.test.check.generators :as gen]
    [com.gfredericks.test.chuck.clojure-test :refer [checking]]
    [merkle-db.key :as key]))


(def record-key
  "Generates record key byte values."
  (gen/fmap key/create (gen/such-that not-empty gen/bytes)))


(def field-key
  "Generates field key values."
  (gen/one-of
    [(gen/such-that not-empty gen/string)
     gen/keyword
     gen/keyword-ns
     gen/symbol
     gen/symbol-ns]))


(def ^:private primitive-field-gens
  "Generates primitive field values."
  [gen/boolean
   gen/large-integer
   (gen/double* {:NaN? false})
   gen/string
   gen/keyword
   gen/keyword-ns
   gen/symbol
   gen/symbol-ns
   gen/uuid])


(def field-value
  "Generates primitive and complex field values."
  (let [primitive-value (gen/one-of primitive-field-gens)]
    (gen/one-of
      [primitive-value
       (gen/set primitive-value)
       (gen/vector primitive-value)
       (gen/map primitive-value primitive-value)])))


(defn record
  "Generates record key/data pairs using the given set of fields."
  [field-keys]
  (gen/tuple record-key (gen/map (gen/elements field-keys) field-value)))
