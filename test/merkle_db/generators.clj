(ns merkle-db.generators
  (:require
    [clojure.spec :as s]
    [clojure.test :as test]
    [clojure.test.check.generators :as gen]
    [com.gfredericks.test.chuck.generators :as tcgen]
    [merkle-db.record :as record]
    [merkle-db.key :as key]))


;; (is (valid? s expr))
;; Asserts that the result of `expr` is valid for spec `s`.
;; Returns the conformed value.
(defmethod test/assert-expr 'valid?
  [msg form]
  `(let [spec# ~(second form)
         value# ~(nth form 2)
         conformed# (s/conform spec# value#)]
     (if (= ::s/invalid conformed#)
       (test/do-report
         {:type :fail
          :message ~msg,
          :expected '~(second form)
          :actual (s/explain-data spec# value#)})
       (test/do-report
         {:type :pass
          :message ~msg,
          :expected '~(second form)
          :actual conformed#}))
     conformed#))


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


(defn families
  "Generates a map of family keys to distinct sets of fields drawn from the
  ones given."
  [field-keys]
  (-> (tcgen/partition field-keys)
      (gen/bind
        (fn [field-sets]
          (gen/tuple
            (gen/vector (s/gen ::record/family-key)
                        (count field-sets))
            (gen/return (map set field-sets)))))
      (->> (gen/fmap (partial apply zipmap)))
      (gen/bind tcgen/sub-map)))


(def data-context
  "Generator for context data and configuration. Generates tuples containing
  the set of field keys, the field families, and a vector of record key/data
  pairs."
  (gen/bind
    (gen/not-empty (gen/set field-key))
    (fn [field-keys]
      (gen/tuple
        (gen/return field-keys)
        (families field-keys)
        (gen/not-empty (gen/vector (record field-keys)))))))
