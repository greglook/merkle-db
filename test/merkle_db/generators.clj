(ns merkle-db.generators
  (:require
    [clojure.spec.alpha :as s]
    [clojure.test :as test]
    [clojure.test.check.generators :as gen]
    [com.gfredericks.test.chuck.generators :as tcgen]
    [merkle-db.key :as key]
    [merkle-db.record :as record]))


(def record-key
  "Generates record key byte values."
  (gen/fmap key/create (gen/such-that not-empty gen/bytes)))


(def field-key
  "Generates field key values."
  (gen/one-of
    [(gen/such-that not-empty gen/string-ascii)
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


(defn record-data
  [field-keys]
  (gen/map (gen/elements field-keys) field-value))


(defn record-entry
  "Generates record key/data pairs using the given set of fields."
  [field-keys]
  (gen/tuple record-key (record-data field-keys)))


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


(defn data-context*
  "Generator for context data and configuration with a specific number of records."
  [field-keys n]
  (gen/tuple
    (gen/return field-keys)
    (families field-keys)
    (gen/fmap
      (partial apply zipmap)
      (gen/tuple
        (gen/set record-key {:num-elements n})
        (gen/vector (record-data field-keys) n)))))


(def data-context
  "Generator for context data and configuration. Generates tuples containing
  the set of field keys, the field families, and a vector of record key/data
  pairs."
  (gen/bind
    (gen/tuple
      (gen/not-empty (gen/set field-key))
      (gen/such-that pos? gen/nat))
    (partial apply data-context*)))
