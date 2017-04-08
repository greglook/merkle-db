(ns merkle-db.bloom
  "Wrapper code for implementing a bloom filter for membership testing."
  (:refer-clojure :exclude [contains?])
  (:require
    [bigml.sketchy.bloom :as bloom]
    [clojure.spec :as s]))


(defrecord BloomFilter
  [bins bits k])


(alter-meta! #'->BloomFilter assoc :private true)
(alter-meta! #'map->BloomFilter assoc :private true)


(defmethod print-method BloomFilter
  [x w]
  (print-method
    (tagged-literal
      'merkle-db.bloom.BloomFilter
      (select-keys x [:bits :k]))
    w))


(defn create
  "Constructs a new bloom filter with the given expected population size and
  desired error (false positive) rate."
  ([expected-size]
   (create expected-size 0.01))
  ([expected-size error-rate]
   (create expected-size error-rate)))


(defn filter?
  "Predicate which tests whether the value is a BloomFilter."
  [x]
  (instance? BloomFilter x))


(defn insert
  "Update the bloom filter by inserting the given value."
  [bf x]
  (bloom/insert bf x))


(defn contains?
  "Tests whether the bloom filter probably contains the given value."
  [bf x]
  (bloom/contains? bf x))
