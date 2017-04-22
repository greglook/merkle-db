(ns merkle-db.bloom
  "Wrapper code for implementing a bloom filter for membership testing."
  (:refer-clojure :exclude [merge])
  (:require
    [bigml.sketchy.bits :as bits]
    [bigml.sketchy.bloom :as bloom]
    [bigml.sketchy.murmur :as murmur]
    [clojure.spec :as s]))


(deftype BloomFilter
  [bins bits k _meta]

  java.lang.Object

  (toString
    [this]
    (format "BloomFilter[%d:%d]" bits k))

  (equals
    [this that]
    (boolean
      (or (identical? this that)
          (when (identical? (class this) (class that))
            (let [that ^BloomFilter that]
              (= bins (.bins that))
              (= bits (.bits that))
              (= k    (.k that)))))))

  (hashCode
    [this]
    (hash [(class this) bins bits k]))


  clojure.lang.IObj

  (meta [this] _meta)

  (withMeta
    [this meta-map]
    (BloomFilter. bins bits k meta-map))


  clojure.lang.IPersistentCollection

  (count
    [this]
    (throw (RuntimeException.
             "Bloom filters are not countable")))

  (cons
    [this x]
    (BloomFilter.
      (apply bits/set bins (take k (murmur/hash-seq x bits)))
      bits k _meta))

  (empty
    [this]
    (BloomFilter.
      (bits/create (bit-shift-left 1 bits))
      bits k _meta))

  (equiv
    [this that]
    (.equals this that))


  clojure.lang.ILookup

  (valAt
    [this key]
    (.valAt this key nil))

  (valAt
    [this key not-found]
    (case key
      :bins bins
      :bits bits
      :k k
      not-found))


  clojure.lang.IFn

  (invoke
    [this x]
    (bloom/contains? this x)))


(alter-meta! #'->BloomFilter assoc :private true)


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
   (let [{:keys [bins bits k]} (bloom/create expected-size error-rate)]
     (->BloomFilter bins bits k nil))))


(defn filter?
  "Predicate which tests whether the value is a BloomFilter."
  [x]
  (instance? BloomFilter x))


(defn mergeable?
  "Returns true if the two bloom filters can be merged together."
  [a b]
  (and (= (:bits a) (:bits b))
       (= (:k a) (:k b))))


(defn merge
  "Merge the bloom filters together. Only works if `bits` and `k` match."
  [a b]
  (when-not (mergeable? a b)
    (throw (IllegalArgumentException.
             (format "Bloom filters with different sizes cannot be merged: %s != %s" a b))))
  (->BloomFilter
    (bits/or (:bins a) (:bins b))
    (:bits a)
    (:k a)
    (clojure.core/merge (meta a) (meta b))))
