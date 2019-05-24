(ns merkle-db.bloom
  "Bloom filters provide a probablistic way to test for membership in a set
  using a fixed amount of space. This is useful for reducing work when the test
  definitively rules out the existence of a record."
  (:refer-clojure :exclude [contains? into merge])
  (:require
    [bigml.sketchy.bits :as bits]
    [bigml.sketchy.bloom :as bloom]
    [bigml.sketchy.murmur :as murmur]))


;; The filter type wraps the bigml.sketchy bloom-filter implementation and
;; provides some extra features:
;;
;; - Strict typing to discriminate from regular maps.
;; - Better printing than the map representation.
;; - Filters can be invoked on elements to test membership, similar to regular
;;   set types.
(deftype BloomFilter
  [bins bits k _meta]

  java.lang.Object

  (toString
    [this]
    (format "#{2^%d bits, %d hashes}" bits k))

  (equals
    [this that]
    (boolean
      (or (identical? this that)
          (when (identical? (class this) (class that))
            (let [that ^BloomFilter that]
              (and (= bins (.bins that))
                   (= bits (.bits that))
                   (= k    (.k that))))))))

  (hashCode
    [this]
    (hash [(class this) bins bits k]))


  clojure.lang.IObj

  (meta [this] _meta)

  (withMeta
    [this meta-map]
    (BloomFilter. bins bits k meta-map))


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
  [bf w]
  (print-method
    (tagged-literal 'merkle-db/bloom-filter [(:bits bf) (:k bf)])
    w))


(defn filter?
  "Predicate which tests whether the value is a BloomFilter."
  [x]
  (instance? BloomFilter x))



;; ## Constructors

(defn create
  "Constructs a new bloom filter with the given expected population size and
  desired error (false positive) rate."
  ([expected-size]
   (create expected-size 0.01))
  ([expected-size error-rate]
   (let [{:keys [bins bits k]} (bloom/create expected-size error-rate)]
     (->BloomFilter bins bits k nil))))


(defn filter->form
  "Build a serializable form for the bloom filter."
  [^BloomFilter bf]
  (with-meta [(.k bf) (.bits bf) (.bins bf)] (._meta bf)))


(defn form->filter
  "Build a bloom filter from a serialized form."
  [form]
  (let [[k bits bins] form]
    (->BloomFilter bins bits k (meta form))))



;; ## Membership

(defn- insert*
  "Insert a single element into a bloom filter. Returns the updated filter."
  [bf x]
  (let [bits (:bits bf)
        k (:k bf)]
    (->BloomFilter
      (apply bits/set (:bins bf) (take k (murmur/hash-seq x bits)))
      bits k (meta bf))))


(defn insert
  "Insert the given elements into a bloom filter. Returns the updated filter."
  [bf & xs]
  (reduce insert* bf xs))


(defn into
  "Insert the collection of elements into a bloom filter. Returns the updated
  filter."
  [bf coll]
  (reduce insert* bf coll))


(defn contains?
  "True if the filter probably contains the given value."
  [bf x]
  (bloom/contains? bf x))


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
