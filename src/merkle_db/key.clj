(ns merkle-db.key
  "Record keys are immutable byte sequences which uniquely identify a record
  within a table. Keys are stored in sorted order by comparing them
  lexicographically.

  The first byte that differs between two keys determines their sort order,
  with the lower byte value ranking first. If the prefix of the longer key
  matches all the bytes in the shorter key, the shorter key ranks first."
  (:refer-clojure :exclude [bytes? compare min max])
  (:import
    blocks.data.PersistentBytes))


;; ## Key Construction

(defn create
  "Construct a new `PersistentBytes` value containing the given byte data,
  which should either be a byte array or a sequence of byte values."
  [data]
  (if (sequential? data)
    (PersistentBytes/wrap (byte-array data))
    (PersistentBytes/copyFrom data)))


(defn get-bytes
  "Return a copy of the raw bytes inside a key."
  ^bytes
  [key]
  (.toByteArray ^PersistentBytes key))


(defn bytes?
  "Predicate which returns true if `x` is a `PersistentBytes` value."
  [x]
  (instance? PersistentBytes x))



;; ## Comparators

(defn compare
  "Lexicographically compares two byte-array keys for order. Returns a negative
  number, zero, or a positive number if `a` is less than, equal to, or greater
  than `b`, respectively.

  This ranking compares each byte in the keys in order; the first byte which
  differs determines the ordering; if the byte in `a` is less than the byte in
  `b`, `a` ranks before `b`, and vice versa.

  If the keys differ in length, and all the bytes in the shorter key match the
  longer key, the shorter key ranks first."
  [a b]
  (let [prefix-len (clojure.core/min (count a) (count b))]
    (loop [i 0]
      (if (< i prefix-len)
        ; Compare next byte in sequence
        (let [ai (bit-and (nth a i) 0xFF)
              bi (bit-and (nth b i) 0xFF)]
          (if (= ai bi)
            (recur (inc i))
            (- ai bi)))
        ; Reached the end of the shorter key, compare lengths.
        (- (count a) (count b))))))


(defn before?
  "Returns true if `k` is ranked before `x`."
  [k x]
  (neg? (compare k x)))


(defn after?
  "Returns true if `k` is ranked after `x`."
  [k x]
  (pos? (compare k x)))


(defn min
  "Returns the least of the given keys."
  ([x]
   x)
  ([x y]
   (if (neg? (compare x y)) x y))
  ([x y & more]
   (reduce min x (cons y more))))


(defn max
  "Returns the greatest of the given keys."
  ([x]
   x)
  ([x y]
   (if (neg? (compare y x)) x y))
  ([x y & more]
   (reduce max x (cons y more))))



;; ## Lexicoder Functions

; TODO: lexicoder functions
