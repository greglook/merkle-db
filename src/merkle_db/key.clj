(ns merkle-db.key
  "Record keys are immutable byte sequences which uniquely identify a record
  within a table. Keys are stored in sorted order by comparing them
  lexicographically.

  The first byte that differs between two keys determines their sort order,
  with the lower byte value ranking first. If the prefix of the longer key
  matches all the bytes in the shorter key, the shorter key ranks first."
  (:refer-clojure :exclude [bytes? compare min max])
  (:import
    blocks.data.PersistentBytes
    (java.nio.charset
      Charset
      StandardCharsets)))


;; ## Key Construction

(defn create
  "Construct a new `PersistentBytes` value containing the given byte data,
  which should either be a byte array or a sequence of byte values."
  [data]
  (PersistentBytes/wrap (byte-array data)))


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



;; ## Lexicoders

(defprotocol Lexicoder
  "Simple codec for transforming values into keys that have specific ordering
  semantics."

  (encode*
    [coder value]
    "Return the encoded value as a byte array.")

  (decode*
    [coder data offset len]
    "Read an object from the byte array at the given offset."))


(defn encode
  "Encodes the given value and returns persistent key bytes."
  [coder value]
  (PersistentBytes/wrap (encode* coder value)))


(defn decode
  "Decodes the given key byte data and returns a value."
  ([coder data]
   (decode coder data 0 (count data)))
  ([coder data offset len]
   (let [byte-data (if (bytes? data)
                     (get-bytes data)
                     (byte-array data))]
     (decode* coder byte-data offset len))))


;; ### String Lexicoder

(defrecord StringLexicoder
  [^Charset charset]

  Lexicoder

  (encode*
    [_ value]
    (.getBytes (str value) charset))

  (decode*
    [_ data offset len]
    (if (empty? data)
      ""
      (String. data offset len charset))))


(alter-meta! #'->StringLexicoder assoc :private true)
(alter-meta! #'map->StringLexicoder assoc :private true)


(defn string-lexicoder
  ([]
   (string-lexicoder StandardCharsets/UTF_8))
  ([charset]
   (->StringLexicoder charset)))


;; ### Long Lexicoder

(defrecord LongLexicoder
  []

  Lexicoder

  (encode*
    [_ value]
    ; Flip sign bit so that positive values sort after negative values.
    (let [lexed (bit-xor (long value) Long/MIN_VALUE)
          data (byte-array 8)]
      (dotimes [i 8]
        (->
          lexed
          (bit-shift-right (- 56 (* i 8)))
          (bit-and 0xFF)
          (as-> b (if (< 127 b) (- b 256) b))
          (->> (aset-byte data i))))
      data))

  (decode*
    [_ data offset len]
    (when (not= len 8)
      (throw (IllegalArgumentException.
               (str "Can only read 8 byte long keys: " len))))
    (loop [i 0
           value 0]
      (if (< i 8)
        (recur (inc i)
               (->
                 (aget data (+ offset i))
                 (long)
                 (bit-and 0xFF)
                 (as-> b (if (neg? b) (+ b 256) b))
                 (bit-shift-left (- 56 (* i 8)))
                 (+ value)))
        (bit-xor value Long/MIN_VALUE)))))


(alter-meta! #'->LongLexicoder assoc :private true)
(alter-meta! #'map->LongLexicoder assoc :private true)


(defn long-lexicoder
  []
  (->LongLexicoder))


; TODO: double lexicoder
; TODO: instant lexicoder
; TODO: uuid lexicoder
