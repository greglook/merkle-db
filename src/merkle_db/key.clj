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
      StandardCharsets)
    java.time.Instant))


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



;; ## Lexicoder Protocol

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



;; ## Lexicoder Utilities

(defn ^:no-doc escape-bytes
  "Escape the given byte sequence, replacing any 0x00 bytes with 0x0101 and any
  0x01 bytes with 0x0102."
  ^bytes
  [^bytes data]
  (let [escape-count (count (filter #(or (== 0x00 %) (== 0x01 %)) data))]
    (if (zero? escape-count)
      ; Nothing to escape, return data unchanged.
      data
      ; Generate escaped bytes.
      (let [edata (byte-array (+ escape-count (alength data)))]
        (loop [i 0, o 0]
          (if (= (alength data) i)
            edata
            (let [b (byte (aget data i))]
              (condp == b
                0x00
                (do (aset-byte edata o 0x01)
                    (aset-byte edata (inc o) 0x01)
                    (recur (inc i) (inc (inc o))))

                0x01
                (do (aset-byte edata o 0x01)
                    (aset-byte edata (inc o) 0x02)
                    (recur (inc i) (inc (inc o))))

                (do (aset-byte edata o b)
                    (recur (inc i) (inc o)))))))))))


(defn ^:no-doc unescape-bytes
  "Unescape the given byte sequence, replacing escaped 0x00 and 0x01 bytes with
  their original values."
  ^bytes
  [^bytes edata]
  (let [escape-count (loop [c 0
                            [ebyte & erest] edata]
                       (if ebyte
                         (if (== 0x01 ebyte)
                           (recur (inc c) (rest erest))
                           (recur c erest))
                         c))]
    (if (zero? escape-count)
      ; Nothing to unescape, return data unchanged.
      edata
      ; Generate unescaped bytes.
      (let [data (byte-array (- (alength edata) escape-count))]
        (loop [i 0, o 0]
          (if (= (alength edata) i)
            data
            (let [b (byte (aget edata i))]
              (if (== 0x01 b)
                (do (aset-byte data o (- (aget edata (inc i)) 1))
                    (recur (inc (inc i)) (inc o)))
                (do (aset-byte data o b)
                    (recur (inc i) (inc o)))))))))))


(defn ^:no-doc join-bytes
  "Join a sequence of byte arrays together with 0x00 bytes."
  [byte-arrays]
  (if (empty? byte-arrays)
    (byte-array 0)
    (let [data (byte-array (reduce + (dec (count byte-arrays))
                                   (map alength byte-arrays)))]
      (loop [elements byte-arrays
             idx 0]
        (if-let [^bytes element (first elements)]
          (let [idx' (+ idx (alength element))]
            (System/arraycopy element 0 data idx (alength element))
            (if (< idx' (alength data))
              (do (aset-byte data idx' 0x00)
                  (recur (rest elements) (long (inc idx'))))
              (recur (rest elements) (long idx'))))
          data)))))


(defn ^:no-doc split-bytes
  "Split a byte array into sections separated by 0x00 bytes."
  [^bytes data offset length]
  (if (empty? data)
    []
    (loop [byte-arrays []
           idx offset
           i offset]
      (if (< i (+ offset length))
        ; Scan for next 0x00 separator.
        (if (== 0x00 (aget data i))
          ; Copy element into new array.
          (let [element (byte-array (- i idx))]
            (System/arraycopy data idx element 0 (alength element))
            (recur (conj byte-arrays element)
                   (inc i)
                   (inc i)))
          ; Keep scanning.
          (recur byte-arrays idx (inc i)))
        ; Hit end of data, copy last element.
        (let [element (byte-array (- i idx))]
          (System/arraycopy data idx element 0 (alength element))
          (conj byte-arrays element))))))



;; ## String Lexicoder

(defrecord StringLexicoder
  [^Charset charset]

  Lexicoder

  (encode*
    [_ value]
    (when (empty? value)
      (throw (IllegalArgumentException.
               "Cannot encode empty strings")))
    (.getBytes (str value) charset))

  (decode*
    [_ data offset len]
    (when (empty? data)
      (throw (IllegalArgumentException.
               "Cannot decode empty byte arrays")))
    (String. ^bytes data (long offset) (long len) charset)))


(alter-meta! #'->StringLexicoder assoc :private true)
(alter-meta! #'map->StringLexicoder assoc :private true)


(defn string-lexicoder*
  "Constructs a new string lexicoder with the given charset."
  [charset]
  (->StringLexicoder charset))


(def string-lexicoder
  "Lexicoder for UTF-8 character strings."
  (string-lexicoder* StandardCharsets/UTF_8))



;; ## Long Lexicoder

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
                 (aget ^bytes data (+ offset i))
                 (long)
                 (bit-and 0xFF)
                 (as-> b (if (neg? b) (+ b 256) b))
                 (bit-shift-left (- 56 (* i 8)))
                 (+ value)))
        (bit-xor value Long/MIN_VALUE)))))


(alter-meta! #'->LongLexicoder assoc :private true)
(alter-meta! #'map->LongLexicoder assoc :private true)


(def long-lexicoder
  "Lexicoder for long integer values."
  (->LongLexicoder))



;; ## Double Lexicoder

(defrecord DoubleLexicoder
  []

  Lexicoder

  (encode*
    [_ value]
    (encode*
      long-lexicoder
      (let [bits (Double/doubleToRawLongBits
                   (if (zero? value) 0.0 (double value)))]
        (if (neg? bits)
          (bit-xor (bit-not bits) Long/MIN_VALUE)
          bits))))

  (decode*
    [_ data offset len]
    (let [bits (decode* long-lexicoder data offset len)]
      (Double/longBitsToDouble
        (if-not (neg? bits)
          bits
          (bit-not (bit-xor bits Long/MIN_VALUE)))))))


(alter-meta! #'->DoubleLexicoder assoc :private true)
(alter-meta! #'map->DoubleLexicoder assoc :private true)


(def double-lexicoder
  "Lexicoder for double-precision floating point values."
  (->DoubleLexicoder))



;; ## Instant Lexicoder

(defrecord InstantLexicoder
  []

  Lexicoder

  (encode*
    [_ value]
    (when-not (instance? Instant value)
      (throw (IllegalArgumentException.
               (str "Input to instant lexicoder must be an Instant, got: "
                    (pr-str value)))))
    (encode* long-lexicoder (.toEpochMilli ^Instant value)))

  (decode*
    [_ data offset len]
    (Instant/ofEpochMilli (decode* long-lexicoder data offset len))))


(alter-meta! #'->InstantLexicoder assoc :private true)
(alter-meta! #'map->InstantLexicoder assoc :private true)


(def instant-lexicoder
  "Lexicoder for instants in time."
  (->InstantLexicoder))



;; ## Sequence Lexicoder

(defrecord SequenceLexicoder
  [element-coder]

  Lexicoder

  (encode*
    [_ value]
    (->> value
         (map #(escape-bytes (encode* element-coder %)))
         (join-bytes)))

  (decode*
    [_ data offset len]
    (->> (split-bytes data offset len)
         (mapv #(let [udata (unescape-bytes %)]
                  (decode* element-coder udata 0 (alength udata)))))))


(alter-meta! #'->SequenceLexicoder assoc :private true)
(alter-meta! #'map->SequenceLexicoder assoc :private true)


(defn sequence-lexicoder
  "Constructs a lexicoder for homogeneous sequences of elements which will be
  coded with the given lexicoder."
  [element-coder]
  (->SequenceLexicoder element-coder))



;; ## Tuple Lexicoder

(defrecord TupleLexicoder
  [coders]

  Lexicoder

  (encode*
    [_ value]
    (when (not= (count value) (count coders))
      (throw (IllegalArgumentException.
               (format "Cannot encode tuple which does not match lexicoder count %d: %s"
                       (count coders) (pr-str value)))))
    (join-bytes (mapv #(escape-bytes (encode* %1 %2)) coders value)))

  (decode*
    [_ data offset len]
    (let [elements (split-bytes data offset len)]
      (when (not= (count coders) (count elements))
        (throw (IllegalArgumentException.
                 (format "Cannot decode tuple which does not match lexicoder count %d: %s"
                         (count coders) (pr-str elements)))))
      (mapv #(let [udata (unescape-bytes %2)]
               (decode* %1 udata 0 (alength udata)))
            coders elements))))


(alter-meta! #'->TupleLexicoder assoc :private true)
(alter-meta! #'map->TupleLexicoder assoc :private true)


(defn tuple-lexicoder
  "Constructs a lexicoder for a fixed-size tuple of values which will be coded
  with the given lexicoders, in order."
  [& coders]
  (->TupleLexicoder (vec coders)))
