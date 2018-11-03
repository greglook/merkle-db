(ns merkle-db.key
  "Record keys are immutable byte sequences which uniquely identify a record
  within a table. Keys are stored in sorted order by comparing them
  lexicographically.

  The first byte that differs between two keys determines their sort order,
  with the lower byte value ranking first. If the prefix of the longer key
  matches all the bytes in the shorter key, the shorter key ranks first."
  (:refer-clojure :exclude [min max])
  (:require
    [clojure.spec.alpha :as s])
  (:import
    clojure.lang.Murmur3
    (java.nio.charset
      Charset
      StandardCharsets)
    java.time.Instant
    java.util.Arrays
    (javax.xml.bind
      DatatypeConverter)))


;; ## Comparators

(defn- compare-bytes
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



;; ## Key Construction

(deftype Key
  [^bytes data
   _meta
   ^:unsynchronized-mutable _hash]

  Object

  (toString
    [this]
    (str "key:" (DatatypeConverter/printHexBinary data)))


  (equals
    [this that]
    (boolean
      (or (identical? this that)
          (and (identical? (class this) (class that))
               (= (count data) (count that))
               (zero? (compare-bytes data (.data ^Key that)))))))


  clojure.lang.IHashEq

  (hashCode
    [this]
    (let [hc _hash]
      (if (zero? hc)
        (let [hc (int (bit-xor (hash :merkle-db/key)
                               (Murmur3/hashOrdered (seq data))))]
          (set! _hash hc)
          hc)
        hc)))


  (hasheq
    [this]
    (.hashCode this))


  clojure.lang.IObj

  (meta
    [this]
    _meta)


  (withMeta
    [this meta-map]
    (Key. data meta-map _hash))


  clojure.lang.Counted

  (count
    [this]
    (count data))


  Comparable

  (compareTo
    [this that]
    (when-not (instance? Key that)
      (throw (RuntimeException. "Keys can only be compared to keys")))
    (compare-bytes data (.data ^Key that))))


(alter-meta! #'->Key assoc :private true)


(defn create
  "Construct a new key value containing the given byte data, which should
  either be a byte array or a sequence of byte values."
  [data]
  (->Key (byte-array data) nil 0))


(defn get-bytes
  "Return a copy of the raw bytes inside a key."
  ^bytes
  [^Key k]
  (Arrays/copyOf ^bytes (.data k) (count k)))


(defn key?
  "Predicate which returns true if `x` is a key value."
  [x]
  (instance? Key x))


(defn hex
  "Return a hexadecimal string encoding the bytes in the key."
  [^Key k]
  (DatatypeConverter/printHexBinary (.data k)))


(defn parse
  "Parse a key from a hexadecimal string."
  [s]
  (create (DatatypeConverter/parseHexBinary s)))


(defmethod print-method Key
  [x w]
  (print-method (tagged-literal 'merkle-db/key (hex x)) w))



;; ## Lexicoder Protocol

(defprotocol ^:no-doc Lexicoder
  "Simple codec for transforming values into keys that have specific ordering
  semantics."

  (lexicoder-config
    [coder]
    "Return a configuration value representing this lexicoder.")

  (encode*
    [coder value]
    "Encode the key value as a byte array.")

  (decode*
    [coder data offset len]
    "Decode a key value from a section of a byte array."))


(defn encode
  "Encodes the given value and returns persistent key bytes."
  [coder value]
  (create (encode* coder value)))


(defn decode
  "Decodes the given key byte data and returns a value."
  ([coder data]
   (decode coder data 0 (count data)))
  ([coder data offset len]
   (let [byte-data (if (key? data)
                     (get-bytes data)
                     (byte-array data))]
     (decode* coder byte-data offset len))))



;; ## Lexicoder Construction

;; Lexicoders can be specified either as a simple type keyword or a vector of
;; a type keyword and some parameters.
(s/def ::lexicoder
  (s/or :simple keyword?
        :params (s/and vector? (s/cat :type keyword?
                                      :args (s/* any?)))))


(defn- config-type
  "Determine the lexicoder type specified by a configuration data structure.
  The structure should be either a keyword or a vector with the first element a
  keyword providing the type value."
  [config]
  (if (keyword? config)
    config
    (if (and (vector? config) (keyword? (first config)))
      (first config)
      (throw (ex-info
               (str "Cannot determine lexicoder type from invalid config; expecting a keyword or vector with keyword, got: "
                    (pr-str config))
               {:config config})))))


(defn- config-params
  "Returns the parameters given to a lexicoder spec. For simple keyword, this
  function returns nil; for the vector form, it returns a sequence of the
  values following the lexicoder type key."
  [config]
  (when (vector? config)
    (next config)))


(defmulti lexicoder
  "Construct a lexicoder from a configuration data structure. The structure
  should be either a keyword or a vector with the first element a keyword
  providing the dispatch value."
  #'config-type)



;; ## Key Encoding Utilities

(defn- to-byte
  "Coerces a number to a byte value."
  [x]
  (if (< 127 x) (- x 256) x))


(defn- from-byte
  "Coerces a byte value to a number."
  [x]
  (if (neg? x) (+ 256 x) x))


(defn- escape-bytes
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


(defn- unescape-bytes
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
                (do (aset-byte data o (dec (aget edata (inc i))))
                    (recur (inc (inc i)) (inc o)))
                (do (aset-byte data o b)
                    (recur (inc i) (inc o)))))))))))


(defn- join-bytes
  "Join a sequence of byte arrays together with 0x00 bytes."
  [byte-arrays]
  (if (empty? byte-arrays)
    (byte-array 0)
    (let [data (byte-array (reduce + (dec (count byte-arrays))
                                   (map count byte-arrays)))]
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


(defn- split-bytes
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



;; ## Bytes Lexicoder

;; The byte lexicoder provides a simple pass-through implementation to convert
;; byte arrays directly into key bytes.
(deftype BytesLexicoder [])


(def bytes-lexicoder
  "Lexicoder for passing through raw byte arrays."
  (->BytesLexicoder))


(ns-unmap *ns* '->BytesLexicoder)


;; Returns the global byte lexicoder.
(defmethod lexicoder :bytes
  [config]
  (when (seq (config-params config))
    (throw (ex-info
             (str "Bytes lexicoder config takes no parameters: " (pr-str config))
             {:config config})))
  bytes-lexicoder)


(extend-type BytesLexicoder

  Lexicoder

  (lexicoder-config
    [_]
    :bytes)

  (encode*
    [_ value]
    (when-not (bytes? value)
      (throw (IllegalArgumentException.
               (format "BytesLexicoder cannot encode non-byte-array value: %s (%s)"
                       (pr-str value)
                       (some-> (class value) (.getName))))))
    (when (zero? (count value))
      (throw (IllegalArgumentException.
               "BytesLexicoder cannot encode empty byte arrays")))
    value)

  (decode*
    [_ data offset len]
    (when (or (empty? data) (zero? len))
      (throw (IllegalArgumentException.
               "BytesLexicoder cannot decode empty byte arrays")))
    (let [data' (byte-array len)]
      (System/arraycopy data offset data' 0 len)
      data')))



;; ## String Lexicoder

;; String lexicoders perform a translation between character sequences and
;; key bytes using a character set.
(deftype StringLexicoder [^Charset charset])


(def ^:private default-string-charset
  "Default character set to lexicode strings with."
  StandardCharsets/UTF_8)


(defn string-lexicoder*
  "Constructs a new string lexicoder with the given charset."
  [charset]
  (->StringLexicoder charset))


(def string-lexicoder
  "Lexicoder for UTF-8 character strings."
  (string-lexicoder* default-string-charset))


(ns-unmap *ns* '->StringLexicoder)


;; Configures a new string lexicoder using either the given character set or
;; the default of UTF-8, if none is provided.
;;
;;     ; Use ASCII string keys
;;     [:string "US-ASCII"]
;;
(defmethod lexicoder :string
  [config]
  (when (< 1 (count (config-params config)))
    (throw (ex-info
             (str "String lexicoder config takes at most one parameter: " (pr-str config))
             {:config config})))
  (if-let [[charset] (config-params config)]
    (string-lexicoder* (Charset/forName charset))
    string-lexicoder))


(extend-type StringLexicoder

  Lexicoder

  (lexicoder-config
    [this]
    (if (= (.charset this) default-string-charset)
      :string
      [:string (str (.charset this))]))

  (encode*
    [this value]
    (when-not (string? value)
      (throw (IllegalArgumentException.
               (format "StringLexicoder cannot encode non-string value: %s (%s)"
                       (pr-str value)
                       (some-> (class value) (.getName))))))
    (when (empty? value)
      (throw (IllegalArgumentException.
               "StringLexicoder cannot encode empty strings")))
    (.getBytes (str value) ^Charset (.charset this)))

  (decode*
    [this data offset len]
    (when (empty? data)
      (throw (IllegalArgumentException.
               "StringLexicoder cannot decode empty byte arrays")))
    (String. ^bytes data (long offset) (long len) ^Charset (.charset this))))



;; ## Integer Lexicoder

;; The integer lexicoder converts integer values into key bytes.
;;
;; In order to reduce key size, the value is encoded with an initial sorting
;; byte which gives the sign of the value and the number of bytes required to
;; represent it. This is similar to the exponent field in a floating-point
;; number.
;;
;; Values between 0 and 255 are represented with a leading byte of 0x80,
;; followed by one byte holding the integer value. This encodes zero as 0x8000.
;; Values between -256 and -1 are represented with a leading byte of 0x7F, so
;; that they sort just before the smallest positive values. The byte following
;; holds the two's-complement representation, so -256 is 0x7F00 and -1 is
;; 0x7FFF.
;;
;; Each further offset from the center increments the number of bytes used to
;; represent the integer value, so 0x7E and 0x81 use two additional bytes, 0x7D
;; and 0x82 use three, 0x7C and 0x83 use four, and so on. This means a 64-bit
;; integer can be encoded with leading bytes 0x78-0x87 in a total of 2-9 bytes.
;;
(deftype IntegerLexicoder [])


(def integer-lexicoder
  "Lexicoder for long integer values."
  (->IntegerLexicoder))


(ns-unmap *ns* '->IntegerLexicoder)


;; Returns the global integer lexicoder.
(defmethod lexicoder :integer
  [config]
  (when (config-params config)
    (throw (ex-info
             (str "Integer lexicoder config takes no parameters: " (pr-str config))
             {:config config})))
  integer-lexicoder)


(defn- flip-long-sign
  "Flip the sign bit on a long value."
  ^long
  [^long x]
  (bit-xor x Long/MIN_VALUE))


(defn- get-long-byte
  "Return the `i`th byte in a long, where 0 gives the least significant byte."
  [l i]
  (bit-and 0xFF (unsigned-bit-shift-right l (* i 8))))


(defn- integer-byte-length
  "Returns the number of bytes required to represent the given integer value,
  ignoring the sign bit.

  If the number is between -256 and 255, then value should be 1; for -65536 to
  -257 and 256 to 65535, the value should be 2; and so on."
  [value]
  (let [prefix (if (neg? value) 0xFF 0x00)
        value (long value)]
    (loop [index 7]
      (if (and (pos? index)
               (= prefix (bit-and 0xFF (unsigned-bit-shift-right value (* 8 index)))))
        (recur (dec index))
        (inc index)))))


(extend-type IntegerLexicoder

  Lexicoder

  (lexicoder-config
    [_]
    :integer)

  (encode*
    [_ value]
    (when-not (integer? value)
      (throw (IllegalArgumentException.
               (format "IntegerLexicoder cannot encode non-integer value: %s (%s)"
                       (pr-str value)
                       (some-> (class value) (.getName))))))
    (let [int-length (integer-byte-length value)
          data (byte-array (inc int-length))
          header (if (neg? value)
                   (- 0x80 int-length)
                   (+ 0x7F int-length))]
      (aset-byte data 0 (to-byte header))
      (dotimes [i int-length]
        (->> (get-long-byte value (- int-length i 1))
             (to-byte)
             (aset-byte data (inc i))))
      data))

  (decode*
    [_ data offset len]
    (let [header (from-byte (aget ^bytes data offset))
          int-length (if (< header 0x80)
                       (- 0x80 header)
                       (- header 0x7F))]
      (when (< len (inc int-length))
        (throw (IllegalArgumentException.
                 (format "Not enough bytes to decode %d-byte integer value: %d left"
                         int-length (dec len)))))
      (loop [i (+ offset len -1)
             shift 0
             value 0]
        (if (<= (inc offset) i)
          (recur (dec i)
                 (+ shift 8)
                 (+ value (bit-shift-left (from-byte (aget ^bytes data i)) shift)))
          (if (and (< header 0x80) (< int-length 8))
            (bit-or value (bit-shift-left -1 (* 8 int-length)))
            value))))))



;; ## Floating Point Lexicoder

;; The float lexicoder converts floating-point values into key bytes.
(deftype FloatLexicoder [])


(def float-lexicoder
  "Lexicoder for double-precision floating point values."
  (->FloatLexicoder))


(ns-unmap *ns* '->FloatLexicoder)


;; Returns the global float lexicoder.
(defmethod lexicoder :float
  [config]
  (when (config-params config)
    (throw (ex-info
             (str "Float lexicoder config takes no parameters: "
                  (pr-str config))
             {:config config})))
  float-lexicoder)


(extend-type FloatLexicoder

  Lexicoder

  (lexicoder-config
    [_]
    :float)

  (encode*
    [_ value]
    (when-not (float? value)
      (throw (IllegalArgumentException.
               (format "FloatLexicoder cannot encode non-floating-point value: %s (%s)"
                       (pr-str value)
                       (some-> (class value) (.getName))))))
    (when (Double/isNaN value)
      (throw (IllegalArgumentException.
               "FloatLexicoder cannot encode NaN value as a valid key")))
    (encode*
      integer-lexicoder
      (let [bits (if (zero? value)
                   0 ; prevent -0.0
                   (Double/doubleToRawLongBits (double value)))]
        (if (neg? bits)
          (flip-long-sign (bit-not bits))
          bits))))

  (decode*
    [_ data offset len]
    (let [bits (decode* integer-lexicoder data offset len)]
      (Double/longBitsToDouble
        (if (neg? bits)
          (bit-not (flip-long-sign bits))
          bits)))))



;; ## Instant Lexicoder

;; The instant lexicoder converts `java.time.Instant` values into key bytes.
(deftype InstantLexicoder [])


(def instant-lexicoder
  "Lexicoder for instants in time."
  (->InstantLexicoder))


(ns-unmap *ns* '->InstantLexicoder)


;; Returns the global instant lexicoder.
(defmethod lexicoder :instant
  [config]
  (when (config-params config)
    (throw (ex-info
             (str "Instant lexicoder config takes no parameters: " (pr-str config))
             {:config config})))
  instant-lexicoder)


(extend-type InstantLexicoder

  Lexicoder

  (lexicoder-config
    [_]
    :instant)

  (encode*
    [_ value]
    (when-not (instance? Instant value)
      (throw (IllegalArgumentException.
               (format "InstantLexicoder cannot encode non-instant value: %s (%s)"
                       (pr-str value)
                       (some-> (class value) (.getName))))))
    (encode* integer-lexicoder (.toEpochMilli ^Instant value)))

  (decode*
    [_ data offset len]
    (Instant/ofEpochMilli (decode* integer-lexicoder data offset len))))



;; ## Sequence Lexicoder

;; A sequence lexicoder takes a homogeneous sequence of elements and encodes it
;; such that the sequence as whole is lexically ordered.
(deftype SequenceLexicoder [element-coder])


(defn sequence-lexicoder
  "Constructs a lexicoder for homogeneous sequences of elements which will be
  coded with the given lexicoder."
  [element-coder]
  (->SequenceLexicoder element-coder))


(ns-unmap *ns* '->SequenceLexicoder)


(defmethod lexicoder :seq
  [config]
  (when (not= 1 (count (config-params config)))
    (throw (ex-info
             (str "Sequence lexicoder config takes exactly one parameter: " (pr-str config))
             {:config config})))
  (sequence-lexicoder (lexicoder (second config))))


;; Configures a new sequence lexicoder by wrapping an element coder:
;;
;;     ; Sequence of string path segments.
;;     [:seq :string]
;;
(extend-type SequenceLexicoder

  Lexicoder

  (lexicoder-config
    [this]
    [:seq (lexicoder-config (.element-coder this))])

  (encode*
    [this value]
    (when-not (sequential? value)
      (throw (IllegalArgumentException.
               (format "SequenceLexicoder cannot encode non-sequential value: %s (%s)"
                       (pr-str value)
                       (some-> (class value) (.getName))))))
    (->> value
         (map #(escape-bytes (encode* (.element-coder this) %)))
         (join-bytes)))

  (decode*
    [this data offset len]
    (mapv #(let [udata (unescape-bytes %)]
             (decode* (.element-coder this) udata 0 (alength udata)))
          (split-bytes data offset len))))



;; ## Tuple Lexicoder

;; Tuple lexicoders encode a heterogeneous mix of key elements as key bytes
;; which will be ordered such that the tuple as a whole is lexically ordered.
;;
;; Specifically, if tuples `[x1 y1]` and `[x2 y2]` are compared, and `x1` ranks
;; before `x2`, the first tuple will rank before the second, no matter what
;; values `y1` and `y2` have. Alternately, if `x1` equals `x2`, then `y1` and
;; `y2` will determine the ranking.
(deftype TupleLexicoder [coders])


(defn tuple-lexicoder
  "Constructs a lexicoder for a fixed-size tuple of values which will be coded
  with the given lexicoders, in order."
  [& coders]
  (->TupleLexicoder (vec coders)))


(ns-unmap *ns* '->TupleLexicoder)


;; Configures a new tuple lexicoder by wrapping a collection of lexicoder
;; configurations:
;;
;;     ; Composite ordering by name then numeric version.
;;     [:tuple :string :integer]
;;
(defmethod lexicoder :tuple
  [config]
  (when-not (config-params config)
    (throw (ex-info
             (str "Tuple lexicoder config takes one or more parameters: " (pr-str config))
             {:config config})))
  (apply tuple-lexicoder (map lexicoder (rest config))))


(extend-type TupleLexicoder

  Lexicoder

  (lexicoder-config
    [this]
    (vec (cons :tuple (map lexicoder-config (.coders this)))))

  (encode*
    [this value]
    (when-not (sequential? value)
      (throw (IllegalArgumentException.
               (format "TupleLexicoder cannot encode non-sequential value: %s (%s)"
                       (pr-str value)
                       (some-> (class value) (.getName))))))
    ; TODO: actually, let this encode shorter keys for prefix searching
    (when (not= (count (.coders this)) (count value))
      (throw (IllegalArgumentException.
               (format "TupleLexicoder cannot encode tuple which does not match lexicoder count %d: %s"
                       (count (.coders this)) (pr-str value)))))
    (join-bytes (mapv #(escape-bytes (encode* %1 %2)) (.coders this) value)))

  (decode*
    [this data offset len]
    (let [elements (split-bytes data offset len)]
      (when (not= (count (.coders this)) (count elements))
        (throw (IllegalArgumentException.
                 (format "Cannot decode tuple which does not match lexicoder count %d: %s"
                         (count (.coders this)) (pr-str elements)))))
      (mapv #(let [udata (unescape-bytes %2)]
               (decode* %1 udata 0 (alength udata)))
            (.coders this) elements))))



;; ## Reverse Lexicoder

;; The reversing lexicoder can be used to invert the ordering of another
;; lexicoder. Each byte in the output of the wrapped coder is subtracted from
;; the max value to reverse the comparison order.
(deftype ReverseLexicoder [coder])


(defn reverse-lexicoder
  "Wraps the given lexicoder to reverse the ordering of keys encoded with it."
  [coder]
  (->ReverseLexicoder coder))


(ns-unmap *ns* '->ReverseLexicoder)


;; Confures a new reverse lexicoder by wrapping another lexicoder
;; configuration:
;;
;;     ; Sort by time, newest first.
;;     [:reverse :instant]
;;
(defmethod lexicoder :reverse
  [config]
  (when (not= 1 (count (config-params config)))
    (throw (ex-info
             (str "Reverse lexicoder config takes exactly one parameter: " (pr-str config))
             {:config config})))
  (reverse-lexicoder (lexicoder (second config))))


(defn- flip-byte
  "Returns the inverse of a byte value, accounting for the sign bit."
  [b]
  (let [x (- 255 (if (neg? b) (+ b 256) b))]
    (if (< 127 x)
      (- x 256)
      x)))


(extend-type ReverseLexicoder

  Lexicoder

  (lexicoder-config
    [this]
    [:reverse (lexicoder-config (.coder this))])

  (encode*
    [this value]
    (let [encoded ^bytes (encode* (.coder this) value)
          rdata (byte-array (alength encoded))]
      (dotimes [i (alength encoded)]
        (aset-byte rdata i (flip-byte (aget encoded i))))
      rdata))

  (decode*
    [this data offset len]
    (let [encoded (byte-array len)]
      (dotimes [i (alength encoded)]
        (aset-byte encoded i (flip-byte (aget ^bytes data (+ offset i)))))
      (decode* (.coder this) encoded 0 len))))
