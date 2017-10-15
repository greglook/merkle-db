(ns merkle-db.index-test
  (:require
    [clojure.set :as set]
    [clojure.spec :as s]
    [clojure.string :as str]
    [clojure.test :refer :all]
    [clojure.test.check.generators :as gen]
    [com.gfredericks.test.chuck.clojure-test :refer [checking]]
    [com.gfredericks.test.chuck.generators :as tcgen]
    [merkledag.core :as mdag]
    [merkledag.link :as link]
    [merkledag.node :as node]
    [merkle-db.generators :as mdgen]
    [merkle-db.graph :as graph]
    [merkle-db.index :as index]
    [merkle-db.key :as key]
    [merkle-db.partition :as part]
    [merkle-db.patch :as patch]
    [merkle-db.record :as record]
    [merkle-db.validate :as validate]
    [merkle-db.test-utils :as tu]))

;;       A
;;      / \
;;     B   C
;;    /|\ / \
;;    012 3 4


(def params
  {::index/fan-out 4
   ::part/limit 5
   ::record/families {:bc #{:b :c}}})


(defn nth-key
  "Generate a key for index i."
  [i]
  (key/create [i]))


(defn nth-record
  "Generate a record for index i."
  [i]
  (cond-> {:a i}
    (zero? (mod i 3)) (assoc :b (- 100 i))
    (zero? (mod i 5)) (assoc :c (+ 20 i))))


(defn records
  "Return a sequence of the records at each of the given indexes."
  [& idxs]
  (map (juxt nth-key nth-record) idxs))


(defn tombstones
  "Return a sequence of tombstone markers at each of the given indexes."
  [& idxs]
  (map (juxt nth-key (constantly ::patch/tombstone)) idxs))


(defn nth-child
  "Loads and returns the nth child of the given index node."
  [store node i]
  (graph/get-link! store node (nth (::index/children node) i)))


(defmacro ^:private with-index-fixture
  [& body]
  `(let [store# (mdag/init-store :types graph/codec-types)
         ~'store store#
         ~'part0 (part/from-records store# params (records 4 5 6))
         ~'part1 (part/from-records store# params (records 7 8 10 11))
         ~'part2 (part/from-records store# params (records 12 13 14 17 18))
         ~'part3 (part/from-records store# params (records 21 23 24 25))
         ~'part4 (part/from-records store# params (records 30 31 32))
         ~'idx0 (index/build-tree store# params [~'part0 ~'part1 ~'part2])
         ~'idx1 (index/build-tree store# params [~'part3 ~'part4])
         ~'root (index/build-tree store# params [~'idx0 ~'idx1])]
     ~@body))


(defmacro ^:private is-index
  [node height child-count record-count first-key-idx last-key-idx]
  `(let [node# ~node]
     (is (= index/data-type (:data/type node#))
         "has index data type")
     (is (= ~height (::index/height node#))
         "has expected height")
     (is (= ~child-count (count (::index/children node#)))
         "has expected number of children")
     (is (= ~record-count (::record/count node#))
         "contains expected number of records")
     (is (= (nth-key ~first-key-idx) (::record/first-key node#))
         "contains expected first key")
     (is (= (nth-key ~last-key-idx) (::record/last-key node#))
         "contains expected last key")))


(deftest index-reading
  (with-index-fixture
    (testing "root properties"
      (is (= index/data-type (:data/type root)))
      (is (= 2 (::index/height root)))
      (is (= 2 (count (::index/children root))))
      (is (= 19 (::record/count root)))
      (is (= (nth-key 4) (::record/first-key root)))
      (is (= (nth-key 32) (::record/last-key root))))
    (testing "read-all"
      (is (thrown? Exception
            (index/read-all store {:data/type :foo} nil)))
      (is (= (records 4 5 6 7 8 10 11 12 13 14 17 18 21 23 24 25 30 31 32)
             (index/read-all store root nil)))
      (is (= [[(nth-key 5) {:c 25}]
              [(nth-key 10) {:c 30}]
              [(nth-key 25) {:c 45}]
              [(nth-key 30) {:c 50}]]
             (index/read-all store root #{:c}))))
    (testing "read-batch"
      (is (thrown? Exception
            (index/read-batch store {:data/type :foo} nil nil)))
      (is (= (records 5 8 23)
             (index/read-batch
               store root nil
               #{(nth-key 8) (nth-key 5) (nth-key 23) (nth-key 80)})))
      (is (= [[(nth-key 12) {:b 88}]
              [(nth-key 21) {:b 79}]]
             (index/read-batch
               store root #{:b}
               #{(nth-key 12) (nth-key 21) (nth-key 22)}))))
    (testing "read-range"
      (is (thrown? Exception
            (index/read-range store {:data/type :foo} nil nil nil)))
      (is (= (records 4 5 6 7 8 10 11 12 13 14 17 18 21 23 24 25 30 31 32)
             (index/read-range store root nil nil nil)))
      (is (= [[(nth-key  6) {:b 94}]
              [(nth-key 12) {:b 88}]
              [(nth-key 18) {:b 82}]
              [(nth-key 21) {:b 79}]
              [(nth-key 24) {:b 76}]
              [(nth-key 30) {:b 70}]]
             (index/read-range store root #{:b} nil nil)))
      (is (= (records 4 5 6 7 8 10)
             (index/read-range store root nil nil (nth-key 10))))
      (is (= (records 21 23 24 25 30 31 32)
             (index/read-range
               store root nil
               (nth-key 20) nil)))
      (is (= [[(nth-key  5) {:c 25}]
              [(nth-key 10) {:c 30}]
              [(nth-key 25) {:c 45}]
              [(nth-key 30) {:c 50}]]
             (index/read-range store root #{:c} (nth-key 5) (nth-key 30)))))))


(deftest empty-root-updates
  (let [store (mdag/init-store :types graph/codec-types)
        root nil]
    (testing "bad input"
      (is (thrown? Exception (index/update-tree store params {:data/type :foo}
                                                (records 1)))))
    (testing "unchanged contents"
      (is (nil? (index/update-tree store params root [])))
      (is (nil? (index/update-tree store params root (tombstones 0)))))
    (testing "insertion"
      (let [root' (index/update-tree store params nil
                                     (concat (tombstones 1 2 3)
                                             (records 4 5)
                                             (tombstones 6 7 8)))]
        (is (= part/data-type (:data/type root')))
        (is (= 2 (::record/count root')))
        (is (= (nth-key 4) (::record/first-key root')))
        (is (= (nth-key 5) (::record/last-key root')))
        (is (= (records 4 5) (index/read-all store root' nil)))))))


(deftest partition-root-updates
  (let [store (mdag/init-store :types graph/codec-types)
        root (part/from-records store params (records 4 5 6))]
    (testing "unchanged contents"
      (is (identical? root (index/update-tree store params root [])))
      (is (identical? root (index/update-tree store params root (tombstones 1 2 7)))))
    (testing "full deletion"
      (is (nil? (index/update-tree store params root (tombstones 4 5 6)))))
    (testing "underflow"
      (let [root' (index/update-tree store params root (tombstones 4 6))]
        (is (= part/data-type (:data/type root')))
        (is (= 1 (::record/count root')))
        (is (= (nth-key 5) (::record/first-key root')))
        (is (= (nth-key 5) (::record/last-key root')))
        (is (= (records 5) (index/read-all store root' nil)))))
    (testing "update"
      (let [root' (index/update-tree store params root
                                     [[(nth-key 3) {:x 123}]
                                      [(nth-key 5) {:y 456}]
                                      [(nth-key 7) {:z 789}]])]
        (is (= part/data-type (:data/type root')))
        (is (= 5 (::record/count root')))
        (is (= (nth-key 3) (::record/first-key root')))
        (is (= (nth-key 7) (::record/last-key root')))
        (is (= [[(nth-key 3) {:x 123}]
                [(nth-key 4) {:a 4}]
                [(nth-key 5) {:y 456}]
                [(nth-key 6) {:a 6, :b 94}]
                [(nth-key 7) {:z 789}]]
               (index/read-all store root' nil)))))
    (testing "overflow"
      (let [root' (index/update-tree store params root
                                     (records 1 2 3 8 9))]
        (is-index root' 1 2 8 1 9)
        (is (= (records 1 2 3 4 5 6 8 9)
               (index/read-all store root' nil)))))))


(deftest index-tree-noop-update
  (with-index-fixture
    (is (identical? root (index/update-tree store params root [])))
    ; TODO: identical? would be a stronger guarantee here
    (is (= root (index/update-tree store params root
                                   (tombstones 0))))
    (is (= root (index/update-tree store params root
                                   (records 5 10 14 23 30))))))


(deftest index-tree-insert-2-parts
  (with-index-fixture
    (let [root' (index/update-tree store params root
                                   (records 0 1 2 3 9 15 16))]
      (is-index root' 2 2 26 0 32)
      (is (= (records 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 21 23
                      24 25 30 31 32)
             (index/read-all store root' nil)))
      (is-index (nth-child store root' 0) 1 4 19 0 18)
      (is-index (nth-child store root' 1) 1 2 7 21 32))))


(deftest index-tree-remove-part-from-B
  (with-index-fixture
    (let [root' (index/update-tree store params root
                                   (tombstones 7 8 10 11))]
      (is-index root' 2 2 15 4 32)
      (is (= (records 4 5 6 12 13 14 17 18 21 23 24 25 30 31 32)
             (index/read-all store root' nil)))
      (let [lchild (nth-child store root' 0)]
        (is-index lchild 1 2 8 4 18)
        (is (= part0 (nth-child store lchild 0)))
        (is (= part2 (nth-child store lchild 1))))
      (is (= idx1 (nth-child store root' 1))))))


(deftest index-tree-underflow-part-in-B
  (with-index-fixture
    (let [root' (index/update-tree store params root
                                   (tombstones 6))]
      (is-index root' 2 2 18 4 32)
      (is (= (records 4 5 7 8 10 11 12 13 14 17 18 21 23 24 25 30 31 32)
             (index/read-all store root' nil)))
      (is (= idx1 (nth-child store root' 1))))))


; TODO: test scenarios
; - underflow partition 1 in B => merged with 2
; - underflow partition 2 in B => merged back with 1
; - remove two partitions in B => part is carried to C
; - remove B entirely => C is new root
; - remove C entirely => B is new root
; - insert full partition into C => B is unchanged
; - insert three partitions into C => split?



;; ## Generative Tests

(defmacro timer
  [label & body]
  `(let [start# (System/nanoTime)
         result# (do ~@body)
         elapsed# (/ (- (System/nanoTime) start#) 1e6)]
     (printf "%s: %.2f ms\n" ~label elapsed#)
     (flush)
     result#))


#_
(deftest ^:generative index-updates
  (let [field-keys #{:a :b :c :d}]
    (checking "tree updates" 50
      [[families fan-out part-limit [rkeys ukeys dkeys]]
         (gen/tuple
           (tcgen/sub-map {:ab #{:a :b}, :cd #{:c :d}})
           (gen/large-integer* {:min 4, :max 32})
           (gen/large-integer* {:min 5, :max 500})
           (gen/bind
             (gen/large-integer* {:min 10, :max 5000})
             (fn [n]
               (let [all-keys (map #(key/encode key/long-lexicoder %) (range n))]
                 (gen/tuple
                   (tcgen/subsequence all-keys)
                   (gen/fmap
                     (fn [fracs]
                       (->> (map list fracs all-keys)
                            (filter #(< 0.85 (first %)))
                            (map second)))
                     (apply gen/tuple (repeat (count all-keys) (gen/double* {:min 0.0, :max 1.0}))))
                   (gen/fmap
                     (fn [fracs]
                       (->> (map list fracs all-keys)
                            (filter #(< 0.85 (first %)))
                            (map second)))
                     (apply gen/tuple (repeat (count all-keys) (gen/double* {:min 0.0, :max 1.0})))))))))]
      (printf "\n===============\n")
      (printf "%d records, %d updates, %d deletions\n"
              (count rkeys) (count ukeys) (count dkeys))
      (flush)
      (let [store (mdag/init-store :types graph/codec-types)
            params {::record/families families
                    ::index/fan-out fan-out
                    ::part/limit part-limit}
            records (map-indexed #(vector %2 {:a %1}) rkeys)
            updates (map-indexed #(vector %2 {:b %1}) ukeys)
            deletions (map vector dkeys (repeat ::patch/tombstone))
            changes (patch/patch-seq deletions updates)
            root (let [parts (timer "partition records"
                               (part/partition-records store params records))]
                   (timer "build tree"
                     (index/build-tree store params parts)))
            root' (timer "update tree"
                    (index/update-tree store params root (vec changes)))
            expected-data (patch/patch-seq changes records)]
        (is (= expected-data (index/read-all store root' nil)))
        (timer "check-asserts"
          (tu/check-asserts
            (validate/run!
              store
              (::node/id (meta root'))
              validate/validate-data-tree
              (assoc params ::record/count (count expected-data)))))
        (printf "---------------\n")
        (flush)))))
