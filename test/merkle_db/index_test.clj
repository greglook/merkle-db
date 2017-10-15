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
         ~'idxB (index/build-tree store# params [~'part0 ~'part1 ~'part2])
         ~'idxC (index/build-tree store# params [~'part3 ~'part4])
         ~'idxA (index/build-tree store# params [~'idxB ~'idxC])]
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
      (is (= index/data-type (:data/type idxA)))
      (is (= 2 (::index/height idxA)))
      (is (= 2 (count (::index/children idxA))))
      (is (= 19 (::record/count idxA)))
      (is (= (nth-key 4) (::record/first-key idxA)))
      (is (= (nth-key 32) (::record/last-key idxA))))
    (testing "read-all"
      (is (thrown? Exception
            (index/read-all store {:data/type :foo} nil)))
      (is (= (records 4 5 6 7 8 10 11 12 13 14 17 18 21 23 24 25 30 31 32)
             (index/read-all store idxA nil)))
      (is (= [[(nth-key 5) {:c 25}]
              [(nth-key 10) {:c 30}]
              [(nth-key 25) {:c 45}]
              [(nth-key 30) {:c 50}]]
             (index/read-all store idxA #{:c}))))
    (testing "read-batch"
      (is (thrown? Exception
            (index/read-batch store {:data/type :foo} nil nil)))
      (is (= (records 5 8 23)
             (index/read-batch
               store idxA nil
               #{(nth-key 8) (nth-key 5) (nth-key 23) (nth-key 80)})))
      (is (= [[(nth-key 12) {:b 88}]
              [(nth-key 21) {:b 79}]]
             (index/read-batch
               store idxA #{:b}
               #{(nth-key 12) (nth-key 21) (nth-key 22)}))))
    (testing "read-range"
      (is (thrown? Exception
            (index/read-range store {:data/type :foo} nil nil nil)))
      (is (= (records 4 5 6 7 8 10 11 12 13 14 17 18 21 23 24 25 30 31 32)
             (index/read-range store idxA nil nil nil)))
      (is (= [[(nth-key  6) {:b 94}]
              [(nth-key 12) {:b 88}]
              [(nth-key 18) {:b 82}]
              [(nth-key 21) {:b 79}]
              [(nth-key 24) {:b 76}]
              [(nth-key 30) {:b 70}]]
             (index/read-range store idxA #{:b} nil nil)))
      (is (= (records 4 5 6 7 8 10)
             (index/read-range store idxA nil nil (nth-key 10))))
      (is (= (records 21 23 24 25 30 31 32)
             (index/read-range
               store idxA nil
               (nth-key 20) nil)))
      (is (= [[(nth-key  5) {:c 25}]
              [(nth-key 10) {:c 30}]
              [(nth-key 25) {:c 45}]
              [(nth-key 30) {:c 50}]]
             (index/read-range store idxA #{:c} (nth-key 5) (nth-key 30)))))))


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
    (is (identical? idxA (index/update-tree store params idxA [])))
    ; TODO: identical? would be a stronger guarantee here
    (is (= idxA (index/update-tree store params idxA
                                   (tombstones 0))))
    (is (= idxA (index/update-tree store params idxA
                                   (records 5 10 14 23 30))))))


(deftest index-tree-insert-2-parts
  (with-index-fixture
    (let [root (index/update-tree store params idxA
                                  (records 0 1 2 3 9 15 16))]
      (is-index root 2 2 26 0 32)
      (is (= (records 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 21 23
                      24 25 30 31 32)
             (index/read-all store root nil)))
      (is-index (nth-child store root 0) 1 4 19 0 18)
      (is-index (nth-child store root 1) 1 2 7 21 32))))


(deftest index-tree-remove-part-from-B
  (with-index-fixture
    (let [root (index/update-tree store params idxA
                                  (tombstones 7 8 10 11))]
      (is-index root 2 2 15 4 32)
      (is (= (records 4 5 6 12 13 14 17 18 21 23 24 25 30 31 32)
             (index/read-all store root nil)))
      (let [lchild (nth-child store root 0)]
        (is-index lchild 1 2 8 4 18)
        (is (= part0 (nth-child store lchild 0)))
        (is (= part2 (nth-child store lchild 1))))
      (is (= idxC (nth-child store root 1))))))


(deftest index-tree-underflow-first-part-in-B
  (with-index-fixture
    (let [root (index/update-tree store params idxA
                                  (tombstones 6))]
      (is-index root 2 2 18 4 32)
      (is (= (records 4 5 7 8 10 11 12 13 14 17 18 21 23 24 25 30 31 32)
             (index/read-all store root nil)))
      (let [lchild (nth-child store root 0)]
        (is-index lchild 1 3 11 4 18)
        (is (= part2 (nth-child store lchild 2))))
      (is (= idxC (nth-child store root 1))))))


(deftest index-tree-underflow-last-part-in-B
  (with-index-fixture
    (let [root (index/update-tree store params idxA
                                  (tombstones 12 14 18))]
      (is-index root 2 2 16 4 32)
      (is (= (records 4 5 6 7 8 10 11 13 17 21 23 24 25 30 31 32)
             (index/read-all store root nil)))
      (let [lchild (nth-child store root 0)]
        (is-index lchild 1 3 9 4 17)
        (is (= part0 (nth-child store lchild 0))))
      (is (= idxC (nth-child store root 1))))))


(deftest index-tree-carry-part-to-C
  (with-index-fixture
    (let [root (index/update-tree store params idxA
                                  (tombstones 4 5 6 12 13 14 17 18))]
      (is-index root 1 3 11 7 32)
      (is (= (records 7 8 10 11 21 23 24 25 30 31 32)
             (index/read-all store root nil)))
      (is (= part1 (nth-child store root 0)))
      (is (= part3 (nth-child store root 1)))
      (is (= part4 (nth-child store root 2))))))


(deftest index-tree-carry-records-to-C
  (with-index-fixture
    (let [root (index/update-tree store params idxA
                                  (tombstones 4 5 6 7 10 11 12 13 17 18))]
      (is-index root 1 3 9 8 32)
      (is (= (records 8 14 21 23 24 25 30 31 32)
             (index/read-all store root nil)))
      (is (= part4 (nth-child store root 2))))))


(deftest index-tree-delete-subtree-B
  (with-index-fixture
    (let [root (index/update-tree store params idxA
                                  (tombstones 4 5 6 7 8 10 11 12 13 14 17 18))]
      (is (= idxC root)))))


(deftest index-tree-delete-subtree-C
  (with-index-fixture
    (let [root (index/update-tree store params idxA
                                  (tombstones 21 23 24 25 30 31 32))]
      (is (= idxB root)))))


(deftest index-tree-delete-to-part
  (with-index-fixture
    (let [root (index/update-tree store params idxA
                                  (tombstones 4 5 6
                                              7 8 10 11
                                              12 13 14 17 18
                                              30 31 32))]
      (is (= part3 root)))))


(deftest index-tree-delete-to-underflow
  (with-index-fixture
    (let [root (index/update-tree store params idxA
                                  (tombstones 4 5 6
                                              7 8 10 11
                                              12 13 14 17 18
                                              21 25
                                              30 31 32))]
      (is (= part/data-type (:data/type root)))
      (is (= 2 (::record/count root)))
      (is (= (nth-key 23) (::record/first-key root)))
      (is (= (nth-key 24) (::record/last-key root)))
      (is (= (records 23 24)
             (index/read-all store root nil))))))


#_
(deftest index-tree-carry-back-part
  (with-index-fixture
    (let [root (index/update-tree store params idxA
                                  (tombstones 21 23 24 25))]
      (is-index root 1 4 9 8 32)
      (is (= (records 4 5 6 7 8 10 11 12 13 14 17 18 30 31 32)
             (index/read-all store root nil)))
      #_(is (= part4 (nth-child store root 2))))))


#_
(deftest index-tree-carry-back-records
  (with-index-fixture
    (let [root (index/update-tree store params idxA
                                  (tombstones 21 23 24 25 31))]
      (is-index root 1 3 9 8 32)
      (is (= (records 8 14 21 23 24 25 30 31 32)
             (index/read-all store root nil)))
      (is (= part4 (nth-child store root 2))))))


(deftest index-tree-delete-all
  (with-index-fixture
    (let [root (index/update-tree store params idxA
                                  (tombstones 4 5 6 7 8 10 11 12 13 14 17 18
                                              21 23 24 25 30 31 32))]
      (is (nil? root)))))


(deftest index-tree-adopt-subtree
  (with-index-fixture
    ;;         ..W..
    ;;        /     \
    ;;       A       X
    ;;      / \     / \
    ;;     B   C   Y   Z
    ;;    /|\ / \ / \ / \
    ;;    012 3 4 5 6 7 8
    ;; =>
    ;;       ..X'.
    ;;      /  |  \
    ;;     C   Y   Z
    ;;    / \ / \ / \
    ;;    3 4 5 6 7 8
    (let [part5 (part/from-records store params (records 35 36 37))
          part6 (part/from-records store params (records 40 42 44))
          part7 (part/from-records store params (records 45 46 49))
          part8 (part/from-records store params (records 51 52 53))
          idxY (index/build-tree store params [part5 part6])
          idxZ (index/build-tree store params [part7 part8])
          idxX (index/build-tree store params [idxY idxZ])
          idxW (index/build-tree store params [idxA idxX])
          root (index/update-tree store params idxW
                                  (tombstones
                                    4 5 6
                                    7 8 10 11
                                    12 13 14 17 18))]
      (is-index root 2 3 19 21 53)
      (is (= idxC (nth-child store root 0)))
      (is (= idxY (nth-child store root 1)))
      (is (= idxZ (nth-child store root 2))))))



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
