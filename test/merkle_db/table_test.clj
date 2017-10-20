(ns merkle-db.table-test
  (:require
    [clojure.set :as set]
    [clojure.test :refer :all]
    [clojure.test.check.generators :as gen]
    [com.gfredericks.test.chuck.generators :as tcgen]
    [merkledag.core :as mdag]
    [merkle-db.graph :as graph]
    [merkle-db.key :as key]
    [merkle-db.patch :as patch]
    [merkle-db.record :as record]
    [merkle-db.table :as table]
    [merkle-db.test-utils :as tu]
    [test.carly.core :as carly :refer [defop]]))


(deftest record-updating-fn
  (let [record-updater @#'table/record-updater]
    (testing "default behavior"
      (let [merge-simple (record-updater {})]
        (is (= {:a "abc", :b 4}
               (merge-simple nil {:a "abc", :c true} {:b 4, :c nil})))))
    (testing "explicit merge-record"
      (let [merge-record (fn custom-merge [rk l r] r)]
        (is (identical? merge-record (record-updater {:merge-record merge-record}))
            "function should be used directly"))
      (is (thrown? IllegalArgumentException
            (record-updater {:merge-record "not a function"})
            "non-function should be illegal")))
    (testing "merge-field"
      (testing "with map of update functions"
        (let [merge-fields (record-updater {:merge-field {:a +, :c conj}})]
          (is (= {:a 3} (merge-fields nil {:a 1} {:a 2})))
          (is (= {:c #{"foo" "bar"}}
                 (merge-fields nil {:c #{"bar"}} {:c "foo"})))
          (is (= {:b 3} (merge-fields nil {:d 1} {:b 3, :d nil})))
          (is (= {:a 468, :b 4, :c ["abc"]}
                 (merge-fields nil
                               {:a 123, :b 1, :c []}
                               {:a 345, :b 4, :c "abc"})))))
      (testing "with custom function"
        (let [merge-fields (record-updater
                             {:merge-field (fn [fk l r]
                                             (case fk
                                               :a (+ l r)
                                               :c (conj l r)
                                               :d (when-not (= r "foo") r)
                                               (* r 2)))})]
          (is (= {:a 3} (merge-fields nil {:a 1} {:a 2})))
          (is (= {:d "bar"} (merge-fields nil {} {:d "bar"})))
          (is (= {:b 6} (merge-fields nil {:d "abc"} {:b 3, :d nil})))
          (is (= {:a 468, :b 8, :c ["abc"]}
                 (merge-fields nil
                               {:a 123, :b 1, :c []}
                               {:a 345, :b 4, :c "abc", :d "foo"})))))
      (is (thrown? IllegalArgumentException
            (record-updater {:merge-field "not a map or function"})
            "non-function should be illegal")))
    (testing "specifying both merge-record and merge-field"
      (is (thrown? IllegalArgumentException
            (record-updater {:merge-record (constantly nil)
                             :merge-field (constantly nil)}))
          "should be illegal"))))


(deftest record-filtering
  (let [changes [[(key/create [0 0]) {:a 10, :b 11, :c 12}]
                 [(key/create [1 0]) ::patch/tombstone]
                 [(key/create [1 1]) {:b 30, :c 32}]
                 [(key/create [2 0]) ::patch/tombstone]
                 [(key/create [3 0]) {:a 50, :b 51}]]]
    (is (= changes (@#'table/filter-records {} changes)))
    (is (= [[(key/create [3 0]) {:a 50, :b 51}]]
           (@#'table/filter-records {:min-key (key/create [2 5])} changes)))
    (is (= [[(key/create [0 0]) {:a 10, :b 11, :c 12}]
            [(key/create [1 0]) ::patch/tombstone]]
           (@#'table/filter-records {:max-key (key/create [1 0])} changes)))
    (is (= [[(key/create [0 0]) {:a 10}]
            [(key/create [1 0]) ::patch/tombstone]
            [(key/create [1 1]) {}]
            [(key/create [2 0]) ::patch/tombstone]
            [(key/create [3 0]) {:a 50}]]
           (@#'table/filter-records {:fields #{:a}} changes)))))



;; ## Table Operations

(defn- gen-range-query
  [ctx]
  (let [n+ (+ (:n ctx) 10)]
    (gen/bind
      (gen/hash-map
        :fields (gen/fmap not-empty (gen/set (gen/elements #{:a :b :c :d :e})))
        :min-key (gen/large-integer* {:min -10, :max n+})
        :max-key (gen/large-integer* {:min -10, :max n+})
        ; TODO: :reverse gen/boolean
        :offset (gen/large-integer* {:min 0, :max n+})
        :limit (gen/large-integer* {:min 1, :max n+}))
      tcgen/sub-map)))


(defn- scan-range
  [model query]
  (cond->> (seq model)
    (seq (:fields query))
      (keep (fn [[k r]]
              (let [r' (select-keys r (:fields query))]
                (when (seq r')
                  [k r']))))
    (:min-key query)
      (drop-while #(< (first %) (:min-key query)))
    (:max-key query)
      (take-while #(<= (first %) (:max-key query)))
    (:reverse query)
      (reverse)
    (:offset query)
      (drop (:offset query))
    (:limit query)
      (take (:limit query))))


(defop Keys
  [query]

  (gen-args
    [ctx]
    [(gen/fmap
       #(dissoc % :fields)
       (gen-range-query ctx))])

  (apply-op
    [this table]
    (doall (table/keys @table query)))

  (check
    [this model result]
    (let [expected (map first (scan-range model query))]
      (is (= expected result)))))


(defop Scan
  [query]

  (gen-args
    [ctx]
    [(gen-range-query ctx)])

  (apply-op
    [this table]
    (doall (table/scan @table query)))

  (check
    [this model result]
    (let [expected (scan-range model query)]
      (is (= (or expected []) result)))))


(defop Read
  [ids fields]

  (gen-args
    [ctx]
    [(gen/set (gen/large-integer* {:min 1, :max (:n ctx)})
              {:max-elements (:n ctx)})
     (gen/fmap not-empty (gen/set (gen/elements #{:a :b :c :d :e})))])

  (apply-op
    [this table]
    (table/read @table ids {:fields fields}))

  (check
    [this model result]
    (let [expected (->> ids
                        (map (juxt identity model))
                        (map (fn [[k r]]
                               (if (seq fields)
                                 [k (select-keys r fields)]
                                 [k r])))
                        (filter (comp seq second))
                        (sort-by first))]
      (is (= expected result)))))


(defop Insert
  [records]

  (gen-args
    [ctx]
    [(gen/fmap
       ; TODO: more sophisticated value generation
       (fn [ids] (mapv #(vector % {:a %, :c %}) ids))
       (gen/set (gen/large-integer* {:min 1, :max (:n ctx)})))])

  (apply-op
    [this table]
    (swap! table table/insert records))

  (check
    [this model result]
    (is (valid? ::table/node-data result))
    (let [extant (set (keys model))
          rkeys (map first records)
          added (set/difference (set rkeys) extant)]
      (is (= (+ (count model) (count added))
             (::record/count result)))))

  (update-model
    [this model]
    (into model records)))


(defop Delete
  [rkeys]

  (gen-args
    [ctx]
    [(gen/set (gen/large-integer* {:min 1, :max (:n ctx)}))])

  (apply-op
    [this table]
    (swap! table table/delete rkeys))

  (check
    [this model result]
    (is (valid? ::table/node-data result))
    (let [extant (set (keys model))
          removed (set/intersection extant rkeys)]
      (is (= (- (count model) (count removed))
             (::record/count result)))))

  (update-model
    [this model]
    (apply dissoc model rkeys)))


(defop Flush!
  []

  (apply-op
    [this table]
    (swap! table table/flush!))

  (check
    [this model result]
    (is (valid? ::table/node-data result))))


(def ^:private op-generators
  (juxt gen->Keys
        gen->Scan
        gen->Read
        gen->Insert
        gen->Delete
        gen->Flush!))


(def gen-context
  "Generator for test contexts; this gives the set of possible keys to use in
  operations."
  (gen/hash-map
    :n (gen/large-integer* {:min 1, :max 100})))


(deftest ^:generative table-behavior
  (let [store (mdag/init-store :types graph/codec-types)]
    (carly/check-system "table behavior" 100
      #(atom (table/bare-table
               store "test-data"
               {:merkle-db.index/fan-out 4
                :merkle-db.partition/limit 5
                :merkle-db.patch/limit 10
                :merkle-db.record/families {:bc #{:b :c}}
                :merkle-db.key/lexicoder :integer}))
      op-generators
      :context-gen gen-context
      :init-model (constantly (sorted-map))
      :concurrency 1
      :repetitions 1
      :report
      {:puget {:print-handlers tu/print-handler}})))
