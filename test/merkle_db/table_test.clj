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
      (let [update-simple (record-updater {})]
        (is (= {:a "abc", :b 4, :c nil}
               (update-simple nil {:a "abc", :c true} {:b 4, :c nil})))))
    (testing "explicit update-record"
      (let [update-record (fn custom-merge [rk l r] r)]
        (is (identical? update-record (record-updater {:update-record update-record}))
            "function should be used directly"))
      (is (thrown? IllegalArgumentException
            (record-updater {:update-record "not a function"})
            "non-function should be illegal")))
    (testing "update-field"
      (testing "with map of update functions"
        (let [update-fields (record-updater {:update-field {:a +, :c conj}})]
          (is (= {:a 3}
                 (update-fields nil {:a 1} {:a 2})))
          (is (= {:c #{"foo" "bar"}}
                 (update-fields nil {:c #{"bar"}} {:c "foo"})))
          (is (= {:b 3, :d nil}
                 (update-fields nil {:d 1} {:b 3, :d nil})))
          (is (= {:a 468, :b 4, :c ["abc"]}
                 (update-fields nil
                                {:a 123, :b 1, :c []}
                                {:a 345, :b 4, :c "abc"})))))
      (testing "with custom function"
        (let [update-fields (record-updater
                              {:update-field (fn [fk l r]
                                               (case fk
                                                 :a (+ l r)
                                                 :c (conj l r)
                                                 :d (when-not (= r "foo") r)
                                                 (* r 2)))})]
          (is (= {:a 3}
                 (update-fields nil {:a 1} {:a 2})))
          (is (= {:d "bar"}
                 (update-fields nil {} {:d "bar"})))
          (is (= {:b 6, :d nil}
                 (update-fields nil {:d "abc"} {:b 3, :d nil})))
          (is (= {:a 468, :b 8, :c ["abc"], :d nil}
                 (update-fields nil
                                {:a 123, :b 1, :c []}
                                {:a 345, :b 4, :c "abc", :d "foo"})))))
      (is (thrown? IllegalArgumentException
            (record-updater {:update-field "not a map or function"})
            "non-function should be illegal")))
    (testing "specifying both update-record and update-field"
      (is (thrown? IllegalArgumentException
            (record-updater {:update-record (constantly nil)
                             :update-field (constantly nil)}))
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

(defn- gen-some-map
  [& kvs]
  (gen/bind (apply gen/hash-map kvs) tcgen/sub-map))


(defn- gen-fields
  [ctx]
  (gen/fmap not-empty (gen/set (gen/elements #{:a :b :c :d :e}))))


(defn- gen-id
  [ctx]
  (gen/large-integer* {:min 1, :max (:n ctx)}))


(defn- gen-ids
  [ctx]
  (gen/set (gen-id ctx) {:max-elements (:n ctx)}))


(defn- gen-record
  [ctx]
  (gen/fmap
    (fn [[id data]]
      (assoc data :id id))
    (gen/tuple
      (gen-id ctx)
      (gen-some-map
        :a gen/large-integer
        :b gen/boolean
        :c (gen/scale #(/ % 10) gen/string)
        :d (gen/double* {:NaN? false, :infinite? false})))))


(defn- gen-range-query
  [ctx]
  (let [n+ (+ (:n ctx) 10)]
    (gen-some-map
      :fields (gen-fields ctx)
      :min-key (gen/large-integer* {:min -10, :max n+})
      :max-key (gen/large-integer* {:min -10, :max n+})
      ; TODO: :reverse gen/boolean
      :offset (gen/large-integer* {:min 0, :max n+})
      :limit (gen/large-integer* {:min 1, :max n+}))))


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
      (take (:limit query))
    true
      (map (fn [[k r]] (assoc r :id k)))))


(defop Keys
  [query]

  (gen-args
    [ctx]
    [(gen/fmap
       #(dissoc % :fields)
       (gen-range-query ctx))])

  (apply-op
    [this table]
    (doall (if (seq query)
             (table/keys @table query)
             (table/keys @table))))

  (check
    [this model result]
    (let [expected (map :id (scan-range model query))]
      (is (= expected result)))))


(defop Scan
  [query]

  (gen-args
    [ctx]
    [(gen-range-query ctx)])

  (apply-op
    [this table]
    (doall (if (seq query)
             (table/scan @table query)
             (table/scan @table))))

  (check
    [this model result]
    (let [expected (scan-range model query)]
      (is (= expected result)))))


(defop Read
  [ids fields]

  (gen-args
    [ctx]
    [(gen-ids ctx) (gen-fields ctx)])

  (apply-op
    [this table]
    (if (seq fields)
      (table/read @table ids {:fields fields})
      (table/read @table ids)))

  (check
    [this model result]
    (let [expected (->> ids
                        (keep (partial find model))
                        (keep
                          (if (seq fields)
                            (fn [[k r]]
                              (when-let [r' (not-empty (select-keys r fields))]
                                [k r']))
                            identity))
                        (sort-by first)
                        (map (fn [[k r]] (assoc r :id k))))]
      (is (= expected result)))))


(defn- model-insert
  "Models an insertion into the database."
  [model records]
  (reduce
    (fn [db r]
      (let [k (:id r)
            v (dissoc r :id)]
        (update db k merge v)))
    model records))


(defop Insert
  [records]

  (gen-args
    [ctx]
    [(gen/vector (gen-record ctx))])

  (apply-op
    [this table]
    (swap! table table/insert records))

  (check
    [this model result]
    (is (valid? ::table/node-data result))
    (let [model' (model-insert model records)]
      (is (= (count model') (::record/count result)))))

  (update-model
    [this model]
    (model-insert model records)))


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
  [opts]

  (gen-args
    [ctx]
    [(gen-some-map :apply-patch? gen/boolean)])

  (apply-op
    [this table]
    (if (seq opts)
      (swap! table table/flush! opts)
      (swap! table table/flush!)))

  (check
    [this model result]
    (is (valid? ::table/node-data result))))


(defop Assoc
  [k v]

  (gen-args
    [ctx]
    [(gen/elements (:table-attrs ctx))
     (gen/one-of [gen/boolean gen/large-integer gen/keyword])])

  (apply-op
    [this table]
    (swap! table assoc k v))

  (check
    [this model result]
    (is (valid? ::table/node-data result))
    (is (= v (get result k)))))


(defop Dissoc
  [k]

  (gen-args
    [ctx]
    [(gen/elements (:table-attrs ctx))])

  (apply-op
    [this table]
    (swap! table dissoc k))

  (check
    [this model result]
    (is (valid? ::table/node-data result))
    (is (not (contains? result k)))))


(def ^:private op-generators
  (juxt gen->Keys
        gen->Scan
        gen->Read
        gen->Insert
        gen->Delete
        gen->Flush!
        gen->Assoc
        gen->Dissoc))


(def gen-context
  "Generator for test contexts; this gives the set of possible keys to use in
  operations."
  (gen/bind
    (gen/large-integer* {:min 1, :max 100})
    (fn [n]
      (gen/hash-map
        :n (gen/return n)
        :table-attrs (gen/set gen/keyword-ns {:min-elements 1})
        :records (gen/fmap
                   #(into (sorted-map) (map (juxt :id identity)) %)
                   (gen/vector (gen-record {:n n})))))))


(deftest ^:generative table-behavior
  (let [store (mdag/init-store :types graph/codec-types)]
    (carly/check-system "table behavior" 100
      (fn init-system
        [ctx]
        (->
          (table/bare-table
            store "test-data"
            {:merkle-db.index/fan-out 5
             :merkle-db.partition/limit 5
             :merkle-db.patch/limit 10
             :merkle-db.record/families {:bc #{:b :c}}
             :merkle-db.table/primary-key :id
             :merkle-db.key/lexicoder :integer})
          (table/insert (vals (:records ctx)))
          (table/flush!)
          (atom)))
      op-generators
      :context-gen gen-context
      :init-model :records
      :concurrency 1
      :repetitions 1
      :report
      {:puget {:print-handlers tu/print-handler}})))
