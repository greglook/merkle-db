(ns merkle-db.validate
  "Validation framework for testing database MerkleDAG structures."
  (:refer-clojure :exclude [run!])
  (:require
    [clojure.future :refer [any? qualified-keyword?]]
    [clojure.set :as set]
    [clojure.spec.alpha :as s]
    [clojure.test :as test]
    [merkle-db.index :as index]
    [merkle-db.key :as key]
    [merkle-db.partition :as part]
    [merkle-db.record :as record]
    [merkle-db.tablet :as tablet]
    [merkledag.core :as mdag]
    [merkledag.link :as link]
    [merkledag.node :as node]
    [multihash.core :as multihash]))


; TODO: move to 'tools' tld?


;; Path from the validation root to the node being checked.
(s/def ::path string?)

;; Type of validation check performed.
(s/def ::type qualified-keyword?)

;; Validation state.
(s/def ::state #{:pass :warn :fail :error})

;; Validation message describing the check in a human-readable way.
(s/def ::message string?)

(s/def ::expected any?)
(s/def ::actual any?)

;; Map representing a specific check on a node in the tree.
(s/def ::result
  (s/keys :req [::type ::state ::message]
          :opt [::expected ::actual]))


(def ^:dynamic *context*
  "Dynamically-bound validation context."
  nil)


(defmacro collect-results
  [& body]
  `(binding [*context* {::results (atom [])
                        ::next (atom [])}]
     (let [value# (do ~@body)]
       {:value value#
        :results @(::results *context*)
        :next @(::next *context*)})))


(defn check-next!
  "Register a validation function to run against the linked node."
  [check-fn link params]
  (swap! (::next *context*)
         conj
         {::check check-fn
          ::node/id (:target link)
          ::link-name (:name link)
          ::params params}))


(defn report!
  "Record a validation result. The function accepts a multihash node id, a
  keyword validation type, a result key (:pass, :warn, :fail, or :error) and a
  message string."
  [type-key state message expected actual]
  (when-let [results (::results *context*)]
    (swap! results
           conj
           {::path (::path *context*)
            ::type type-key
            ::state state
            ::message message
            ::expected expected
            ::actual actual})
    nil))


(defmacro check
  ([type-key test-form message]
   `(check ~type-key ~test-form ~message :fail))
  ([type-key test-form message bad-state]
   `(try
      ~(cond
         ; Equality test.
         (and (list? test-form)
              (= '= (first test-form))
              (= 3 (count test-form)))
           `(let [expected# ~(nth test-form 1)
                  actual# ~(nth test-form 2)
                  condition# (= expected# actual#)
                  state# (if condition# :pass ~bad-state)]
              (report! ~type-key state# ~message expected# actual#)
              condition#)

         ; Comparison test.
         (and (list? test-form)
              (contains? #{'< '> '<= '>=} (first test-form))
              (= 3 (count test-form)))
           `(let [v0# ~(nth test-form 1)
                  v1# ~(nth test-form 2)
                  condition# (~(first test-form) v0# v1#)
                  state# (if condition# :pass ~bad-state)]
              (report! ~type-key state# ~message
                       '~test-form
                       (if condition#
                         condition#
                         (list '~'not (list '~(first test-form) v0# v1#))))
              condition#)

         ; Predicate test.
         (and (list? test-form)
              (= 2 (count test-form)))
           `(let [actual# ~(second test-form)
                  condition# (~(first test-form) actual#)
                  state# (if condition# :pass ~bad-state)]
              (report! ~type-key state# ~message
                       '~test-form
                       (if condition#
                         condition#
                         (list '~'not (list '~(first test-form) actual#))))
              condition#)

         :else
           `(let [condition# ~test-form
                  state# (if condition# :pass ~bad-state)]
              (report! ~type-key state# ~message '~test-form condition#)
              condition#))
      (catch Throwable t#
        (report! ~type-key :error
                 (str "Error checking assertion "
                      (pr-str '~test-form) ": "
                      (.getName (class t#)) " "
                      (.getMessage t#))
                 '~test-form t#)
        nil))))


(defn run!
  "Validate a full graph structure by walking the nodes and links with
  validation functions. Returns a map from node ids to information maps, which
  contain a `:paths` set and a `:results` vector with assertion result maps."
  [store root-id root-check params]
  (loop [checks (conj clojure.lang.PersistentQueue/EMPTY
                      {::path []
                       ::check root-check
                       ::node/id root-id
                       ::params params})
         results {}]
    (if-let [next-check (peek checks)]
      ; Process next check task.
      (let [node-id (::node/id next-check)
            path (::path next-check)
            data (mdag/get-data store node-id nil ::missing-node)]
        (if (identical? ::missing-node data)
          ; Node not found, add error result.
          (recur (pop checks)
                 (assoc results node-id
                        {::paths #{path}
                         ::results [{::type ::missing-node, ::status :error}]}))
          ; Run check function on node data.
          (let [check (::check next-check)
                ;_ (prn ::run-check check data (::params next-check))
                output (collect-results (check data (::params next-check)))]
            (recur (into (pop checks)
                         (map #(-> %
                                   (assoc ::path (conj path (::link-name %)))
                                   (dissoc ::link-name)))
                         (:next output))
                   (-> results
                       (update-in [node-id ::paths] (fnil conj #{}) path)
                       (update-in [node-id ::results] (fnil into []) (:results output)))))))
      ; No more checks, return result aggregate.
      results)))


(defmacro check-asserts
  [results]
  `(doseq [[node-id# info#] ~results
           result# (::results info#)]
     (test/do-report
       {:type (::state result#)
        :message (format "Node %s (%s): %s"
                         (multihash/base58 node-id#)
                         (str/join ", " (map (partial str/join "/") (::paths info#)))
                         (::message result#))
        :expected (::expected
                    result#
                    [(::type result#)
                     (str/join \/ (::path result#))
                     (::node/id result#)])
        :actual (::actual result# (::state result#))})))



;; ## Validation Functions

(defn validate-tablet
  [tablet params]
  (when (check :data/type
          (= :merkle-db/tablet (:data/type tablet))
          "Node has expected data type")
    (check ::spec
      (s/valid? ::tablet/node-data tablet)
      (s/explain-str ::tablet/node-data tablet))
    (check ::record/count
      (seq (tablet/read-all tablet))
      "Tablet should not be empty")
    (when-let [family-keys (get (::record/families params)
                                (::record/family-key params))]
      (let [bad-fields (->> (::records tablet)
                            (mapcat (comp clojure.core/keys second))
                            (remove (set family-keys))
                            (set))]
        (check ::record/families
          (empty? bad-fields)
          (format "Tablet record data should only contain values for fields in family %s (%s)"
                  (::record/family-key params)
                  family-keys))))
    (when-let [boundary (::record/first-key params)]
      (check ::record/first-key
        (not (key/before? (tablet/first-key tablet) boundary))
        "First key in partition is within the subtree boundary"))
    (when-let [boundary (::record/last-key params)]
      (check ::record/last-key
        (not (key/after? (tablet/last-key tablet) boundary))
        "Last key in partition is within the subtree boundary"))
    ; TODO: records are sorted by key
    ))


(defn validate-partition
  [part params]
  (when (check :data/type
          (= :merkle-db/partition (:data/type part))
          "Node has expected data type")
    (check ::spec
      (s/valid? ::part/node-data part)
      (s/explain-str ::part/node-data part))
    ; TODO: warn when partition limit or families don't match params
    (when (and (::part/limit params)
               (::record/count params)
               (<= (::part/limit params) (::record/count params)))
      (check ::part/underflow
        (<= (Math/ceil (/ (::part/limit params) 2)) (::record/count part))
        "Partition is at least half full if tree has at least :merkle-db.partition/limit records"))
    (check ::part/overflow
      (<= (::record/count part) (::part/limit params))
      "Partition has at most :merkle-db.partition/limit records")
    (when-let [boundary (::record/first-key params)]
      (check ::record/first-key
        (not (key/before? (::record/first-key part) boundary))
        "First key in partition is within the subtree boundary"))
    (when-let [boundary (::record/last-key params)]
      (check ::record/last-key
        (not (key/after? (::record/last-key part) boundary))
        "Last key in partition is within the subtree boundary"))
    (check ::base-tablet
      (:base (::part/tablets part))
      "Partition contains a base tablet")
    ; TODO: partition first-key matches actual first record key in base tablet
    ; TODO: partition last-key matches actual last record key in base tablet
    ; TODO: record/count is accurate
    ; TODO: every key present tests true against membership filter
    (doseq [[tablet-family link] (::part/tablets part)]
      (check-next!
        validate-tablet link
        (assoc params
               ::record/families (::record/families part)
               ::record/family-key tablet-family
               ::record/first-key (::record/first-key part)
               ::record/last-key (::record/last-key part))))))


(defn validate-index
  [index params]
  (when (check :data/type
          (= :merkle-db/index (:data/type index))
          "Node has expected data type")
    (check ::spec
      (s/valid? ::index/node-data index)
      (s/explain-str ::index/node-data index))
    (check ::index/keys
      (= (dec (count (::index/children index)))
         (count (::index/keys index)))
      "Index nodes have one fewer key than child links")
    (if (::index/root? params)
      (check ::index/fan-out
        (<= 2 (count (::index/children index)) (::index/fan-out params))
        "Root index node has at between [2, f] children")
      (check ::index/fan-out
        (<= (int (Math/ceil (/ (::index/fan-out params) 2)))
            (count (::index/children index))
            (::index/fan-out params))
        "Internal index node has between [ceil(f/2), f] children"))
    (check ::index/height
      (= (::index/height params) (::index/height index))
      "Index node has expected height")
    (doseq [[first-key child-link last-key]
              (map vector
                   (cons (::record/first-key params) (::index/keys index))
                   (::index/children index)
                   (conj (::index/keys index) (::record/last-key params)))
              :let [height' (dec (::index/height index))]]
      (check-next!
        (if (zero? height')
          validate-partition
          validate-index)
        child-link
        (assoc params
               ::index/root? false
               ::index/height height'
               ::record/first-key first-key
               ::record/last-key last-key)))))


(defn validate-data-tree
  [root params]
  (cond
    (zero? (::record/count params))
      (check ::index/empty
        (nil? root)
        "Empty tree has nil root")
    (or (< (::record/count params) (::part/limit params))
        (and (= (::record/count params) (::part/limit params))
             (= :merkle-db/partition (:data/type root))))
      (validate-partition root params)
    :else
      (validate-index
        root
        (assoc params
               ::index/root? true
               ::index/height (::index/height root)))))
