(ns merkle-db.validate
  "Validation framework for testing database MerkleDAG structures."
  (:require
    [clojure.future :refer [any? qualified-keyword?]]
    [clojure.set :as set]
    [clojure.spec :as s]
    [merkledag.core :as mdag]
    [merkledag.link :as link]
    [merkledag.node :as node]))


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
          ::link link
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

         ; Predicate test.
         (and (list? test-form)
              (= 2 (count test-form)))
           `(let [actual# ~(second test-form)
                  condition# (~(first test-form) actual#)
                  state# (if condition# :pass ~bad-state)]
              (report! ~type-key state# ~message
                       '~test-form
                       (list ~'not (list '~(first test-form) actual#)))
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


(defn validate!
  [store root-check root-id params]
  (loop [checks (conj clojure.lang.PersistentQueue/EMPTY
                      {::path []
                       ::check root-check
                       ::node/id root-id
                       ::params params})
         results {}]
    (if-let [next-check (first checks)]
      (let [data (mdag/get-data store (::node/id next-check) ::missing-node)]
        (if (identical? ::missing-node data)
          ; Node not found
          ; TODO: add missing-node warning
          (recur (next checks) results)
          ; Run check function.
          (let [check (::check next-check)
                output (collect-results (check data (::params next-check)))]
            (recur (into (next checks)
                         (map #(-> %
                                   (assoc ::path (conj (::path next-check) (:name (::link %))))
                                   (dissoc ::link)))
                         (:next output))
                   (-> results
                       (update-in [(::node/id next-check) ::paths] (fnil conj #{}) (::path next-check))
                       (update-in [(::node/id next-check) ::results (fnil into []) (:results output)]))))))
      results)))
