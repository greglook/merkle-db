(ns merkle-db.validate
  "Validation framework for testing database MerkleDAG structures."
  (:require
    [clojure.future :refer [qualified-keyword?]]
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

;; Map representing a specific check on a node in the tree.
(s/def ::result
  (s/keys :req [::node/id
                ::type
                ::path
                ::state
                ::message]))


(def ^:dynamic *context*
  "Dynamically-bound validation context."
  nil)


(defmacro collect-results
  [& body]
  `(binding [*context* {::results (atom []), ::path []}]
     ~@body
     @(::results *context*)))


(defmacro checking-node
  [node-id & body]
  `(binding [*context* (assoc *context* ::node/id ~node-id)]
     ~@body))


(defmacro check-link
  [store link check-fn]
  `(let [link# ~link
         check# ~check-fn
         data# (mdag/get-data ~store link#)]
     (binding [*context* (-> *context*
                             (assoc ::node/id (::link/target link#))
                             (update ::path conj (::link/name link#)))]
       (check# data#))))


(defn report!
  "Record a validation result. The function accepts a multihash node id, a
  keyword validation type, a result key (:pass, :warn, :fail, or :error) and a
  message string."
  [type-key state message]
  (when-let [results (::results *context*)]
    (swap! results
           conj
           {::node/id (::node/id *context*)
            ::path (::path *context*)
            ::type type-key
            ::state state
            ::message message})
    nil))


(defmacro check
  ([type-key test-form]
   `(check ~type-key ~test-form (pr-str (list 'not '~test-form))))
  ([type-key test-form message]
   `(check ~type-key ~test-form ~message :fail))
  ([type-key test-form message bad-state]
   `(let [type-key# ~type-key]
      (try
        (let [condition# ~test-form
              state# (if condition# :pass ~bad-state)]
          (report! type-key# state# ~message)
          condition#)
        (catch Throwable t#
          (report! type-key# :error
                   (str "Error checking assertion "
                        (pr-str '~test-form) ": "
                        (.getName (class t#)) " "
                        (.getMessage t#)))
          nil)))))
