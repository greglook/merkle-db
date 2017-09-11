(ns merkle-db.test-utils
  (:require
    [clojure.string :as str]
    [clojure.spec :as s]
    [clojure.test :as test]
    [merkledag.node :as node]
    [merkle-db.validate :as validate]
    [multihash.core :as multihash]))


;; (is (valid? s expr))
;; Asserts that the result of `expr` is valid for spec `s`.
;; Returns the conformed value.
(defmethod test/assert-expr 'valid?
  [msg form]
  `(let [spec# ~(second form)
         value# ~(nth form 2)
         conformed# (s/conform spec# value#)]
     (if (= ::s/invalid conformed#)
       (test/do-report
         {:type :fail
          :message ~msg,
          :expected '~(second form)
          :actual (s/explain-data spec# value#)})
       (test/do-report
         {:type :pass
          :message ~msg,
          :expected '~(second form)
          :actual conformed#}))
     conformed#))


(defmacro check-asserts
  [results]
  `(doseq [[node-id# info#] ~results
           result# (::validate/results info#)]
     (test/do-report
       {:type (::validate/state result#)
        :message (format "Node %s (%s): %s"
                         (multihash/base58 (::node/id result#))
                         (str/join "/" (::validate/path result#))
                         (::validate/message result#))
        :expected (::validate/expected
                    result#
                    [(::validate/type result#)
                     (str/join \/ (::validate/path result#))
                     (::node/id result#)])
        :actual (::validate/actual result# (::validate/state result#))})))
