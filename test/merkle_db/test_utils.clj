(ns merkle-db.test-utils
  (:require
    [clojure.string :as str]
    [clojure.spec :as s]
    [clojure.test :as test]))


(defn check-spec
  "Internal implementation of `valid?` and `invalid?`."
  [msg spec-form spec value valid?]
  (let [conformed (s/conform spec value)
        explained (s/explain-data spec value)]
    (if (= valid? (= ::s/invalid conformed))
      (test/do-report
        {:type :fail
         :message msg
         :expected spec-form
         :actual explained})
      (test/do-report
        {:type :pass
         :message msg
         :expected spec-form
         :actual conformed}))
    conformed))


;; (is (valid? s expr))
;; Asserts that the result of `expr` is valid for spec `s`.
;; Returns the conformed value.
(defmethod test/assert-expr 'valid?
  [msg form]
  `(check-spec
     ~msg
     '~(second form)
     ~(second form)
     ~(nth form 2)
     true))


;; (is (invalid? s expr))
;; Asserts that the result of `expr` is not valid for spec `s`.
(defmethod test/assert-expr 'invalid?
  [msg form]
  `(check-spec
     ~msg
     '~(second form)
     ~(second form)
     ~(nth form 2)
     false))
