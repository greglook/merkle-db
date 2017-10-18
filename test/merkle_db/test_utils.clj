(ns merkle-db.test-utils
  (:require
    [clojure.string :as str]
    [clojure.spec :as s]
    [clojure.test :as test]))


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


;; (is (invalid? s expr))
;; Asserts that the result of `expr` is not valid for spec `s`.
(defmethod test/assert-expr 'invalid?
  [msg form]
  `(let [spec# ~(second form)
         value# ~(nth form 2)
         conformed# (s/conform spec# value#)]
     (if (= ::s/invalid conformed#)
       (test/do-report
         {:type :pass
          :message ~msg,
          :expected '~('not (second form))
          :actual (s/explain-data spec# value#)})
       (test/do-report
         {:type :fail
          :message ~msg,
          :expected '~('not (second form))
          :actual conformed#}))
     conformed#))
