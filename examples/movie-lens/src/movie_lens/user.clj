(ns movie-lens.user
  (:require
    [clojure.spec.alpha :as s]))


;; User attributes.
(s/def ::id integer?)
