(ns movie-lens.dataset
  (:require
    [clojure.data.csv :as csv]
    [clojure.java.io :as io]
    [clojure.string :as str])
  (:import
    java.time.Instant))


(defn parse-movie-row
  [[id title genres]]
  {:movie/id (Long/parseLong id)
   :movie/title title
   :movie/genres (set (map (comp keyword str/lower-case #(str/replace % #"[^a-zA-Z0-9]+" "_"))
                           (str/split genres #"\|")))})


(defn parse-rating-row
  [[user-id movie-id rating timestamp]]
   {:user/id (Long/parseLong user-id)
    :movie/id (Long/parseLong movie-id)
    :rating/score (Double/parseDouble rating)
    :rating/time (Instant/ofEpochSecond (Long/parseLong timestamp))})
