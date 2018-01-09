(ns movie-lens.movie
  (:require
    [clojure.spec.alpha :as s]
    [clojure.string :as str]))


;; Movie entity properties.
(s/def ::id integer?)
(s/def ::title string?)
(s/def ::genres (s/coll-of simple-keyword? :kind set?))


(s/def ::record
  (s/keys :req [::id ::title]
          :opt [::genres]))


(def table-parameters
  {:merkle-db.table/primary-key ::id
   :merkle-db.key/lexicoder :integer
   :merkle-db.index/fan-out 256
   :merkle-db.partition/limit 5000})


(def csv-header
  "movieId,title,genres")


(defn- sanitize-genre
  [genre-string]
  (-> genre-string
      (str/replace #"[^a-zA-Z0-9]+" "_")
      (str/lower-case)
      (keyword)))


(defn parse-row
  "Parse a row from the `movies.csv` data file."
  [[movie-id title genres]]
  {::id (Long/parseLong movie-id)
   ::title title
   ::genres (set (map sanitize-genre (str/split genres #"\|")))})
