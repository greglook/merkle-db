(ns movie-lens.link
  (:require
    [clojure.spec.alpha :as s]
    [clojure.string :as str]
    [movie-lens.movie :as movie]))


;; External identifiers.
(s/def :imdb.movie/id integer?)
(s/def :tmdb.movie/id integer?)


(s/def ::record
  (s/keys :req [::movie/id]
          :opt [:imdb.movie/id
                :tmdb.movie/id]))


(def table-parameters
  {:merkle-db.table/primary-key ::movie/id
   :merkle-db.key/lexicoder :integer
   :merkle-db.index/fan-out 256
   :merkle-db.partition/limit 5000})


(def csv-header
  "movieId,imdbId,tmdbId")


(defn parse-row
  "Parse a row from the `links.csv` data file."
  [[movie-id imdb-id tmdb-id]]
  {::movie/id (Long/parseLong movie-id)
   :imdb.movie/id (when-not (str/blank? imdb-id)
                    (Long/parseLong imdb-id))
   :tmdb.movie/id (when-not (str/blank? tmdb-id)
                    (Long/parseLong tmdb-id))})
