(ns movie-lens.movie
  (:require
    [clojure.spec.alpha :as s]
    [clojure.string :as str]))


;; Movie entity properties.
(s/def ::id integer?)
(s/def ::title string?)
(s/def ::year integer?)
(s/def ::genres (s/coll-of simple-keyword? :kind set?))

;; External identifiers.
(s/def :imdb.movie/id integer?)
(s/def :tmdb.movie/id integer?)


(s/def ::record
  (s/keys :req [::id
                ::title]
          :opt [::year
                ::genres
                :imdb.movie/id
                :tmdb.movie/id]))


(def table-parameters
  {:merkle-db.table/name "movies"
   :merkle-db.table/primary-key ::id
   :merkle-db.key/lexicoder :integer
   :merkle-db.index/fan-out 256
   :merkle-db.partition/limit 5000
   :merkle-db.record/families {:links #{:imdb.movie/id
                                        :tmdb.movie/id}}})


(def movies-csv-header
  "movieId,title,genres")


(def links-csv-header
  "movieId,imdbId,tmdbId")


(defn- sanitize-genre
  [genre-string]
  (-> genre-string
      (str/replace #"[^a-zA-Z0-9]+" "_")
      (str/lower-case)
      (keyword)))


(defn- parse-title-year
  [raw-title]
  (if-let [[title year-str] (next (re-find #"^(.+) \((\d+)\)$" raw-title))]
    [title (Integer/parseInt year-str)]
    [raw-title nil]))


(defn parse-movies-row
  "Parse a row from the `movies.csv` data file."
  [[movie-id raw-title genres]]
  (let [[title year] (parse-title-year raw-title)
        genres (set (map sanitize-genre (str/split genres #"\|")))]
    (cond-> {::id (Long/parseLong movie-id)
             ::title title}
      year
        (assoc ::year year)
      (seq genres)
        (assoc ::genres genres))))


(defn parse-links-row
  "Parse a row from the `links.csv` data file."
  [[movie-id imdb-id tmdb-id]]
  (cond-> {::id (Long/parseLong movie-id)}
    (not (str/blank? imdb-id))
      (assoc :imdb.movie/id (Long/parseLong imdb-id))
    (not (str/blank? tmdb-id))
      (assoc :tmdb.movie/id (Long/parseLong tmdb-id))))
