(ns movie-lens.dataset
  "Functions for parsing and loading the MovieLens dataset source files."
  (:require
    [clojure.data.csv :as csv]
    [clojure.java.io :as io]
    [clojure.string :as str]
    [sparkling.core :as spark])
  (:import
    java.time.Instant))


(defn- csv-rdd
  "Create an RDD from the given CSV file."
  [spark-ctx csv-file header parser]
  (->> (spark/text-file spark-ctx (str csv-file) #_4)
       (spark/filter (fn remove-header [line] (not= line header)))
       (spark/flat-map csv/read-csv)
       (spark/map parser)))


(defn- parse-movie-row
  "Parse a row from the movies data file."
  [[movie-id title genres]]
  {:movie/id (Long/parseLong movie-id)
   :movie/title title
   :movie/genres (set (map (comp keyword str/lower-case #(str/replace % #"[^a-zA-Z0-9]+" "_"))
                           (str/split genres #"\|")))})


(defn load-movies
  "Load the `movies.csv` file into a Spark RDD."
  [spark-ctx dataset-dir]
  (csv-rdd spark-ctx
           (io/file dataset-dir "movies.csv")
           "movieId,title,genres"
           parse-movie-row))


(defn- parse-link-row
  "Parse a row from the `links.csv` data file."
  [[movie-id imdb-id tmdb-id]]
  {:movie/id (Long/parseLong movie-id)
   :imdb/id (Long/parseLong imdb-id)
   :tmdb/id (Long/parseLong tmdb-id)})


(defn load-links
  "Load the `links.csv` file into a Spark RDD."
  [spark-ctx dataset-dir]
  (csv-rdd spark-ctx
           (io/file dataset-dir "links.csv")
           "movieId,imdbId,tmdbId"
           parse-link-row))


(defn parse-rating-row
  "Parse a row from the `ratings.csv` data file."
  [[user-id movie-id rating timestamp]]
  {:user/id (Long/parseLong user-id)
   :movie/id (Long/parseLong movie-id)
   :movie.rating/score (Double/parseDouble rating)
   :movie.rating/time (Instant/ofEpochSecond (Long/parseLong timestamp))})


(defn load-tags
  "Load the `ratings.csv` file into a Spark RDD."
  [spark-ctx dataset-dir]
  (csv-rdd spark-ctx
           (io/file dataset-dir "ratings.csv")
           "userId,movieId,rating,timestamp"
           parse-rating-row))


(defn parse-tag-row
  "Parse a row from the `tags.csv` data file."
  [[user-id movie-id tag timestamp]]
  {:user/id (Long/parseLong user-id)
   :movie/id (Long/parseLong movie-id)
   :movie.tag/value tag
   :movie.tag/time (Instant/ofEpochSecond (Long/parseLong timestamp))})


(defn load-tags
  "Load the `tags.csv` file into a Spark RDD."
  [spark-ctx dataset-dir]
  (csv-rdd spark-ctx
           (io/file dataset-dir "tags.csv")
           "userId,movieId,tag,timestamp"
           parse-link-row))
