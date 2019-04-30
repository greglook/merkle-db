(ns movie-lens.dataset
  "Functions for parsing and loading the MovieLens dataset source files."
  (:require
    [clojure.data.csv :as csv]
    [clojure.java.io :as io]
    [clojure.tools.logging :as log]
    [merkle-db.database :as db]
    [merkle-db.spark.load :as msl]
    [merkle-db.table :as table]
    [merkle-db.tools.stats :as stats]
    [merkledag.node :as node]
    [movie-lens.movie :as movie]
    [movie-lens.rating :as rating]
    [movie-lens.tag :as tag]
    [movie-lens.util :as u]
    [multihash.core :as multihash]
    [sparkling.core :as spark]
    [sparkling.destructuring :as sde]))


(defn- csv-rdd
  "Create an RDD from the given CSV file."
  [spark-ctx parts csv-file header parser]
  (->> (spark/text-file spark-ctx (str csv-file) parts)
       (spark/filter (fn remove-header [line] (not= line header)))
       (spark/flat-map csv/read-csv)
       (spark/map parser)))


(defn- load-movies-table!
  [spark-ctx init-store dataset-dir]
  (let [elapsed (u/stopwatch)
        movies-path (str dataset-dir "movies.csv")
        links-path (str dataset-dir "links.csv")]
    (log/info "Loading movies data from" movies-path "and" links-path)
    (let [movies-csv (csv-rdd spark-ctx 4
                              movies-path
                              movie/movies-csv-header
                              movie/parse-movies-row)
          links-csv (csv-rdd spark-ctx 4
                             links-path
                             movie/links-csv-header
                             movie/parse-links-row)
          record-rdd (->> (spark/left-outer-join
                            (spark/key-by ::movie/id movies-csv)
                            (spark/key-by ::movie/id links-csv))
                          (spark/values)
                          (spark/map
                            (sde/fn [(movie ?links)]
                              (merge movie links))))
          table (msl/build-table!
                  init-store
                  movie/table-parameters
                  record-rdd)
          stats (stats/collect-table-stats table)]
      ;(u/pprint table)
      (stats/print-table-stats stats)
      (log/infof "Loaded movies table data (%s) in %s"
                 (multihash/base58 (::node/id table))
                 (u/duration-str @elapsed))
      table)))


(defn- load-tags-table!
  [spark-ctx init-store dataset-dir]
  (let [elapsed (u/stopwatch)
        csv-path (str dataset-dir "tags.csv")]
    (log/info "Loading tags data from" csv-path)
    (let [table (msl/build-table!
                  init-store
                  tag/table-parameters
                  (csv-rdd spark-ctx 8
                           csv-path
                           tag/csv-header
                           tag/parse-row))
          stats (stats/collect-table-stats table)]
      (stats/print-table-stats stats)
      (log/infof "Loaded tags table (%s) in %s"
                 (multihash/base58 (::node/id table))
                 (u/duration-str @elapsed))
      table)))


(defn- load-ratings-table!
  [spark-ctx init-store dataset-dir]
  (let [elapsed (u/stopwatch)
        csv-path (str dataset-dir "ratings.csv")]
    (log/info "Loading ratings data from" csv-path)
    (let [table (msl/build-table!
                  init-store
                  rating/table-parameters
                  (csv-rdd spark-ctx 64
                           csv-path
                           rating/csv-header
                           rating/parse-row))
          stats (stats/collect-table-stats table)]
      (stats/print-table-stats stats)
      (log/infof "Loaded ratings table (%s) in %s"
                 (multihash/base58 (::node/id table))
                 (u/duration-str @elapsed))
      table)))


(defn load-dataset!
  [spark-ctx init-store dataset-dir]
  (let [store (init-store)
        movies (load-movies-table! spark-ctx init-store dataset-dir)
        tags (load-tags-table! spark-ctx init-store dataset-dir)
        ratings (load-ratings-table! spark-ctx init-store dataset-dir)]
    (->
      (db/empty-db store {:data/title "MovieLens Dataset"})
      (db/set-table "movies" movies)
      (db/set-table "ratings" ratings)
      (db/set-table "tags" tags)
      (db/flush!))))
