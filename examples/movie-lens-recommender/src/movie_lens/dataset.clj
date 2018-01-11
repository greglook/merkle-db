(ns movie-lens.dataset
  "Functions for parsing and loading the MovieLens dataset source files."
  (:require
    [clojure.data.csv :as csv]
    [clojure.java.io :as io]
    [clojure.tools.logging :as log]
    [merkle-db.spark :as mdbs]
    [movie-lens.link :as link]
    [movie-lens.movie :as movie]
    [movie-lens.rating :as rating]
    [movie-lens.tag :as tag]
    [sparkling.core :as spark]))


(def tables
  [{:table "movies"
    :filename "movies.csv"
    :params movie/table-parameters
    :header movie/csv-header
    :parser movie/parse-row}
   {:table "links"
    :filename "links.csv"
    :params link/table-parameters
    :header link/csv-header
    :parser link/parse-row}
   {:table "tags"
    :filename "tags.csv"
    :params tag/table-parameters
    :header tag/csv-header
    :parser tag/parse-row}
   #_
   {:table "ratings"
    :filename "ratings.csv"
    :params rating/table-parameters
    :header rating/csv-header
    :parser rating/parse-row}])


(defn csv-rdd
  "Create an RDD from the given CSV file."
  [spark-ctx csv-file header parser]
  (->> (spark/text-file spark-ctx (str csv-file) 8)
       (spark/filter (fn remove-header [line] (not= line header)))
       (spark/flat-map csv/read-csv)
       (spark/map parser)))


(defn- load-table!
  [spark-ctx store-cfg dataset-dir table-cfg]
  (let [{:keys [table filename params header parser]} table-cfg
        csv-path (str dataset-dir filename)]
    (log/info "Loading table" table "from" csv-path)
    ; TODO: table stats?
    (mdbs/build-table!
      store-cfg
      (assoc params :merkle-db.table/name table)
      (csv-rdd spark-ctx csv-path header parser))))


(defn load-dataset!
  [spark-ctx store-cfg dataset-dir]
  ; TODO: possible to do this in parallel?
  (into []
        (map (partial load-table! spark-ctx store-cfg dataset-dir))
        tables))
