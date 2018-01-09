(ns movie-lens.dataset
  "Functions for parsing and loading the MovieLens dataset source files."
  (:require
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [movie-lens.link :as link]
    [movie-lens.load :as load]
    [movie-lens.movie :as movie]
    [movie-lens.rating :as rating]
    [movie-lens.tag :as tag]))


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
   #_
   {:table "ratings"
    :filename "ratings.csv"
    :params rating/table-parameters
    :header rating/csv-header
    :parser rating/parse-row}
   #_
   {:table "tags"
    :filename "tags.csv"
    :params tag/table-parameters
    :header tag/csv-header
    :parser tag/parse-row}])


(defn- load-table!
  [spark-ctx store store-cfg dataset-dir table-cfg]
  (let [{:keys [table filename params header parser]} table-cfg
        csv-path (str dataset-dir filename)]
    (log/info "Loading table" table "from" csv-path)
    (load/build-table!
      store store-cfg
      (assoc params :merkle-db.table/name table)
      (load/csv-rdd spark-ctx csv-path header parser))))


(defn load-dataset!
  [spark-ctx store store-cfg dataset-dir]
  (into []
        (map (fn load-dataset-table
               [table-cfg]
               (load-table! spark-ctx store store-cfg dataset-dir table-cfg)))
        tables))
