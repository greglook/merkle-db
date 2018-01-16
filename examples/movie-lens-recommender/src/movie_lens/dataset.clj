(ns movie-lens.dataset
  "Functions for parsing and loading the MovieLens dataset source files."
  (:require
    [clojure.data.csv :as csv]
    [clojure.java.io :as io]
    [clojure.tools.logging :as log]
    [merkle-db.database :as db]
    [merkle-db.spark :as mdbs]
    [merkle-db.table :as table]
    [merkledag.node :as node]
    [movie-lens.link :as link]
    [movie-lens.movie :as movie]
    [movie-lens.rating :as rating]
    [movie-lens.tag :as tag]
    [movie-lens.util :as u]
    [multihash.core :as multihash]
    [sparkling.core :as spark]))


(def tables
  [{:name "movies"
    :file "movies.csv"
    :params movie/table-parameters
    :header movie/csv-header
    :parser movie/parse-row}
   {:name "links"
    :file "links.csv"
    :params link/table-parameters
    :header link/csv-header
    :parser link/parse-row}
   {:name "tags"
    :file "tags.csv"
    :params tag/table-parameters
    :header tag/csv-header
    :parser tag/parse-row}
   #_
   {:name "ratings"
    :file "ratings.csv"
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
  [spark-ctx init-store dataset-dir table-cfg]
  (let [{:keys [name file params header parser]} table-cfg
        csv-path (str dataset-dir file)
        start (System/currentTimeMillis)]
    (log/info "Loading table" name "from" csv-path)
    (let [table (mdbs/build-table!
                  init-store
                  (assoc params ::table/name name)
                  (csv-rdd spark-ctx csv-path header parser))
          stats (table/collect-stats table)
          elapsed (/ (- (System/currentTimeMillis) start) 1e3)]
      (log/infof "Loaded table %s (%s) in %s"
                 name (multihash/base58 (::node/id table))
                 (u/duration-str elapsed))
      ;(u/pprint table)
      (table/print-stats stats)
      (newline)
      table)))


(defn load-dataset!
  [spark-ctx init-store dataset-dir]
  (let [store (init-store)]
    (->>
      tables
      (reduce
        (fn load-table-data
          [db table-cfg]
          (let [table (load-table! spark-ctx init-store dataset-dir table-cfg)]
            (db/set-table db (:name table-cfg) table)))
        (db/empty-db store {:data/title "MovieLens Dataset"}))
      (db/flush!))))
