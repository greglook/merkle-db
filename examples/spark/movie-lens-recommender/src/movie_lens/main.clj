(ns movie-lens.main
  (:gen-class)
  (:require
    [blocks.core :as block]
    [blocks.store.file :as bsf]
    [clojure.java.io :as io]
    [clojure.string :as str]
    ;[clojure.tools.cli :as cli]
    [clojure.tools.logging :as log]
    ;[com.stuartsierra.component :as component]
    [merkle-db.connection :as conn]
    [merkle-db.db :as db]
    [merkle-db.table :as table]
    [merkledag.core :as mdag]
    ;[merkledag.link :as link]
    ;[merkledag.node :as node]
    [merkledag.ref.file :as mrf]
    [movie-lens.dataset :as dataset]
    [sparkling.conf :as conf]
    [sparkling.core :as spark]))


(defn load-dataset
  "Load the dataset into tables in a merkle-db database."
  [spark-ctx graph csv-dir]
  ; take input CSV
  ; convert to RDD of rows
  (let [movies (dataset/load-movies spark-ctx csv-dir)]
    (prn (spark/take 5 movies)))

  ; map to pairRDD of [key record]
  ; sort by key
  ; split pairRDD into partition-sized chunks
  ; convert each chunk into a merkle-db partition node
  ; build index tree over partition-node metadata
  )


(defn -main
  "Main entry point for example."
  [& args]
  ; TODO: parameterize block and ref locations?
  (let [dataset-path (first args)
        graph (mdag/init-store
                :store (bsf/file-block-store "var/db/blocks")
                :cache {:total-size-limit (* 32 1024)}
                :types merkle-db.graph/codec-types)
        tracker (doto (mrf/file-ref-tracker "var/db/refs.tsv")
                  (mrf/load-history!))
        conn (conn/connect graph tracker)]
    (spark/with-context spark-ctx (-> (conf/spark-conf)
                                      (conf/app-name "movie-lens-recommender")
                                      ; TODO: parameterize master
                                      (conf/master "local"))
      (load-dataset spark-ctx graph dataset-path)
      ; Pause until user hits enter.
      (println "Main sequence complete - press RETURN to terminate")
      (flush)
      (read-line))))
