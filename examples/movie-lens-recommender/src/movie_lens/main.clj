(ns movie-lens.main
  (:gen-class)
  (:require
    [blocks.core :as block]
    [blocks.store.file]
    [clojure.string :as str]
    [clojure.tools.cli :as cli]
    [clojure.tools.logging :as log]
    [merkle-db.database :as db]
    [merkle-db.spark.table-rdd :as table-rdd]
    [merkle-db.table :as table]
    [merkledag.core :as mdag]
    ;[merkledag.ref.file :as mrf]
    [movie-lens.dataset :as dataset]
    [movie-lens.movie :as movie]
    [movie-lens.tag :as tag]
    [movie-lens.util :as u]
    [multihash.core :as multihash]
    [sparkling.conf :as conf]
    [sparkling.core :as spark]
    [sparkling.destructuring :as sde]))


(def cli-options
  [["-b" "--blocks URL" "Location of backing block storage"
    :default "file://data/db/blocks"]
   ["-r" "--refs URL" "Location of backing ref tracker"
    :default "file://data/db/refs.tsv"]
   ["-m" "--master URL" "Spark master connection URL"
    :default "local"]
   ["-h" "--help"]])


(def commands
  ["load-db" "most-tagged"])


; TODO: best way to pass around store connection parameters to the executors?
(defn- store-constructor
  "Initialize a MerkleDAG graph store from the given config."
  [cfg]
  (fn init
    []
    (mdag/init-store
      :store (block/->store (:blocks-url cfg))
      :cache {:total-size-limit (:cache-size cfg (* 32 1024 1024))}
      :types merkle-db.graph/codec-types)))


(defn- load-db
  [opts args]
  (when-not (= 1 (count args))
    (binding [*out* *err*]
      (println "load-db takes exactly one argument, the path to the dataset directory")
      (System/exit 3)))
  (let [dataset-path (if (str/ends-with? (first args) "/")
                       (first args)
                       (str (first args) "/"))
        store-cfg {:blocks-url (:blocks opts)}
        start (System/currentTimeMillis)]
    (spark/with-context spark-ctx (-> (conf/spark-conf)
                                      (conf/app-name "movie-lens-recommender")
                                      (conf/master (:master opts)))
      (try
        (log/info "Loading dataset tables from" dataset-path)
        (let [db (dataset/load-dataset!
                   spark-ctx
                   (store-constructor store-cfg)
                   dataset-path)
              elapsed (/ (- (System/currentTimeMillis) start) 1e3)]
          (log/infof "Completed database build %s in total time %s"
                     (multihash/base58 (:merkledag.node/id db))
                     (u/duration-str elapsed))
          (u/pprint db)
          ; TODO: register in ref-tracker...
          ,,,)
        (catch Throwable err
          (log/error err "Spark task failed!")))
      ; Pause until user hits enter.
      (printf "\nPress RETURN to exit\n")
      (flush)
      (read-line))))


(defn- most-tagged
  "Calculate the most-tagged movies."
  [opts args]
  (let [start (System/currentTimeMillis)
        store-cfg {:blocks-url (:blocks opts)}
        init-store (store-constructor store-cfg)
        db-root-id (multihash/decode (first args))
        db (db/load-database (init-store) {:merkledag.node/id db-root-id})]
    (log/info "Loaded database root node")
    (u/pprint db)
    (spark/with-context spark-ctx (-> (conf/spark-conf)
                                      (conf/app-name "movie-lens-recommender")
                                      (conf/master (:master opts)))
      (try
        (let [movies (db/get-table db "movies")
              tags (db/get-table db "tags")
              top-10 (->> (table-rdd/scan spark-ctx init-store tags)
                          (spark/values)
                          (spark/key-by ::movie/id)
                          (spark/count-by-key)
                          (sort-by val (comp - compare))
                          (take 10)
                          (vec))
              movie-lookup (into {}
                                 (map (juxt ::movie/id identity))
                                 (table/read movies (map first top-10)))]
          (println "Most-tagged movies in dataset:")
          (newline)
          (println "Rank  Tags  Movie (Year)")
          (println "----  ----  ---------------------")
          (dotimes [i 10]
            (let [[movie-id tag-count] (nth top-10 i)
                  movie (get movie-lookup movie-id)]
              (printf "%3d   %4d  %s (%s)\n"
                      (inc i)
                      tag-count
                      (::movie/title movie)
                      (::movie/year movie "unknown")))))
        (catch Throwable err
          (log/error err "Spark task failed!")))
      ; Pause until user hits enter.
      (printf "\nPress RETURN to exit\n")
      (flush)
      (read-line))))


(defn -main
  "Main entry point for example."
  [& raw-args]
  (let [{:keys [options arguments summary errors]} (cli/parse-opts raw-args cli-options)
        command (first arguments)]
    (when errors
      (binding [*out* *err*]
        (doseq [err errors]
          (println errors))
        (System/exit 1)))
    (when (or (:help options) (nil? command) (= "help" command))
      (println "Usage: lein run [opts] <command> [args...]")
      (println "Commands:" (str/join ", " commands))
      (newline)
      (println summary)
      (flush)
      (System/exit 0))
    (case command
      "load-db"
        (load-db options (rest arguments))

      "most-tagged"
        (most-tagged options (rest arguments))

      ; Unknown command
      (binding [*out* *err*]
        (println "The argument" (pr-str command) "is not a supported command")
        (System/exit 2)))))
