(ns movie-lens.main
  (:gen-class)
  (:require
    [blocks.store.file]
    [clojure.string :as str]
    [clojure.tools.cli :as cli]
    [clojure.tools.logging :as log]
    ;[merkle-db.connection :as conn]
    ;[merkle-db.database :as db]
    ;[merkledag.ref.file :as mrf]
    [movie-lens.dataset :as dataset]
    [movie-lens.util :as u]
    [multihash.core :as multihash]
    [sparkling.conf :as conf]
    [sparkling.core :as spark]))


(def cli-options
  [["-b" "--blocks URL" "Location of backing block storage"
    :default "file://data/db/blocks"]
   ["-r" "--refs URL" "Location of backing ref tracker"
    :default "file://data/db/refs.tsv"]
   ["-m" "--master URL" "Spark master connection URL"
    :default "local"]
   ["-h" "--help"]])


(def commands
  ["load-db"])


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
        (let [db (dataset/load-dataset! spark-ctx store-cfg dataset-path)
              elapsed (/ (- (System/currentTimeMillis) start) 1e3)]
          (log/infof "Built database root %s in %s"
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

      ; Unknown command
      (binding [*out* *err*]
        (println "The argument" (pr-str command) "is not a supported command")
        (System/exit 2)))))
