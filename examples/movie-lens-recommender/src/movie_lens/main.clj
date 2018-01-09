(ns movie-lens.main
  (:gen-class)
  (:require
    [blocks.store.file]
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.tools.cli :as cli]
    [clojure.tools.logging :as log]
    [merkle-db.connection :as conn]
    [merkle-db.db :as db]
    [merkle-db.index :as index]
    [merkle-db.key :as key]
    [merkle-db.partition :as part]
    [merkle-db.record :as record]
    [merkle-db.table :as table]
    [merkledag.core :as mdag]
    [merkledag.node :as node]
    [merkledag.ref.file :as mrf]
    [movie-lens.dataset :as dataset]
    [puget.printer :as puget]
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
  ["load-db"])


(defmethod print-method multihash.core.Multihash
  [x w]
  (print-method (tagged-literal 'data/hash (multihash.core/base58 x)) w))


(def pprinter
  (-> (puget/pretty-printer
        {:print-color true
         :width 200})
      (update :print-handlers
              puget.dispatch/chained-lookup
              {multihash.core.Multihash
               (puget/tagged-handler 'data/hash
                                     multihash.core/base58)

               merkledag.link.MerkleLink
               (puget/tagged-handler 'merkledag/link
                                     merkledag.link/link->form)})))


(defn- pprint
  "Pretty print something."
  [x]
  (puget/render-out pprinter x))


(defn duration-str
  "Convert a duration in seconds into a human-readable duration string."
  [elapsed]
  (if (< elapsed 60)
    (format "%.2f sec" (double elapsed))
    (let [hours (int (/ elapsed 60 60))
          minutes (mod (int (/ elapsed 60)) 60)
          seconds (mod (int elapsed) 60)]
      (str (if (pos? hours)
             (format "%d:%02d" hours minutes)
             minutes)
           (format ":%02d" seconds)))))


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
        (let [tables (dataset/load-dataset! spark-ctx store-cfg dataset-path)]
          (pprint tables)
          ; TODO: construct database root
          ,,,)
        (catch Throwable err
          (log/error err "Spark task failed!")))
      (let [elapsed (/ (- (System/currentTimeMillis) start) 1e3)]
        ; Pause until user hits enter.
        (printf "\nDatabase load complete in %s - press RETURN to exit\n"
                (duration-str elapsed))
        (flush)
        (read-line)))))


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
