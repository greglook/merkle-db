(ns movie-lens.main
  (:gen-class)
  (:require
    [blocks.core :as block]
    [blocks.store.file]
    [blocks.store.s3]
    [clojure.string :as str]
    [clojure.tools.cli :as cli]
    [clojure.tools.logging :as log]
    [merkle-db.database :as db]
    [merkle-db.spark.table-rdd :as table-rdd]
    [merkle-db.table :as table]
    [merkledag.core :as mdag]
    [movie-lens.dataset :as dataset]
    [movie-lens.movie :as movie]
    [movie-lens.rating :as rating]
    [movie-lens.tag :as tag]
    [movie-lens.util :as u]
    [multihash.core :as multihash]
    [sparkling.conf :as conf]
    [sparkling.core :as spark]
    [sparkling.destructuring :as sde]
    [sparkling.serialization]))


(def cli-options
  [["-b" "--blocks URL" "Location of backing block storage"
    :default "file://data/db/blocks"]
   ["-r" "--refs URL" "Location of backing ref tracker"
    :default "file://data/db/refs.tsv"]
   ["-m" "--master URL" "Spark master connection URL"]
   [nil  "--no-prompt" "Disable the prompt for user input before exiting"]
   ["-h" "--help"]])


(def commands
  ["load-db" "most-tagged" "best-rated"])


; TODO: best way to pass around store connection parameters to the executors?
(defn- store-constructor
  "Initialize a MerkleDAG graph store from the given config."
  [cfg]
  (fn init
    []
    (require 'blocks.store.s3)
    (mdag/init-store
      :encoding [:mdag :gzip :cbor]
      :store (block/->store (:blocks-url cfg))
      :cache {:total-size-limit (:cache-size cfg (* 32 1024 1024))}
      :types merkle-db.graph/codec-types)))


(defn- load-db
  [spark-ctx opts args]
  (when-not (= 1 (count args))
    (binding [*out* *err*]
      (println "Usage: load-db <dataset-dir>")
      (System/exit 3)))
  (let [dataset-path (if (str/ends-with? (first args) "/")
                       (first args)
                       (str (first args) "/"))]
    (log/info "Loading dataset tables from" dataset-path)
    (let [db (dataset/load-dataset! spark-ctx (:init-store opts) dataset-path)]
      (log/info "Loaded database tables into root node"
                (multihash/base58 (:merkledag.node/id db)))
      (u/pprint db))))


(defn- most-tagged
  "Calculate the most-tagged movies."
  [spark-ctx opts args]
  (when (or (empty? args) (< 2 (count args)))
    (binding [*out* *err*]
      (println "Usage: most-tagged <db-root-id> [n]")
      (System/exit 3)))
  (let [start (System/currentTimeMillis)
        init-store (:init-store opts)
        db-root-id (multihash/decode (first args))
        n (Integer/parseInt (or (second args) "10"))
        db (db/load-database (init-store) {:merkledag.node/id db-root-id})
        ;_ (log/info "Loaded database root node")
        ;_ (u/pprint db)
        movies (db/get-table db "movies")
        tags (db/get-table db "tags")
        top (->> (table-rdd/scan spark-ctx init-store tags)
                 (spark/values)
                 (spark/key-by ::movie/id)
                 (spark/count-by-key)
                 (sort-by val (comp - compare))
                 (take n)
                 (vec))
        movie-lookup (into {}
                           (map (juxt ::movie/id identity))
                           (table/read movies (map first top)))]
    (println "Most-tagged movies in dataset:")
    (newline)
    (println "Rank  Tags  Movie (Year)")
    (println "----  ----  ---------------------")
    (dotimes [i (min n (count top))]
      (let [[movie-id tag-count] (nth top i)
            movie (get movie-lookup movie-id)]
        (printf "%3d   %4d  %s (%s)\n"
                (inc i)
                tag-count
                (::movie/title movie)
                (::movie/year movie "unknown"))))))


(defn- best-rated
  "Calculate the best-rated movies."
  [spark-ctx opts args]
  (when (or (empty? args) (< 2 (count args)))
    (binding [*out* *err*]
      (println "Usage: best-rated <db-root-id> [n]")
      (System/exit 3)))
  (let [start (System/currentTimeMillis)
        init-store (:init-store opts)
        db-root-id (multihash/decode (first args))
        n (Integer/parseInt (or (second args) "10"))
        db (db/load-database (init-store) {:merkledag.node/id db-root-id})
        movies (db/get-table db "movies")
        ratings (db/get-table db "ratings")
        top (->> (table-rdd/scan spark-ctx init-store ratings)
                 (spark/values)
                 (spark/key-by ::movie/id)
                 (spark/combine-by-key
                   (fn seq-fn
                     [rating-record]
                     (if-let [score (::rating/score rating-record)]
                       [1 score]
                       [0 0.0]))
                   (fn conj-fn
                     [[rcount rsum] rating-record]
                     (if-let [score (::rating/score rating-record)]
                       [(inc rcount) (+ rsum score)]
                       [rcount rsum]))
                   (fn merge-fn
                     [[rcount1 rsum1] [rcount2 rsum2]]
                     [(+ rcount1 rcount2) (+ rsum1 rsum2)]))
                 (spark/filter
                   (sde/fn [(movie-id [rcount rsum])]
                     (<= 100 rcount)))
                 (spark/map-values
                   (fn avg
                     [[rcount rsum]]
                     (/ rsum rcount)))
                 (spark/collect)
                 (map (juxt sde/key sde/value))
                 (sort-by second (comp - compare))
                 (take n)
                 (vec))
        movie-lookup (into {}
                           (map (juxt ::movie/id identity))
                           (table/read movies (map first top)))]
    (println "Best-rated movies in dataset:")
    (newline)
    (println "Rank  Rating  Movie (Year)")
    (println "----  ------  ---------------------")
    (dotimes [i n]
      (let [[movie-id avg-rating] (nth top i)
            movie (get movie-lookup movie-id)]
        (printf "%3d   %6.3f  %s (%s)\n"
                (inc i)
                (double avg-rating)
                (::movie/title movie)
                (::movie/year movie "unknown"))))))


(defn -main
  "Main entry point for example."
  [& raw-args]
  (let [{:keys [options arguments summary errors]} (cli/parse-opts raw-args cli-options)
        command (first arguments)
        success? (atom true)]
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
    (let [command-fn (case command
                       "load-db" load-db
                       "most-tagged" most-tagged
                       "best-rated" best-rated
                       (binding [*out* *err*]
                         (println "The argument" (pr-str command)
                                  "is not a supported command")
                         (System/exit 2)))
          options (assoc options :init-store (store-constructor {:blocks-url (:blocks options)}))
          start (System/currentTimeMillis)
          elapsed (delay (/ (- (System/currentTimeMillis) start) 1e3))]
      (spark/with-context spark-ctx (-> (conf/spark-conf)
                                        (conf/app-name "movie-lens-recommender")
                                        (cond->
                                          (:master options)
                                            (conf/master (:master options))))
        (try
          (command-fn spark-ctx options (rest arguments))
          (catch Throwable err
            (log/error err "Command failed!")
            (when-let [err (ex-data err)]
              (reset! success? false)
              (u/pprint err))))
        (when-not (:no-prompt options)
          (println "\nCommand finished in" (u/duration-str @elapsed))
          (println "Press RETURN to exit")
          (flush)
          (read-line)))
      (when-not @success?
        (System/exit 1)))))
