(ns movie-lens.main
  (:gen-class)
  (:require
    [clojure.java.io :as io]
    [clojure.string :as str]
    ;[clojure.tools.cli :as cli]
    ;[clojure.tools.logging :as log]
    [sparkling.conf :as conf]
    [sparkling.core :as spark]))


(defn -main
  "Main entry point for example."
  [& args]
  (spark/with-context sc (-> (conf/spark-conf)              ; this creates a spark context from the given context
                             (conf/app-name "sparkling-test")
                             (conf/master "local"))
    (let [lines-rdd (spark/into-rdd sc ["This is a first line"   ;; here we provide data from a clojure collection.
                                        "Testing spark"          ;; You could also read from a text file, or avro file.
                                        "and sparkling"          ;; You could even approach a JDBC datasource
                                        "Happy hacking!"])]
      (println
        "\n================ YAHOOO ===============\n"
        (spark/collect                      ;; get every element from the filtered RDD
          (spark/filter                     ;; filter elements in the given RDD (lines-rdd)
            #(.contains % "spark")          ;; a pure clojure function as filter predicate
            lines-rdd))))))
