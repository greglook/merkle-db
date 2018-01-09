(ns movie-lens.main
  (:gen-class)
  (:require
    [blocks.core :as block]
    [clojure.java.io :as io]
    [clojure.string :as str]
    ;[clojure.tools.cli :as cli]
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
    [movie-lens.load :as load]
    [puget.printer :as puget]
    [sparkling.conf :as conf]
    [sparkling.core :as spark]
    [sparkling.destructuring :as sde]))


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


(defn -main
  "Main entry point for example."
  [& args]
  ; TODO: parameterize:
  ; - block url
  ; - ref tracker
  ; - spark master
  (let [dataset-path (if (str/ends-with? (first args) "/")
                       (first args)
                       (str (first args) "/"))
        store-cfg {:block-url "data/db/blocks"}
        store (load/init-store store-cfg)
        #_#_
        tracker (doto (mrf/file-ref-tracker "data/db/refs.tsv")
                  (mrf/load-history!))
        #_#_
        conn (conn/connect graph tracker)]
    (spark/with-context spark-ctx (-> (conf/spark-conf)
                                      (conf/app-name "movie-lens-recommender")
                                      (conf/master "local"))
      (try
        (log/info "Loading dataset tables from" dataset-path)
        (let [tables (dataset/load-dataset! spark-ctx store store-cfg dataset-path)
              ,,,]
          (pprint tables)
          ,,,)
        (catch Throwable err
          (log/error err "Spark task failed!")))
      ; Pause until user hits enter.
      (println "\nMain sequence complete - press RETURN to terminate")
      (flush)
      (read-line))))
