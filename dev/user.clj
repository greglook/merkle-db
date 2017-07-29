(ns user
  "Custom repl customization for local development."
  (:require
    [blocks.core :as block]
    [blocks.store.file :refer [file-block-store]]
    [clojure.java.io :as io]
    [clojure.repl :refer :all]
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.string :as str]
    [clojure.test.check.generators :as gen]
    [clojure.tools.namespace.repl :refer [refresh]]
    [com.stuartsierra.component :as component]
    (merkledag
      [core :as mdag]
      [link :as link]
      [node :as node])
    [merkledag.ref.file :as mrf]
    (merkle-db
      [bloom :as bloom]
      [connection :as conn]
      [db :as db]
      [generators :as mdgen]
      [index :as index]
      [key :as key]
      [partition :as part]
      [record :as record]
      [table :as table]
      [tablet :as tablet]
      [viz :as viz])
    [multihash.core :as multihash]
    [multihash.digest :as digest]))


; TODO: replace this with reloaded repl
(def conn
  (conn/connect
    (mdag/init-store
      :store (file-block-store "var/db/blocks")
      :cache {:total-size-limit (* 32 1024)}
      :types record/codec-types)
    (doto (mrf/file-ref-tracker "var/db/refs.tsv")
      (mrf/load-history!))))


(def db
  (try
    (conn/open-db conn "iris")
    (catch Exception ex
      (println "Failed to load iris database:" (.getMessage ex)))))


(defn alter-db
  [f & args]
  (apply alter-var-root #'db f args))



;; ## visualization dev utils

(defn viz-index-build
  [filename branch-factor part-limit n]
  (let [field-keys #{:a :b :c :d :e :f}
        record-keys (map (partial key/encode key/long-lexicoder) (range n))
        record-data (rand-nth (gen/sample (gen/vector (mdgen/record-data field-keys) n)))
        records (zipmap record-keys record-data)
        ;families (rand-nth (gen/sample (mdgen/families field-keys)))
        families {:bc #{:b :c}, :de #{:d :e}}
        store (mdag/init-store :types record/codec-types)
        params {::record/count n
                ::record/families families
                ::index/branching-factor branch-factor
                ::part/limit part-limit}
        parts (part/from-records store params records)
        root (index/build-index store params parts)]
    (viz/save-tree store
                   (::node/id (meta root))
                   (constantly true)
                   filename)
    [store root]))
