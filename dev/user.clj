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
      [patch :as patch]
      [record :as record]
      [table :as table]
      [tablet :as tablet]
      [viz :as viz])
    [multihash.core :as multihash]
    [multihash.digest :as digest]
    [rhizome.viz :as rhizome]))


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

(defn sample-record-data
  "Generates a vector of `n` maps of record data using the given field keys."
  [field-keys n]
  (rand-nth (gen/sample (gen/vector (mdgen/record-data field-keys) n))))


(defn sample-subset
  "Given a sequence of values, returns a subset of them in the same order, where
  each element has the given chance of being in the subset."
  [p xs]
  (->> (repeatedly #(< (rand) p))
       (map vector xs)
       (filter second)
       (mapv first)))


(defn viz-index-build
  [filename branch-factor part-limit n]
  (let [field-keys #{:a :b :c :d :e :f}
        families {:bc #{:b :c}, :de #{:d :e}}
        record-keys (map (partial key/encode key/long-lexicoder) (range n))
        record-data (sample-record-data field-keys n)
        records (zipmap record-keys record-data)
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


(defn gen-update!
  [field-keys families n]
  (let [record-keys (map (partial key/encode key/long-lexicoder) (range n))
        extant-keys (sample-subset 0.5 record-keys)
        records (zipmap extant-keys
                        (sample-record-data
                          field-keys
                          (count extant-keys)))
        changes (merge
                  (let [ins-keys (sample-subset 0.25 record-keys)]
                    (zipmap ins-keys (sample-record-data field-keys (count ins-keys))))
                  (let [del-keys (sample-subset 0.25 record-keys)]
                    (zipmap del-keys (repeat ::patch/tombstone))))]
    {:families families
     :records records
     :changes changes}))


(defn viz-update
  [update-map branch-factor part-limit]
  (let [{:keys [families records changes]} update-map
        store (mdag/init-store :types record/codec-types)
        params {::record/families families
                ::index/branching-factor branch-factor
                ::part/limit part-limit}
        parts (part/from-records store params records)
        root (index/build-index store params parts)
        root' (index/update-tree store params root changes)]
    [store root root']))
