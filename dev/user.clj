(ns user
  "Custom repl customization for local development."
  (:require
    [blocks.core :as block]
    [blocks.store.file :refer [file-block-store]]
    [clojure.data.csv :as csv]
    [clojure.java.io :as io]
    [clojure.repl :refer :all]
    [clojure.set :as set]
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.string :as str]
    [clojure.test.check.generators :as gen]
    [clojure.tools.namespace.repl :refer [refresh]]
    [com.stuartsierra.component :as component]
    [merkledag.core :as mdag]
    [merkledag.link :as link]
    [merkledag.node :as node]
    [merkledag.ref.file :as mrf]
    [merkle-db.bloom :as bloom]
    [merkle-db.connection :as conn]
    [merkle-db.database :as db]
    [merkle-db.generators :as mdgen]
    [merkle-db.graph :as graph]
    [merkle-db.index :as index]
    [merkle-db.key :as key]
    [merkle-db.partition :as part]
    [merkle-db.patch :as patch]
    [merkle-db.record :as record]
    [merkle-db.table :as table]
    [merkle-db.tablet :as tablet]
    [merkle-db.viz :as viz]
    [multihash.core :as multihash]
    [multihash.digest :as digest]
    [rhizome.viz :as rhizome]))


; TODO: replace this with reloaded repl
(def conn
  (conn/connect
    (mdag/init-store
      :store (file-block-store "var/db/blocks")
      :cache {:total-size-limit (* 32 1024)}
      :types graph/codec-types)
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


(defn build-table
  [table-name params records]
  (let [store (.store conn)
        lexicoder (key/lexicoder (::key/lexicoder params :bytes))
        root (->>
               records
               (map (partial record/encode-entry
                             lexicoder
                             (::table/primary-key params)))
               (part/partition-records store params)
               (index/build-tree store params))
        node (-> params
                 (assoc :data/type :merkle-db/table
                        ::record/count (::record/count root 0))
                 (cond-> root (assoc ::table/data (mdag/link "data" root)))
                 (dissoc ::table/patch)
                 (->> (mdag/store-node! store nil))
                 (::node/data))]
    (table/load-table store table-name node)))



;; ## visualization dev utils

(defn sample-record-data
  "Generates a vector of `n` maps of record data using the given field keys."
  [field-keys n]
  (rand-nth (gen/sample (gen/vector (gen/map (gen/elements field-keys)
                                             gen/large-integer)
                                    n))))


(defn sample-subset
  "Given a sequence of values, returns a subset of them in the same order, where
  each element has the given chance of being in the subset."
  [p xs]
  (->> (repeatedly #(< (rand) p))
       (map vector xs)
       (filter second)
       (mapv first)))


(defn viz-index-build
  [filename fan-out part-limit n]
  (let [field-keys #{:a :b :c :d :e :f}
        families {:bc #{:b :c}, :de #{:d :e}}
        record-keys (map (partial key/encode key/integer-lexicoder) (range n))
        record-data (sample-record-data field-keys n)
        records (zipmap record-keys record-data)
        store (mdag/init-store :types graph/codec-types)
        params {::record/count n
                ::record/families families
                ::index/fan-out fan-out
                ::part/limit part-limit}
        parts (part/from-records store params records)
        root (index/build-tree store params parts)]
    (viz/save-tree store
                   (::node/id (meta root))
                   (constantly true)
                   filename)
    [store root]))


(defn gen-update!
  "Generate an example update case for the given set of fields, families, and
  number of contextual records."
  [field-keys families n]
  (let [record-keys (map (partial key/encode key/integer-lexicoder) (range n))
        extant-keys (sample-subset 0.5 record-keys)
        records (zipmap extant-keys
                        (sample-record-data
                          field-keys
                          (count extant-keys)))
        pick-half (fn [xs]
                    (let [half (int (/ (count xs) 2))]
                      (condp > (rand)
                        0.25 nil
                        0.50 (take half xs)
                        0.75 (drop half xs)
                             xs)))
        changes (merge
                  (let [ins-keys (sample-subset 0.25 (pick-half record-keys))]
                    (zipmap ins-keys (sample-record-data field-keys (count ins-keys))))
                  (let [del-keys (sample-subset 0.25 (pick-half record-keys))]
                    (zipmap del-keys (repeat ::patch/tombstone))))]
    {::record/families families
     ::index/fan-out 4
     ::part/limit 5
     :records records
     :changes changes}))


(defn viz-update
  [update-case]
  (let [store (mdag/init-store :types graph/codec-types)
        root (->> (:records update-case)
                  (sort-by first)
                  (part/partition-records store update-case)
                  (index/build-tree store update-case))]
    (try
      (let [root' (index/update-tree store update-case root
                                     (sort-by first (:changes update-case)))
            old-nodes (graph/find-nodes
                        store {}
                        (mdag/get-node store (::node/id (meta root)))
                        (constantly true))
            new-nodes (graph/find-nodes
                        store {}
                        (mdag/get-node store (::node/id (meta root')))
                        (constantly true))
            all-nodes (merge old-nodes new-nodes)
            shared-ids (set/intersection (set (keys old-nodes)) (set (keys new-nodes)))]
        (rhizome/view-graph
          (vals all-nodes)
          (fn adjacent
            [node]
            (keep #(when-let [target (get all-nodes (::link/target %))]
                     (vary-meta target assoc ::link/name (::link/name %)))
                  (::node/links node)))
          :node->descriptor (fn [node]
                              (assoc (viz/node->descriptor node)
                                     :color (cond
                                              (shared-ids (::node/id node)) :blue
                                              (new-nodes (::node/id node)) :green
                                              :else :black)))
          :edge->descriptor viz/edge->descriptor
          :options {:dpi 64})
        ['context update-case
         'original-root (link/identify root) root
         'updated-root (link/identify root') root'])
      (catch Exception ex
        (throw (ex-info "Error updating tree!"
                        {:context update-case
                         :original root}
                        ex))))))


(defn read-test-case
  "Read a test case from standard input as output by the generative test case."
  []
  (let [tcase (read-string (read-line))
        {:syms [families fan-out part-limit rkeys ukeys dkeys]} tcase
        records (map-indexed #(vector (key/create (.data %2)) {:a %1}) rkeys)
        updates (map-indexed #(vector (key/create (.data %2)) {:b %1}) ukeys)
        deletions (map #(vector (key/create (.data %)) ::patch/tombstone) dkeys)
        changes (vec (into (sorted-map) (concat updates deletions)))
        update-map {::record/families families
                    ::index/fan-out fan-out
                    ::part/limit part-limit
                    :records records
                    :changes changes}]
    (viz-update update-map)))


; TODO: load testing framework
; - prepare fresh ref tracker and block store
; - inputs are dataset name and table params
;   - ! start time
;   - ! repository HEAD commit
;   - ! record dataset/table and table params
; - load table as fast as possible
;   - data must already be sorted?
;     - or: take part-limit records, sort them, write out somewhere temporary
;     - merge-combine k sorted parts together, writing out to new temp files
;     - when there are fewer than k parts left, load the merged stream from all of them
;   - ! input data size
;   - ! input data rows
;   - ! load elapsed
;   - ! table stats
;   - during load(?) sample n records for later querying
;   - ! sampled record ids
;   - table/read the sampled records, check results
;   - table/read some nonexistent records, check results
;   - ! elapsed read times
