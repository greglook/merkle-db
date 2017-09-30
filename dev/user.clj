(ns user
  "Custom repl customization for local development."
  (:require
    [blocks.core :as block]
    [blocks.store.file :refer [file-block-store]]
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
    [merkle-db.db :as db]
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
        store (mdag/init-store :types graph/codec-types)
        params {::record/count n
                ::record/families families
                ::index/branching-factor branch-factor
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
        store (mdag/init-store :types graph/codec-types)
        params {::record/families families
                ::index/branching-factor branch-factor
                ::part/limit part-limit}
        parts (part/partition-records store params records)
        root (index/build-tree store params parts)]
    (try
      (let [root' (index/update-tree store params root changes)
            old-nodes (viz/find-nodes store {}
                                      (mdag/get-node store (::node/id (meta root)))
                                      (constantly true))
            new-nodes (viz/find-nodes store {}
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
        ['context update-map
         'original-root (link/identify root) root
         'updated-root (link/identify root') root'])
      (catch Exception ex
        (throw (ex-info "Error updating tree!"
                        {:context update-map
                         :original root
                         :params params}
                        ex))))))


(def example-reuse
  {:families {:bc #{:b :c}},
   :records {(key/create [0]) {},
             (key/create [5]) {:a true
                               :b :abc
                               :c #{"g" "h" "i"}
                               :d #uuid "348f877b-eb00-4bf2-bced-0ea47991f627"}}
   :changes {(key/create [0]) {}
             (key/create [2]) {}}})


(def example-subtree
  {:families {:bc #{:b :c}}
   :records {(key/create [0]) {:a 0 :b 0 :c 0}
             (key/create [1]) {:a 1, :c 1}
             (key/create [2]) {:a 2, :b 2, :d 2}}
   :changes {(key/create [5]) {:a 5}
             (key/create [6]) {:x 6}
             (key/create [7]) {:y 7}}})


(def example-delete
  {:families {:bc #{:b :c}}
   :records {(key/create [0]) {:b true, :d "abc"}
             (key/create [3]) {:a :t, :c "qqq"}}
   :changes {(key/create [0]) ::patch/tombstone
             (key/create [1]) {:a 123}
             (key/create [3]) ::patch/tombstone,
             (key/create [5]) ::patch/tombstone}})


(def example-index-shared
  {:families {:bc #{:b :c}},
   :records {(key/create [0]) {},
             (key/create [2]) {:b {:Ec -1.0}},
             (key/create [4]) {},
             (key/create [7]) {},
             (key/create [8]) {},
             (key/create [9]) {:b #{3.0}}}
   :changes {(key/create [0]) ::patch/tombstone,
             (key/create [3]) {}
             (key/create [4]) {:a true, :x 45}}})
