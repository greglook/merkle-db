(ns user
  "Custom repl customization for local development."
  (:require
    [blocks.core :as block]
    [blocks.store.file :refer [file-block-store]]
    [blocks.store.memory :refer [memory-block-store]]
    [clojure.repl :refer :all]
    [clojure.set :as set]
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.string :as str]
    [clojure.tools.namespace.repl :refer [refresh]]
    [com.stuartsierra.component :as component]
    [merkledag.core :as mdag]
    [merkledag.link :as link]
    [merkledag.node :as node]
    [merkle-db.bloom :as bloom]
    [merkle-db.connection :as conn]
    [merkle-db.database :as db]
    [merkle-db.graph :as graph]
    [merkle-db.index :as index]
    [merkle-db.key :as key]
    [merkle-db.partition :as part]
    [merkle-db.patch :as patch]
    [merkle-db.record :as record]
    [merkle-db.table :as table]
    [merkle-db.tablet :as tablet]
    [multihash.core :as multihash]
    [multihash.digest :as digest]))


(defn init-store
  []
  (mdag/init-store
    ;:store (file-block-store "var/db/blocks")
    ;:cache {:total-size-limit (* 32 1024)}
    :types graph/codec-types))


(defn test-serialize
  []
  (let [store (init-store)
        bf (reduce bloom/add (bloom/create 20) (range 10))]
    (println "before" (Integer/toHexString (System/identityHashCode bf)) (:bins bf))
    (let [node (mdag/store-node! store nil {:bf bf})
          node' (mdag/get-node store (::node/id node))
          bf' (:bf (::node/data node'))]
      (println "after" (Integer/toHexString (System/identityHashCode bf')) (:bins bf'))
      (= bf bf'))))
