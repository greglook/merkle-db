(ns user
  "Custom repl customization for local development."
  (:require
    [blocks.core :as block]
    [clojure.java.io :as io]
    [clojure.repl :refer :all]
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.string :as str]
    [clojure.test.check.generators :as gen]
    [clojure.tools.namespace.repl :refer [refresh]]
    [merkledag.refs.memory :refer [memory-ref-tracker]]
    [merkle-db.bloom :as bloom]
    [merkle-db.connection :as conn]
    [merkle-db.db :as db]
    [merkle-db.generators :as mdgen]
    [merkle-db.key :as key]
    [merkle-db.node :as node]
    [merkle-db.partition :as part]
    [merkle-db.tablet :as tablet]
    [multihash.core :as multihash]
    [multihash.digest :as digest])
  (:import
    blocks.data.Block
    multihash.core.Multihash))


(def mem-conn
  (merkle_db.connection.Connection.
    (node/memory-node-store)
    (memory-ref-tracker)))
