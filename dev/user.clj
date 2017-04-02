(ns user
  "Custom repl customization for local development."
  (:require
    [blocks.core :as block]
    [clojure.java.io :as io]
    [clojure.repl :refer :all]
    [clojure.string :as str]
    [merkle-db.key :as key]
    [merkle-db.node :as node]
    [merkle-db.partition :as part]
    [merkle-db.tablet :as tablet]
    [multihash.core :as multihash]
    [multihash.digest :as digest])
  (:import
    blocks.data.Block
    multihash.core.Multihash))


; Conditional imports from clj-stacktrace and clojure.tools.namespace:
(try (require '[clojure.stacktrace :refer [print-cause-trace]]) (catch Exception e nil))
(try (require '[clojure.tools.namespace.repl :refer [refresh]]) (catch Exception e nil))
