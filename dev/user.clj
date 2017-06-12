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
    [merkledag.core :as mdag]
    [merkledag.link :as link]
    [merkledag.node :as node]
    [merkledag.ref.file :refer [file-ref-tracker]]
    [merkle-db.bloom :as bloom]
    [merkle-db.connection :as conn]
    [merkle-db.db :as db]
    [merkle-db.generators :as mdgen]
    [merkle-db.key :as key]
    [merkle-db.partition :as part]
    [merkle-db.tablet :as tablet]
    [multihash.core :as multihash]
    [multihash.digest :as digest]))


(def conn
  (merkle_db.connection.Connection.
    (mdag/init-store
      :store (file-block-store "var/db/blocks")
      :cache {:total-size-limit (* 32 1024)})
    (file-ref-tracker
      "var/db/refs.edn")))


(defn bootstrap
  []
  (conn/create-db! conn "iris" {})
  ; create table
  ; load data
  )
