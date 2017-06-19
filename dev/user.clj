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
      [key :as key]
      [partition :as part]
      [tablet :as tablet])
    [multihash.core :as multihash]
    [multihash.digest :as digest]))


; TODO: replace this with reloaded repl
(def conn
  (merkle_db.connection.Connection.
    (mdag/init-store
      :store (file-block-store "var/db/blocks")
      :cache {:total-size-limit (* 32 1024)})
    (doto (mrf/file-ref-tracker "var/db/refs.edn")
      (mrf/load-history!))))


(defn bootstrap
  []
  (conn/create-db! conn "iris" {})
  ; create table
  ; load data
  )
