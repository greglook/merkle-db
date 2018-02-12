(defproject mvxcvi/merkle-db.tools "0.1.0-SNAPSHOT"
  :description "MerkleDB utility tools."
  :url "https://github.com/greglook/merkle-db"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :deploy-branches ["master"]
  :pedantic? :abort

  :dependencies
  [[org.clojure/clojure "1.9.0"]
   [mvxcvi/merkle-db "0.1.0-SNAPSHOT"]
   [rhizome "0.2.9"]]

  :aot
  [clojure.tools.logging.impl
   merkle-db.spark.key-partitioner
   merkle-db.spark.table-rdd
   sparkling.destructuring
   sparkling.serialization]

  :profiles
  {:repl
   {:source-paths ["dev"]
    :dependencies
    [[clj-stacktrace "0.2.8"]
     [org.clojure/tools.namespace "0.2.11"]]}})
