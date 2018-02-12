(defproject mvxcvi/merkle-db.spark "0.1.0-SNAPSHOT"
  :description "MerkleDB integration with Apache Spark."
  :url "https://github.com/greglook/merkle-db"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :deploy-branches ["master"]
  :pedantic? :abort

  :dependencies
  [[org.clojure/clojure "1.9.0"]
   [mvxcvi/merkle-db "0.1.0-SNAPSHOT"]
   [gorillalabs/sparkling "2.1.2"
    :exclusions [org.objenesis/objenesis]]

   ; Version pinning
   ;[com.fasterxml.jackson.core/jackson-databind "2.6.6"]
   ;[com.fasterxml.jackson.core/jackson-annotations "2.6.5"]
   ;[commons-codec "1.10"]
   ;[joda-time "2.8.1"]
   ;[org.apache.httpcomponents/httpclient "4.5.2"]
   ]

  :aot
  [clojure.tools.logging.impl
   merkle-db.spark.key-partitioner
   merkle-db.spark.table-rdd
   sparkling.destructuring
   sparkling.serialization]

  :profiles
  {:provided
   {:dependencies
    [[org.apache.spark/spark-core_2.11 "2.2.1"
      :exclusions [commons-codec
                   commons-net
                   log4j
                   org.apache.commons/commons-compress
                   org.scala-lang/scala-reflect
                   org.slf4j/slf4j-log4j12]]
     [org.apache.spark/spark-mllib_2.11 "2.2.1"
      :exclusions [log4j org.slf4j/slf4j-log4j12]]
     [com.thoughtworks.paranamer/paranamer "2.6"]]}})
