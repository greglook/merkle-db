(defproject merkle-db/spark "0.2.0"
  :description "MerkleDB integration with Apache Spark."
  :url "https://github.com/greglook/merkle-db/blob/master/lib/spark"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :aliases
  {"doc" ["do" ["codox"] ["marg" "--dir" "target/doc/marginalia"]]}

  :monolith/inherit true
  :deploy-branches ["master"]

  :dependencies
  [[org.clojure/clojure "1.10.0"]
   [org.clojure/tools.logging "0.4.0"]
   [merkle-db/core "0.2.0"]
   [gorillalabs/sparkling "2.1.3"
    :exclusions [org.objenesis/objenesis]]

   ;; Version pinning
   [com.fasterxml.jackson.core/jackson-core "2.7.9"]
   [com.google.code.findbugs/jsr305 "3.0.2"]
   [joda-time "2.9.9"]
   [org.slf4j/slf4j-api "1.7.25"]]

  :codox
  {:metadata {:doc/format :markdown}
   :source-uri "https://github.com/greglook/merkle-db/blob/master/lib/spark/{filepath}#L{line}"
   :output-path "target/doc/codox"}

  :profiles
  {:provided
   {:dependencies
    [[org.apache.spark/spark-core_2.11 "2.4.3"
      :exclusions [commons-codec
                   commons-net
                   log4j
                   org.apache.commons/commons-compress
                   org.scala-lang/scala-reflect
                   org.slf4j/slf4j-log4j12]]
     [org.apache.spark/spark-mllib_2.11 "2.4.3"
      :exclusions [commons-codec
                   log4j
                   org.slf4j/slf4j-log4j12]]
     [com.thoughtworks.paranamer/paranamer "2.8"]]}

   :dev
   {:aot
    [clojure.tools.logging.impl
     merkle-db.spark.key-partitioner
     merkle-db.spark.table-rdd
     sparkling.destructuring
     sparkling.serialization]}})
