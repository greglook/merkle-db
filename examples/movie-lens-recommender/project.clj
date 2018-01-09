(defproject mvxcvi/movie-lens-recommender "0.1.0-SNAPSHOT"
  :description "Example of building a recommender model from the MovieLens dataset."
  :url "https://github.com/greglook/merkle-db"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :dependencies
  [[org.clojure/clojure "1.9.0"]
   [org.clojure/data.csv "0.1.4"]
   [gorillalabs/sparkling "2.1.2"]
   [mvxcvi/merkle-db "0.1.0-SNAPSHOT"]
   [ch.qos.logback/logback-classic "1.1.7"]
   [org.slf4j/log4j-over-slf4j "1.7.25"]
   [org.apache.spark/spark-core_2.11 "2.1.1"
    :exclusions [log4j org.slf4j/slf4j-log4j12]]
   [org.apache.spark/spark-mllib_2.11 "2.1.1"
    :exclusions [log4j org.slf4j/slf4j-log4j12]]]

  :aot
  [clojure.tools.logging.impl
   movie-lens.main
   sparkling.destructuring
   sparkling.serialization])