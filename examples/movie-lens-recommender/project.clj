(defproject mvxcvi/movie-lens-recommender "0.1.0-SNAPSHOT"
  :description "Example of building a recommender model from the MovieLens dataset."
  :url "https://github.com/greglook/merkle-db"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :dependencies
  [[org.clojure/clojure "1.9.0"]
   [org.clojure/data.csv "0.1.4"]
   [org.clojure/tools.cli "0.3.5"]
   [mvxcvi/merkle-db "0.1.0-SNAPSHOT"]
   [gorillalabs/sparkling "2.1.2"
    :exclusions [org.objenesis/objenesis]]
   [org.slf4j/log4j-over-slf4j "1.7.25"]
   [ch.qos.logback/logback-classic "1.2.3"]]

  :main movie-lens.main
  :jvm-opts ["-Xms1g" "-Xmx8g" "-Dspark.driver.memory=6g"]

  :aot
  [clojure.tools.logging.impl
   merkle-db.spark.key-partitioner
   merkle-db.spark.table-rdd
   movie-lens.main
   sparkling.destructuring
   sparkling.serialization]

  :hiera
  {:cluster-depth 2
   :vertical false
   :show-external false}

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
     [com.thoughtworks.paranamer/paranamer "2.6"]]}

   :profiler-agent
   {:jvm-opts
    ["-javaagent:riemann-jvm-profiler.jar=prefix=movie-lens,host=localhost,localhost-pid?=true"]}

   :uberjar
   {:aot :all}})
