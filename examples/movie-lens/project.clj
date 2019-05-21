(defproject mvxcvi/movie-lens "0.1.1-SNAPSHOT"
  :description "Example project demonstrating working with the MovieLens dataset."
  :url "https://github.com/greglook/merkle-db"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :dependencies
  [[org.clojure/clojure "1.10.0"]
   [org.clojure/data.csv "0.1.4"]
   [org.clojure/tools.cli "0.4.2"]
   [mvxcvi/blocks "1.2.0"]
   [mvxcvi/blocks-s3 "0.4.0"
    :exclusions [net.jpountz.lz4/lz4]]
   [mvxcvi/merkle-db "0.1.1-SNAPSHOT"]
   [gorillalabs/sparkling "2.1.3"
    :exclusions [org.objenesis/objenesis]]
   [org.slf4j/log4j-over-slf4j "1.7.26"]
   [ch.qos.logback/logback-classic "1.2.3"]
   [riemann-clojure-client "0.5.0"]

   ;; Version pinning
   [com.fasterxml.jackson.core/jackson-core "2.7.9"]
   [commons-codec "1.12"]
   [joda-time "2.10.2"]
   [org.apache.httpcomponents/httpclient "4.5.8"]]

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
    [[org.apache.spark/spark-core_2.11 "2.4.3"
      :exclusions [commons-codec
                   commons-net
                   io.netty/netty-all
                   log4j
                   org.apache.commons/commons-compress
                   org.scala-lang/scala-reflect
                   org.slf4j/slf4j-log4j12]]
     [org.apache.spark/spark-mllib_2.11 "2.4.3"
      :exclusions [commons-codec
                   log4j
                   org.slf4j/slf4j-log4j12]]
     [com.thoughtworks.paranamer/paranamer "2.8"]]}

   :profiler-agent
   {:jvm-opts
    ["-javaagent:riemann-jvm-profiler.jar=prefix=movie-lens,host=localhost,localhost-pid?=true"]}

   :uberjar
   {:target-path "target/uberjar"
    :aot :all}})
