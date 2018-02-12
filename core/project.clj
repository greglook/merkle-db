(defproject mvxcvi/merkle-db.core "0.1.0-SNAPSHOT"
  :description "Hybrid data store built on merkle trees."
  :url "https://github.com/greglook/merkle-db"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :monolith/inherit true
  :deploy-branches ["master"]
  :pedantic? :abort

  :dependencies
  [[org.clojure/clojure "1.9.0"]
   [org.clojure/tools.logging "0.4.0"]
   [bigml/sketchy "0.4.1"]
   [mvxcvi/multihash "2.0.3"]
   [mvxcvi/blocks "1.1.0"]
   [mvxcvi/merkledag-core "0.4.1"]
   [mvxcvi/merkledag-ref "0.2.0"]]

  :codox
  {:metadata {:doc/format :markdown}
   :source-uri "https://github.com/greglook/merkle-db/blob/master/{filepath}#L{line}"
   :output-path "target/doc/api"}

  :profiles
  {:dev
   {:dependencies
    [[org.clojure/data.csv "0.1.4"]
     [org.clojure/test.check "0.9.0"]
     [com.gfredericks/test.chuck "0.2.8"]
     [commons-logging "1.2"]
     [mvxcvi/test.carly "0.4.1"]
     [riddley "0.1.14"]]}

   :repl
   {:source-paths ["dev"]
    :dependencies
    [[clj-stacktrace "0.2.8"]
     [org.clojure/tools.namespace "0.2.11"]
     [rhizome "0.2.9"]]}

   :coverage
   {:plugins [[lein-cloverage "1.0.10"]]
    :dependencies [[commons-logging "1.2"]]
    :jvm-opts ["-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.SimpleLog"
               "-Dorg.apache.commons.logging.simplelog.defaultlog=trace"]}})
