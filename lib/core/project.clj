(defproject mvxcvi/merkle-db-core "0.1.0"
  :description "Hybrid data store built on merkle trees."
  :url "https://github.com/greglook/merkle-db"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :monolith/inherit true
  :deploy-branches ["master"]

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
   :source-uri "https://github.com/greglook/merkle-db/blob/master/lib/core/{filepath}#L{line}"
   :output-path "../../target/doc/codox/core"}

  :profiles
  {:dev
   {:dependencies
    [[org.clojure/test.check "0.9.0"]
     [commons-logging "1.2"]
     [com.gfredericks/test.chuck "0.2.8"]
     [mvxcvi/test.carly "0.4.1"]
     [riddley "0.1.14"]]}})
