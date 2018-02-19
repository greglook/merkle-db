(defproject mvxcvi/merkle-db-tools "0.1.1-SNAPSHOT"
  :description "MerkleDB utility tools."
  :url "https://github.com/greglook/merkle-db"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :monolith/inherit true
  :deploy-branches ["master"]

  :dependencies
  [[org.clojure/clojure "1.9.0"]
   [mvxcvi/merkle-db-core "0.1.1-SNAPSHOT"]
   [rhizome "0.2.9"]]

  :codox
  {:metadata {:doc/format :markdown}
   :source-uri "https://github.com/greglook/merkle-db/blob/master/lib/tools/{filepath}#L{line}"
   :output-path "../../target/doc/codox/tools"})
