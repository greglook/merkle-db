(defproject mvxcvi/merkle-db "0.1.0-SNAPSHOT"
  :description "Hybrid data store built on merkle trees."
  :url "https://github.com/greglook/merkle-db"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :deploy-branches ["master"]
  :pedantic? :abort

  :dependencies
  [[org.clojure/clojure "1.8.0"]
   [clojure-future-spec "1.9.0-alpha14"]
   [mvxcvi/blocks "0.8.0"]
   [mvxcvi/merkledag "0.2.0-SNAPSHOT"]]

  :whidbey
  {:tag-types {'blocks.data.PersistentBytes {'blocks/bytes #(apply str (map (partial format "%02x") (seq %)))}}}

  :profiles
  {:repl
   {:source-paths ["dev"]}})
