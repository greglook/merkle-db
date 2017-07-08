(defproject mvxcvi/merkle-db "0.1.0-SNAPSHOT"
  :description "Hybrid data store built on merkle trees."
  :url "https://github.com/greglook/merkle-db"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :aliases
  {"docs" ["do" ["codox"] ["doc-lit"]]
   "doc-lit" ["marg" "--dir" "target/doc/marginalia"
              "src/merkle_db/connection.clj"
              "src/merkle_db/lock.clj"
              "src/merkle_db/db.clj"
              "src/merkle_db/table.clj"
              "src/merkle_db/index.clj"
              "src/merkle_db/partition.clj"
              "src/merkle_db/tablet.clj"
              "src/merkle_db/key.clj"
              "src/merkle_db/bloom.clj"
              "src/merkle_db/data.clj"]}

  :deploy-branches ["master"]
  :pedantic? :abort

  :dependencies
  [[org.clojure/clojure "1.8.0"]
   [org.clojure/tools.logging "0.4.0"]
   [clojure-future-spec "1.9.0-alpha14"]
   [bigml/sketchy "0.4.1"]
   [mvxcvi/merkledag-core "0.2.0"]
   [mvxcvi/merkledag-ref "0.1.0"]
   [mvxcvi/blocks "0.9.1"]
   #_[mvxcvi/baton "0.1.0-SNAPSHOT"]
   [rhizome "0.2.7" :scope "provided"]]

  :test-selectors
  {:default (complement :generative)
   :generative :generative}

  :hiera
  {:cluster-depth 2
   :vertical false
   :show-external false
   :ignore-ns #{clojure bigml}}

  :codox
  {:metadata {:doc/format :markdown}
   :source-uri "https://github.com/greglook/merkle-db/blob/master/{filepath}#L{line}"
   :output-path "target/doc/api"}

  :whidbey
  {:tag-types
   {'blocks.data.Block {'blocks.data.Block (partial into {})}
    'merkledag.link.MerkleLink {'merkledag/link 'merkledag.link/link->form}
    'merkle_db.bloom.BloomFilter {'merkle-db/bloom-filter #(select-keys % [:bits :k])}
    'merkle_db.db.Database {'merkle-db/db (partial into {})}
    'merkle_db.key.Key {'merkle-db/key 'merkle-db.key/hex}
    'merkle_db.table.Table {'merkle-db/table (partial into {})}
    'multihash.core.Multihash {'data/hash 'multihash.core/base58}}}

  :profiles
  {:dev
   {:dependencies
    [[org.clojure/test.check "0.9.0"]
     [com.gfredericks/test.chuck "0.2.7"]
     [riddley "0.1.14"]]}

   :repl
   {:source-paths ["dev"]
    :dependencies
    [[clj-stacktrace "0.2.8"]
     [org.clojure/tools.namespace "0.2.11"]]}})
