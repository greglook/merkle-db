(defproject mvxcvi/merkle-db "0.1.0-SNAPSHOT"
  :description "Hybrid data store built on merkle trees."
  :url "https://github.com/greglook/merkle-db"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :aliases
  {"coverage" ["with-profile" "+coverage" "cloverage"
               "--ns-exclude-regex" "merkle-db.validate"]}

  :deploy-branches ["master"]
  :pedantic? :abort

  :dependencies
  [[org.clojure/clojure "1.9.0"]
   [org.clojure/tools.logging "0.4.0"]
   [bigml/sketchy "0.4.1"]
   [mvxcvi/merkledag-core "0.4.0-SNAPSHOT"]
   [mvxcvi/merkledag-ref "0.2.0"]
   [mvxcvi/blocks "1.1.0"]]

  :test-selectors
  {:default (complement :generative)
   :generative :generative}

  :cljfmt
  {:remove-consecutive-blank-lines? false
   :indents {checking [[:block 2]]
             check-system [[:block 2]]
             valid? [[:block 1]]
             invalid? [[:block 1]]}}

  :hiera
  {:cluster-depth 2
   :vertical false
   :show-external false
   :ignore-ns #{clojure bigml merkle-db.validate}}

  :codox
  {:metadata {:doc/format :markdown}
   :source-uri "https://github.com/greglook/merkle-db/blob/master/{filepath}#L{line}"
   :output-path "target/doc/api"}

  :whidbey
  {:tag-types
   {'blocks.data.Block {'blocks.data.Block (partial into {})}
    'merkledag.link.MerkleLink {'merkledag/link 'merkledag.link/link->form}
    'merkle_db.bloom.BloomFilter {'merkle-db/bloom-filter (juxt :bits :k)}
    'merkle_db.db.Database {'merkle-db/db (partial into {})}
    'merkle_db.key.Key {'merkle-db/key 'merkle-db.key/hex}
    'merkle_db.table.Table {'merkle-db/table (partial into {})}
    'multihash.core.Multihash {'data/hash 'multihash.core/base58}}}

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
