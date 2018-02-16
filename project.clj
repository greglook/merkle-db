(defproject merkle-db "0.1.0-SNAPSHOT"
  :description "Hybrid data store built on merkle trees."
  :url "https://github.com/greglook/merkle-db"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :aliases
  {"coverage" ["with-profile" "+coverage" "monolith" "with-all" "cloverage"]}

  :min-lein-version "2.7.0"
  :pedantic? :abort

  :plugins
  [[lein-monolith "1.0.1"]
   [mvxcvi/lein-cljfmt "0.7.0-SNAPSHOT"]
   [lein-cloverage "1.0.10"]]

  :dependencies
  [[org.clojure/clojure "1.9.0"]
   [merkle-db/core "0.1.0-SNAPSHOT"]
   [merkle-db/spark "0.1.0-SNAPSHOT"]
   [merkle-db/tools "0.1.0-SNAPSHOT"]]

  :monolith
  {:project-dirs ["core" "spark" "tools"]
   :inherit [:pedantic?
             :test-selectors
             :cljfmt
             :hiera
             :whidbey]}

  :test-selectors
  {:default (complement :generative)
   :generative :generative}

  :cljfmt
  {:padding-lines 2
   :max-consecutive-blank-lines 3
   :single-import-break-width 30
   :indents {checking [[:block 2]]
             check-system [[:block 2]]
             valid? [[:block 1]]
             invalid? [[:block 1]]}}

  :hiera
  {:cluster-depth 2
   :vertical false
   :show-external false
   :ignore-ns #{clojure bigml merkle-db.validate}}

  :whidbey
  {:tag-types
   {'blocks.data.Block {'blocks.data.Block (partial into {})}
    'merkledag.link.MerkleLink {'merkledag/link 'merkledag.link/link->form}
    'merkle_db.bloom.BloomFilter {'merkle-db/bloom-filter (juxt :bits :k)}
    'merkle_db.database.Database {'merkle-db/database (partial into {})}
    'merkle_db.key.Key {'merkle-db/key 'merkle-db.key/hex}
    'merkle_db.table.Table {'merkle-db/table (partial into {})}
    'multihash.core.Multihash {'data/hash 'multihash.core/base58}}}

  :profiles
  {:dev
   {:dependencies
    [[org.clojure/data.csv "0.1.4"]
     [org.clojure/test.check "0.9.0"]
     [org.clojure/tools.logging "0.4.0"]
     [com.gfredericks/test.chuck "0.2.8"]
     [commons-logging "1.2"]
     [mvxcvi/test.carly "0.4.1"]
     [riddley "0.1.14"]
     [org.apache.spark/spark-core_2.11 "2.2.1"
      :exclusions [commons-codec
                   commons-net
                   log4j
                   org.apache.commons/commons-compress
                   org.scala-lang/scala-reflect
                   org.slf4j/slf4j-log4j12]]
     [org.apache.spark/spark-mllib_2.11 "2.2.1"
      :exclusions [log4j org.slf4j/slf4j-log4j12]]
     [com.thoughtworks.paranamer/paranamer "2.6"]]}

   :repl
   {:source-paths ["dev"]
    :dependencies
    [[clj-stacktrace "0.2.8"]
     [org.clojure/tools.namespace "0.2.11"]]}

   :coverage
   {:dependencies
    [[commons-logging "1.2"]]
    :jvm-opts ["-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.SimpleLog"
               "-Dorg.apache.commons.logging.simplelog.defaultlog=trace"]}})
