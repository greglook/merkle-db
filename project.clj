(defproject mvxcvi/merkle-db "0.1.1-SNAPSHOT"
  :description "Hybrid data store built on merkle trees."
  :url "https://github.com/greglook/merkle-db"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :aliases
  {"combined" ["monolith" "with-all" "with-profile" "+combined"]}

  :min-lein-version "2.7.0"
  :pedantic? :abort

  :plugins
  [[lein-cloverage "1.1.1"]
   [lein-cprint "1.3.1"]
   [lein-hiera "1.1.0"]
   [lein-monolith "1.2.1"]
   [mvxcvi/lein-cljfmt "0.7.0"]
   [mvxcvi/puget "1.1.2"]]

  :dependencies
  [[org.clojure/clojure "1.10.0"]
   [mvxcvi/merkle-db-core "0.1.1-SNAPSHOT"]
   [mvxcvi/merkle-db-spark "0.1.1-SNAPSHOT"]
   [mvxcvi/merkle-db-tools "0.1.1-SNAPSHOT"]]

  :monolith
  {:project-dirs ["lib/*"]
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
   :single-import-break-width 40
   :indents {checking [[:block 2]]
             check-system [[:block 2]]
             check [[:block 1]]
             valid? [[:block 1]]
             invalid? [[:block 1]]}}

  :hiera
  {:cluster-depth 2
   :vertical false
   :show-external false
   :ignore-ns #{clojure bigml merkle-db.tools.validate}}

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
    [[org.clojure/data.csv "0.1.4"]]}

   :repl
   {:source-paths ["dev"]
    :dependencies
    [[clj-stacktrace "0.2.8"]
     [org.clojure/tools.namespace "0.2.11"]]}

   :combined
   {:dependencies
    [[org.clojure/test.check "0.9.0"]
     [com.gfredericks/test.chuck "0.2.8"]
     [com.thoughtworks.paranamer/paranamer "2.8"]
     [commons-logging "1.2"]
     [mvxcvi/test.carly "0.4.1"]
     [org.apache.spark/spark-core_2.11 "2.4.3"
      :exclusions [commons-codec
                   commons-net
                   log4j
                   org.apache.commons/commons-compress
                   org.scala-lang/scala-reflect
                   org.slf4j/slf4j-log4j12]]
     [org.apache.spark/spark-mllib_2.11 "2.4.3"
      :exclusions [commons-codec
                   log4j
                   org.slf4j/slf4j-log4j12]]]

    :aot
    [merkle-db.spark.key-partitioner
     merkle-db.spark.table-rdd]

    :jvm-opts ["-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.SimpleLog"
               "-Dorg.apache.commons.logging.simplelog.defaultlog=trace"]}})
