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
   [bigml/sketchy "0.4.1"]
   [mvxcvi/blocks "0.10.0-SNAPSHOT"]
   [mvxcvi/merkledag "0.2.0-SNAPSHOT"]]

  :hiera
  {:cluster-depth 2
   :vertical false
   :show-external true
   :ignore-ns #{clojure bigml}}

  :whidbey
  {:tag-types {'blocks.data.Block {'blocks.data.Block (partial into {})}
               'blocks.data.PersistentBytes {'data/bytes #(apply str (map (partial format "%02x") (seq %)))}
               'merkle-db.bloom.BloomFilter {'merkle-db.bloom.BloomFilter #(select-keys % [:bits :k])}
               'multihash.core.Multihash {'data/hash 'multihash.core/base58}}}

  :profiles
  {:dev
   {:dependencies
    [[org.clojure/test.check "0.9.0"]
     [com.gfredericks/test.chuck "0.2.7"]]}

   :repl
   {:source-paths ["dev"]}})
