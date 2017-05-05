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
               'merkledag.link.MerkleLink {'data/link (juxt :name :target :tsize)}
               'merkle_db.bloom.BloomFilter {'merkle-db.bloom.BloomFilter #(select-keys % [:bits :k])}
               'merkle_db.core.Database {'merkle-db.core.Database (juxt :db-name :root-id)}
               'multihash.core.Multihash {'data/hash 'multihash.core/base58}}}

  :profiles
  {:dev
   {:dependencies
    [[org.clojure/test.check "0.9.0"]
     [com.gfredericks/test.chuck "0.2.7"]]}

   :repl
   {:source-paths ["dev"]
    :dependencies
    [[clj-stacktrace "0.2.8"]
     [org.clojure/tools.namespace "0.2.11"]]

    :injections
    [(let [pct-var (ns-resolve (doto 'clojure.stacktrace require) 'print-cause-trace)
           pst-var (ns-resolve (doto 'clj-stacktrace.repl require) 'pst+)]
       (alter-var-root pct-var (constantly (deref pst-var))))]}})
