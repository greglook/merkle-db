(defproject mvxcvi/merkle-db "0.1.0-SNAPSHOT"
  :description "Hybrid data store built on merkle trees."
  :url "https://github.com/greglook/merkle-db"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :min-lein-version "2.7.0"

  :plugins
  [[lein-monolith "1.0.1"]
   [mvxcvi/lein-cljfmt "0.7.0-SNAPSHOT"]
   ;[lein-cloverage "1.0.9"]
   ]

  :dependencies
  [[org.clojure/clojure "1.9.0"]]

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

  :monolith
  {:inherit
   [:test-selectors
    :cljfmt
    :hiera
    :whidbey]

   :project-dirs
   ["core"
    "spark"
    "tools"]})
