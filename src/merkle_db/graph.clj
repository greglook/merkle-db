(ns merkle-db.graph
  "Common code and utilities for working with MerkleDAG graphs."
  (:require
    [merkledag.core :as mdag]
    [merkledag.link :as link]
    [merkledag.node :as node]
    [merkle-db.bloom :as bloom]
    [merkle-db.key :as key])
  (:import
    java.time.Instant
    java.time.format.DateTimeFormatter
    merkle_db.bloom.BloomFilter
    merkle_db.key.Key))


;; ## Codecs

(def codec-types
  "Map of codec type information that can be used with MerkleDAG stores."
  {'inst
   {:description "An instant in time."
    :reader #(Instant/parse ^String %)
    :writers {Instant #(.format DateTimeFormatter/ISO_INSTANT ^Instant %)}}

   'merkle-db/key
   {:description "Record key byte data."
    :reader key/parse
    :writers {Key key/hex}}

   'merkle-db/bloom-filter
   {:description "Probablistic Bloom filter."
    :reader bloom/form->filter
    :writers {BloomFilter bloom/filter->form}}})



;; ## Utilities

(defn get-link!
  "Load the data for the given link, throwing an exception if the node is not
  found."
  ([store link]
   (get-link! store nil link))
  ([store node link]
   (let [node-id (::node/id (meta node))
         child (mdag/get-data store link nil ::not-found)]
     (when (identical? ::not-found child)
       (throw (ex-info (if node
                         (format "Broken child link from %s node %s to: %s"
                                 (:data/type node) node-id link)
                         (str "Broken child link to: " link))
                       {:parent node-id
                        :child link})))
     child)))
