(ns merkle-db.spark.table-rdd
  "Integration code for using MerkleDB with Spark."
  (:gen-class
    :name merkle_db.spark.TableRDD
    :state state
    :init init
    :main false
    :extends org.apache.spark.rdd.RDD
    :constructors {[org.apache.spark.SparkContext
                    clojure.lang.Fn
                    multihash.core.Multihash]
                   [org.apache.spark.SparkContext
                    scala.collection.Seq
                    scala.reflect.ClassTag]})
  (:require
    [clojure.tools.logging :as log]
    [merkle-db.graph :as graph]
    [merkle-db.index :as index]
    [merkle-db.key :as key]
    [merkle-db.partition :as part]
    [merkle-db.record :as record]
    [merkle-db.spark.key-partitioner :as partitioner]
    [merkle-db.table :as table]
    [merkledag.core :as mdag]
    [merkledag.node :as node]
    [sparkling.core :as spark]
    [sparkling.destructuring :as sde])
  (:import
    merkle_db.spark.KeyPartitioner
    (org.apache.spark
      Partition
      TaskContext)
    scala.collection.mutable.ArrayBuffer
    scala.reflect.ClassManifestFactory$))


;; ## Table Partition

(deftype TablePartition
  [idx node-id read-fn]

  Partition

  (index
    [this]
    idx)


  Object

  (toString
    [this]
    (format "TablePartition[%d: %s %s]"
            idx node-id read-fn))


  (equals
    [this that]
    (boolean
      (or (identical? this that)
          (when (identical? (class this) (class that))
            (let [that ^TablePartition that]
              (and (= idx (.idx that))
                   (= node-id (.node-id that))))))))


  (hashCode
    [this]
    (hash [(class this) idx node-id])))


(alter-meta! #'->TablePartition assoc :private true)



;; ## RDD Methods

(defn -init
  [spark-ctx init-store lexicoder primary-key parts]
  (let [rdd-deps (ArrayBuffer.)
        class-tag (.fromClass ClassManifestFactory$/MODULE$ Object)]
    [[spark-ctx rdd-deps class-tag]
     {:init-store init-store
      :lexicoder lexicoder
      :primary-key primary-key
      :parts parts}]))


(defn -partitioner
  [this]
  (let [{:keys [lexicoder parts]} (.state this)]
    (scala.Option/apply (KeyPartitioner. lexicoder parts))))


(defn -partitions
  [this]
  (let [{:keys [init-store parts]} (.state this)]
    (->> parts
         (map-indexed
           (fn [idx part]
             (->TablePartition
               idx
               (::node/id part)
               (::read-fn part))))
         (into-array TablePartition))))


(defn -compute
  [this ^TablePartition split ^TaskContext task-context]
  (let [{:keys [init-store lexicoder primary-key]} (.state this)
        store (init-store)
        node-id (.node-id split)
        read-fn (.read-fn split)
        part (graph/get-link! store node-id)]
    (.iterator ^Iterable (read-fn store part))))
