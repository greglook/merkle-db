(ns merkle-db.spark.table-rdd
  "Integration code for using MerkleDB with Spark."
  (:gen-class
    :name merkle_db.spark.TableRDD
    :state parts
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
    [merkle-db.index :as index]
    [merkle-db.key :as key]
    [merkle-db.partition :as part]
    [merkle-db.record :as record]
    [merkle-db.spark.table-partitioner :as partitioner]
    [merkle-db.table :as table]
    [merkledag.core :as mdag]
    [merkledag.node :as node]
    [sparkling.core :as spark]
    [sparkling.destructuring :as sde])
  (:import
    merkle_db.spark.table_partitioner.TablePartition
    merkle_db.spark.TablePartitioner
    org.apache.spark.TaskContext
    scala.collection.mutable.ArrayBuffer
    scala.reflect.ClassManifestFactory$))


(defn -init
  [spark-ctx init-store root-id]
  (let [rdd-deps (ArrayBuffer.)
        class-tag (.fromClass ClassManifestFactory$/MODULE$ Object)
        ; TODO: load parts
        parts nil]
    [[spark-ctx rdd-deps class-tag] parts]))


(defn -partitioner
  [this]
  (scala.Option/apply (TablePartitioner. (.parts this))))


(defn -partitions
  [this]
  (->> (.parts this)
       (map-indexed partitioner/table-partition)
       (into-array TablePartition)))


(defn -compute
  [this ^TablePartition split ^TaskContext task-context]
  ; TODO: load records from partition
  ; scala.collection.Iterator<T>
  (throw (RuntimeException. "NYI")))
