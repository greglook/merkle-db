(ns merkle-db.spark.table-rdd
  "A merkle-db table is represented in Spark by a pair RDD with deserialized
  keys and the full record as the value."
  (:gen-class
    :name merkle_db.spark.TableRDD
    :state state
    :init init
    :main false
    :extends org.apache.spark.rdd.RDD
    :constructors {[org.apache.spark.SparkContext
                    clojure.lang.Fn
                    java.lang.Object
                    java.lang.Object]
                   [org.apache.spark.SparkContext
                    scala.collection.Seq
                    scala.reflect.ClassTag]})
  (:require
    [clojure.tools.logging :as log]
    [merkle-db.graph :as graph]
    [merkle-db.index :as index]
    [merkle-db.key :as key]
    [merkle-db.partition :as part]
    [merkle-db.patch :as patch]
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
  [idx node-id first-key last-key read-fn]

  Partition

  (index
    [this]
    idx)


  Object

  (toString
    [this]
    (format "TablePartition[%d: %s (%s - %s)]"
            idx node-id first-key last-key))


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
        class-tag (.fromClass ClassManifestFactory$/MODULE$ scala.Tuple2)]
    [[spark-ctx rdd-deps class-tag]
     {:init-store init-store
      :lexicoder lexicoder
      :primary-key primary-key
      :parts (into []
                   (map-indexed
                     (fn [idx part]
                       (->TablePartition
                         idx
                         (::node/id part)
                         (::record/first-key part)
                         (::record/last-key part)
                         (::read-fn part))))
                   parts)}]))


(defn -partitioner
  [this]
  (let [{:keys [lexicoder parts]} (.state this)]
    (scala.Option/apply
      (KeyPartitioner.
        lexicoder
        (mapv #(.last-key ^TablePartition %)
              (butlast parts))))))


(defn -partitions
  [this]
  (into-array TablePartition (:parts (.state this))))


(defn -compute
  [this ^TablePartition split ^TaskContext task-context]
  (let [{:keys [init-store lexicoder primary-key]} (.state this)
        store (init-store)
        node-id (.node-id split)
        read-fn (.read-fn split)
        part (graph/get-link! store node-id)]
    (.iterator ^Iterable (read-fn store part))))
