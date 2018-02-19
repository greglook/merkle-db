(ns merkle-db.spark.key-partitioner
  "Spark partitioner that knows how to lexicode assign records to table
  partitions."
  (:gen-class
    :name merkle_db.spark.KeyPartitioner
    :state state
    :init init
    :main false
    :extends org.apache.spark.Partitioner
    :constructors {[Object Object] []})
  (:require
    [merkle-db.key :as key])
  (:import
    merkle_db.spark.KeyPartitioner
    org.apache.spark.Partitioner))


(defn ^:no-doc -init
  [lexicoder splits]
  [[] {:lexicoder lexicoder, :splits splits}])


(defn ^:no-doc -numPartitions
  [this]
  (inc (count (:splits (.state this)))))


(defn ^:no-doc -getPartition
  [this key-value]
  ; TODO: should this deal with lexicoder?
  (let [{:keys [lexicoder splits]} (.state this)
        record-key (key/encode lexicoder key-value)]
    (loop [idx 0]
      (if (< idx (count splits))
        (if (key/before? record-key (nth splits idx))
          idx
          (recur (inc idx)))
        idx))))


(defn key-partitioner
  "Construct a new table partitioner from the given key lexicoder and
  partition splits."
  [lexicoder splits]
  (KeyPartitioner. lexicoder splits))
