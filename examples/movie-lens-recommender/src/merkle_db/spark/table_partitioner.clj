(ns merkle-db.spark.table-partitioner
  "Integration code for using MerkleDB with Spark."
  (:gen-class
    :name merkle_db.spark.TablePartitioner
    :state parts
    :init init
    :main false
    :extends org.apache.spark.Partitioner
    :constructors {[Object] []})
  (:require
    [merkle-db.key :as key]
    [merkle-db.partition :as part]
    [merkle-db.record :as record]
    [merkledag.link :as link]
    [merkledag.node :as node])
  (:import
    (org.apache.spark
      Partition
      Partitioner)))


; TODO: record or type?
(deftype TablePartition
  [idx
   node-id
   first-key
   last-key
   read-op
   fields
   read-args]

  Partition

  (index
    [this]
    idx)


  Object

  (toString
    [this]
    (format "TablePartition[%d: %s %s %s %s]"
            idx read-op node-id fields read-args))


  (equals
    [this that]
    (boolean
      (or (identical? this that)
          (when (identical? (class this) (class that))
            (let [that ^TablePartition that]
              (and (= idx (.idx that))
                   (= node-id (.node-id that))
                   (= read-op (.read-op that))
                   (= fields (.fields that))
                   (= read-args (.read-args that))))))))


  (hashCode
    [this]
    (hash [(class this) idx node-id read-op fields read-args])))


(alter-meta! #'->TablePartition assoc :private true)
;(alter-meta! #'map->TablePartition assoc :private true)


(defn table-partition
  [idx part]
  (->TablePartition
    idx
    (::node/id part)
    (::record/first-key part)
    (::record/last-key part)))


(defn -init
  [parts]
  [[] parts])


(defn -numPartitions
  [this]
  (count (.parts this)))


(defn -getPartition
  [this record-key]
  (let [num-parts (count (.parts this))]
    (loop [idx 0]
      (if (< idx num-parts)
        (let [part ^TablePartition (nth (.parts this) idx)]
          (if (key/before? record-key (.last-key part))
            idx
            (recur (inc idx))))
        num-parts))))


(defn table-partitioner
  "Construct a new table partitioner from the given partitions."
  [parts]
  (->>
    parts
    (map-indexed
      (fn [idx part]
        (->TablePartition
          idx
          (link/identify part)
          (::record/first-key part)
          (::record/last-key part))))
    (vec)
    (merkle_db.spark.TablePartitioner.)))
