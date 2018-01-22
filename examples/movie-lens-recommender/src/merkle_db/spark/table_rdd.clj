(ns merkle-db.spark.table-rdd
  "A merkle-db table is represented in Spark by a pair RDD with _serialized_
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
    [multihash.core :as multihash]
    [sparkling.core :as spark]
    [sparkling.destructuring :as sde])
  (:import
    clojure.lang.ILookup
    (merkle_db.spark
      KeyPartitioner
      TableRDD)
    (org.apache.spark
      Partition
      TaskContext)
    scala.collection.mutable.ArrayBuffer
    scala.reflect.ClassManifestFactory$))


;; A `TableRDD` represents the lazy application of one of the table read
;; functions to a particular table. The RDD itself should contain just the
;; shared state necessary to produce its partitions on demand and - when
;; combined with one of those partitions - compute the records to load. The
;; implication here is that the RDD shouldn't cache its partitions, or else
;; every worker has to pay the cost of serializing them.
;;
;; A `TableRDD` effectively has the type `PairRDD[(Key, Map)]`, where the keys
;; are the _serialized_ keys and the values are fully-decoded records.
;;
;; There are three kinds of RDDs we can read:
;;
;; ### Keys RDD
;;
;; A keys-only RDD can efficiently return just the keys present in a table. The
;; RDD pair values will be maps containing only the primary key field(s).
;; Optionally, the data may be restricted by key range:
;;
;; - `:min-key`
;;   Return records with keys equal to or greater than the marker.
;; - `:max-key`
;;   Return records with keys equal to or less than the marker.
;;
;; ### Scan RDD
;;
;; This RDD represents a scan over some or all of the records in the table.
;;
;; - `:fields`
;;   Only return data for the selected set of fields. If provided, only
;;   records with data for one or more of the fields are returned, otherwise
;;   all fields and records (including empty ones) are returned.
;; - `:min-key`
;;   Return records with keys equal to or greater than the marker.
;; - `:max-key`
;;   Return records with keys equal to or less than the marker.
;;
;; A table scan RDD does not depend on any parent RDDs; the RDD itself must
;; contain any pending (unflushed) changes and a link to the table's patch
;; tablet, if any. The RDD also holds the graph store constructor and a link to
;; the table's data tree root.
;;
;; When the partitions for the table are requested, the RDD should load the
;; data tree to discern which partitions fall within the min/max key bounds.
;; Same for the partitioner; it might be worthwhile to cache the partition
;; splits if the partitioner is called repeatedly.
;;
;; Computing the records in a partition requires the node-id of the partition;
;; the key boundaries and fields should be available from the RDD.
;;
;; ### Read RDD
;;
;; Finally, a read RDD looks for specific records in the table. The keys in
;; each pair are shuffled using the partitioner from the table, then a worker
;; can compute the result by:
;; - loading the partition node by id
;; - iterate over keys from parent rdd partition, filter using membership bloom filter
;; - sort remaining keys and do a bulk-binary-search in the needed tablets
;; - return sorted iterator of found records
;;
;; - `:fields`
;;   Only return data for the selected set of fields. If provided, only
;;   records with data for one or more of the fields are returned, otherwise
;;   all fields and records are returned.


;; ## Table Partition

(deftype TablePartition
  [idx node-id]

  Partition

  (index
    [this]
    idx)


  ILookup

  (valAt
    [this k]
    (.valAt this k nil))


  (valAt
    [this k not-found]
    (case k
      ::idx idx
      ::node/id node-id
      not-found))


  Comparable

  (compareTo
    [this that]
    (if (= this that)
      0
      (compare idx (::idx that))))


  Object

  (toString
    [this]
    (format "TablePartition[%d: %s]" idx (multihash/base58 node-id)))


  (equals
    [this that]
    (boolean
      (or (identical? this that)
          (when (identical? (class this) (class that))
            (and (= idx (::idx that))
                 (= node-id (::node/id that)))))))


  (hashCode
    [this]
    (hash [(class this) idx node-id])))


(alter-meta! #'->TablePartition assoc :private true)



;; ## Partition Scan

(defn- load-partitions
  [this]
  (let [{:keys [init-store data-link scan-opts]} (.state this)
        store (init-store)]
    (map first (index/find-partition-range
                 store
                 (graph/get-link! store data-link)
                 (:min-key scan-opts)
                 (:max-key scan-opts)))))


(defn- load-part-changes
  "Return the table's patch changes as a map from keys to records or
  tombstones. This will load the table's patch tablet if present and merge in
  any locally pending changes."
  [store pending patch-link part]
  (let [first-key (::record/first-key part)
        last-key (::record/last-key part)]
    (into
      (sorted-map)
      (filter
        (fn select-changes
          [[k r]]
          (and (or (nil? first-key) (not (key/before? k first-key)))
               (or (nil? last-key) (not (key/after? k last-key))))))
      (concat
        (when-let [patch (mdag/get-data store patch-link)]
          (::patch/changes patch))
        pending))))



;; ## RDD Methods

(defn -init
  [spark-ctx init-store table scan-opts]
  (let [rdd-deps (ArrayBuffer.)
        class-tag (.fromClass ClassManifestFactory$/MODULE$ scala.Tuple2)]
    [[spark-ctx rdd-deps class-tag]
     {:init-store init-store
      :lexicoder (::key/lexicoder table :bytes)
      :primary-key (::table/primary-key table)
      :data-link (::table/data table)
      :patch-link (::table/patch table)
      :pending (.pending table)
      :scan-opts scan-opts}]))


(defn -partitioner
  [this]
  (let [parts (load-partitions this)]
    (scala.Option/apply
      (KeyPartitioner.
        (:lexicoder (.state this))
        (mapv ::record/first-key (rest parts))))))


(defn -getPartitions
  [this]
  (->>
    (load-partitions this)
    (map-indexed #(->TablePartition %1 (::node/id (meta %2))))
    (into-array TablePartition)))


(defn -compute
  [this ^TablePartition tpart ^TaskContext task-context]
  (let [{:keys [init-store lexicoder primary-key patch-link pending scan-opts]} (.state this)
        lexicoder (key/lexicoder lexicoder) ; TODO: improve terms
        fields (:fields scan-opts)
        min-k (:min-key scan-opts)
        max-k (:max-key scan-opts)
        store (init-store)
        part (graph/get-link! store (::node/id tpart))]
    (->>
      (patch/patch-seq
        (@#'table/filter-records
          scan-opts
          (load-part-changes store pending patch-link part))
        (when part
          (if (or min-k max-k)
            (part/read-range store part fields min-k max-k)
            (part/read-all store part fields))))
      ;^Iterable
      (map (fn decode-record-tuple
             [entry]
             (spark/tuple
               (first entry)
               (record/decode-entry lexicoder primary-key entry))))
      (.iterator)
      (scala.collection.JavaConversions/asScalaIterator))))



;; ## RDD Construction

; TODO: implement constructors

; table/scan
; - if table has no pending changes, no patch tablet, and no data link,
;   return an empty rdd
; - if table has only pending changes and/or a patch link, emit an RDD with one
;   partition to represent the merged patch data
; - otherwise, determine which partitions must be loaded to satisfy the scan
; - on load, each compute must:
;   - load-part-changes
;     - contain the pending table changes
;     - load the patch tablet
;     - merge and filter changes to just the relevant partition range
;   - read data from the partition tablets
;   - merge changes and partition data and return record sequence


; table/read
; - if table has no pending changes, no patch tablet, and no data link,
;   return an empty rdd
; - if table has only pending changes and/or a patch link, emit an RDD with one
;   partition to represent the filtered patch data
; - otherwise, determine which partitions must be loaded to satisfy the read
; - on load, each partition must:
;   - load-part-changes
;     - contain the pending table changes
;     - load the patch tablet
;     - merge and filter changes to just the relevant partition range
;   - read data from the partition tablets (filtering out keys with bloom filter)
;   - merge changes and partition data and return record sequence


(defn scan
  ([spark-ctx init-store table]
   (scan spark-ctx init-store table nil))
  ([spark-ctx init-store table scan-opts]
   (let [lexicoder (@#'table/table-lexicoder table)
         min-k (some->> (:min-key scan-opts) (key/encode lexicoder))
         max-k (some->> (:max-key scan-opts) (key/encode lexicoder))]
     (TableRDD.
       spark-ctx
       init-store
       table
       (assoc scan-opts :min-key min-k, :max-key max-k)))))


; XXX: for assigning keys to records in a batch read or batch update, we need
; to use the same partitioner logic that Spark does, **NOT** the range bounds
; on each partition. Otherwise, some keys might fall 'between' partitions and
; get lost. This should also match the non-parallel key assignment in updates,
; so double-check that.
