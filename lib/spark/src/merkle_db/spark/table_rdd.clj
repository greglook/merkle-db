(ns merkle-db.spark.table-rdd
  "A `TableRDD` represents the lazy application of a scan over the records in a
  table. The scan may be bounded by a minimum and maximum key to load just a
  subset of the records in the table, as well as specifying a subset of the
  fields to load for each record.

  The RDD effectively has the type `PairRDD[(Key, Map)]`, where the keys are
  the _serialized_ table keys and the values are fully-decoded records. A
  table scan RDD does not depend on any parent RDDs, since it is a direct data source."
  (:gen-class
    :name merkle_db.spark.TableRDD
    :state state
    :init init
    :main false
    :extends org.apache.spark.rdd.RDD
    :constructors {[org.apache.spark.SparkContext
                    clojure.lang.Fn
                    java.lang.Object  ; merkle_db.table.Table ?
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
    merkle_db.key.Key
    (merkle_db.spark
      KeyPartitioner
      TableRDD)
    (org.apache.spark
      Partition
      TaskContext)
    (org.apache.spark.api.java
      JavaPairRDD
      JavaSparkContext)
    scala.collection.JavaConversions
    scala.collection.mutable.ArrayBuffer
    scala.reflect.ClassManifestFactory$))


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

(defn- class-tag
  "Generates a Scala `ClassTag` for the given class."
  [^Class cls]
  (.fromClass ClassManifestFactory$/MODULE$ cls))


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

(defn ^:no-doc -init
  [spark-ctx init-store table scan-opts]
  [[spark-ctx (ArrayBuffer.) (class-tag scala.Tuple2)]
   {:init-store init-store
    :lexicoder (::key/lexicoder table :bytes)
    :primary-key (::table/primary-key table)
    :data-link (::table/data table)
    :patch-link (::table/patch table)
    :pending (.pending table)
    :scan-opts scan-opts}])


(defn ^:no-doc -partitioner
  [this]
  (let [parts (load-partitions this)]
    (scala.Option/apply
      (KeyPartitioner.
        (:lexicoder (.state this))
        (mapv ::record/first-key (rest parts))))))


(defn ^:no-doc -getPartitions
  [this]
  ; - if table has no pending changes, no patch tablet, and no data link,
  ;   return an empty rdd
  ; - if table has only pending changes and/or a patch link, emit an RDD with one
  ;   partition to represent the merged patch data
  ; - otherwise, determine which partitions must be loaded to satisfy the scan
  (->>
    (load-partitions this)
    (map-indexed #(->TablePartition %1 (::node/id (meta %2))))
    (into-array TablePartition)))


(defn ^:no-doc -compute
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
      (map (fn decode-record-tuple
             [entry]
             (spark/tuple
               (first entry)
               (record/decode-entry lexicoder primary-key entry))))
      (.iterator)
      (JavaConversions/asScalaIterator))))



;; ## RDD Construction

(defmacro with-op-scope
  "Apply a Spark context operation scope around the statements in the body."
  [spark-ctx scope-name & body]
  `(.withScope org.apache.spark.rdd.RDDOperationScope$/MODULE$
     ~spark-ctx ~scope-name true false
     (proxy [scala.Function0] []
       (apply [] ~@body))))


(defn scan
  ([spark-ctx init-store table]
   (scan spark-ctx init-store table nil))
  ([spark-ctx init-store table scan-opts]
   (let [lexicoder (@#'table/table-lexicoder table)
         min-k (some->> (:min-key scan-opts) (key/encode lexicoder))
         max-k (some->> (:max-key scan-opts) (key/encode lexicoder))
         sc (.sc ^JavaSparkContext spark-ctx)]
     (with-op-scope sc "merkle-db.table/scan"
       (-> (TableRDD.
             (.sc ^JavaSparkContext spark-ctx)
             init-store
             table
             (assoc scan-opts :min-key min-k, :max-key max-k))
           (JavaPairRDD/fromRDD
             (class-tag Key)
             (class-tag Object))
           (.setName (str "TableRDD: " (::table/name table "??"))))))))


; XXX: for assigning keys to records in a batch read or batch update, we need
; to use the same partitioner logic that Spark does, **NOT** the range bounds
; on each partition. Otherwise, some keys might fall 'between' partitions and
; get lost. This should also match the non-parallel key assignment in updates,
; so double-check that.
