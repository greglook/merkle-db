(ns merkle-db.spark.load
  "Code for loading MerkleDB tables with Spark."
  (:require
    [blocks.core :as block]
    [clojure.tools.logging :as log]
    [merkle-db.index :as index]
    [merkle-db.key :as key]
    [merkle-db.partition :as part]
    [merkle-db.record :as record]
    [merkle-db.spark.util :refer [with-job-group]]
    [merkle-db.table :as table]
    [merkledag.core :as mdag]
    [merkledag.node :as node]
    [sparkling.core :as spark]
    [sparkling.destructuring :as sde]))


(defn- inject-meta
  "Attach the metadata on `x` by associating it with a special key."
  [x]
  (if-let [m (meta x)]
    (assoc x ::meta m)
    x))


(defn- extract-meta
  "Convert injected metadata on `x` back into Clojure metadata and remove it
  from `x`."
  [x]
  (if-let [m (::meta x)]
    (vary-meta (dissoc x ::meta) merge m)
    x))


(defn- write-partitions
  "Make a sequence of partitions from the given records."
  [init-store table-params records]
  (let [store (init-store)
        parts (->> records
                   (map (juxt sde/key sde/value))
                   (part/partition-records store table-params)
                   (mapv inject-meta))]
    (log/debugf "Encoded spark partition with %d records into %d table partitions"
                (reduce + 0 (map ::record/count parts))
                (count parts))
    ; TODO: strip out info not needed to build the index
    parts))


(defn- build-table-parts!
  "Constructs a sequence of partitions from the RDD of data maps. This causes
  Spark execution."
  [init-store table-params data]
  (let [context (.context data) ; TODO: type-hint or pass explicitly
        table-name (or (:merkle-db.table/name table-params)
                       (str (gensym "unknown-")))
        records (->>
                  data
                  (spark/map-to-pair
                    (fn encode-record
                      [data]
                      (let [[k r] (record/encode-entry
                                    (key/lexicoder (::key/lexicoder table-params))
                                    (::table/primary-key table-params)
                                    data)]
                        (spark/tuple k r))))
                  (spark/sort-by-key)
                  (with-job-group
                    context
                    (str "sort:" table-name)
                    (str "Sorting " table-name " records")))]
    (->>
      records
      (spark/map-partition
        (fn write-table-parts
          [record-iter]
          (write-partitions init-store table-params (iterator-seq record-iter))))
      (spark/collect)
      (with-job-group
        context
        (str "write:" table-name)
        (str "Writing " table-name " partitions"))
      (mapv extract-meta))))


(defn build-table!
  "Load the dataset into tables in a merkle-db database."
  [init-store table-params record-rdd]
  (let [store (init-store)
        parts (build-table-parts! init-store table-params record-rdd)
        data-root (index/build-tree store table-params parts)]
    (-> table-params
        (assoc :data/type :merkle-db/table
               ::record/count (::record/count data-root 0))
        (cond->
          data-root
          (assoc ::table/data (mdag/link "data" data-root)))
        (dissoc ::node/id
                ::table/name
                ::table/patch)
        (->>
          (mdag/store-node! store nil)
          (::node/data)
          (table/load-table store (::table/name table-params))))))
