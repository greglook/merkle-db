(ns movie-lens.load
  "Tooling for loading data to construct a new MerkleDB table."
  (:require
    [blocks.core :as block]
    [clojure.data.csv :as csv]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [merkle-db.index :as index]
    [merkle-db.key :as key]
    [merkle-db.partition :as part]
    [merkle-db.record :as record]
    [merkle-db.table :as table]
    [merkledag.core :as mdag]
    [merkledag.node :as node]
    [sparkling.core :as spark]
    [sparkling.destructuring :as sde]))


(defn init-store
  "Initialize a MerkleDAG graph store from the given config."
  [cfg]
  ; TODO: make this more configurable...
  (mdag/init-store
    :store (block/->store (:blocks-url cfg))
    :cache {:total-size-limit (:cache-size cfg (* 1024 1024))}
    :types merkle-db.graph/codec-types))


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


(defn csv-rdd
  "Create an RDD from the given CSV file."
  [spark-ctx csv-file header parser]
  (->> (spark/text-file spark-ctx (str csv-file) #_4)
       (spark/filter (fn remove-header [line] (not= line header)))
       (spark/flat-map csv/read-csv)
       (spark/map parser)))


(defn- write-partitions
  "Make a sequence of partitions from the given records."
  [store-cfg table-params part-idx records]
  (log/debug "Processing spark partition" part-idx)
  (let [store (init-store store-cfg)
        parts (->> records
                   (map (sde/fn [(k r)] [k r]))
                   (part/partition-records store table-params)
                   (map inject-meta)
                   (vec))]
    (log/debugf "Encoded spark partition %d with %d records into %d table partitions"
                part-idx
                (reduce + 0 (map ::record/count parts))
                (count parts))
    ; TODO: strip out info not needed to build the index
    parts))


(defn- build-table-parts!
  "Constructs a sequence of partitions from the RDD of data maps. This causes
  Spark execution."
  [store-cfg table-params data]
  (->>
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
    (spark/map-partition-with-index
      (fn write-table-parts
        [idx record-iter]
        (->> (iterator-seq record-iter)
             (write-partitions store-cfg table-params idx)
             (.iterator))))
    (spark/collect)
    (map extract-meta)))


(defn build-table!
  "Load the dataset into tables in a merkle-db database."
  [store store-cfg table-params record-rdd]
  (let [parts (build-table-parts! store-cfg table-params record-rdd)
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
