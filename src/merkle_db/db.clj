(ns merkle-db.db
  (:require
    [clojure.future :refer [nat-int?]]
    [clojure.spec :as s]
    [merkledag.link :as link]
    [merkle-db.data :as data]
    [merkle-db.node :as node]
    [merkle-db.table :as table]))


;; ## Specs

;; Database name.
(s/def ::name (s/and string? #(<= 1 (count %) 512)))

;; Database version.
(s/def ::version nat-int?)

;; Map of table names to node links.
(s/def ::tables (s/map-of ::table/name link/merkle-link?))

;; Description of a specific version of a database.
(s/def ::version-info
  (s/keys :req [:merkledag.node/id
                ::name
                ::version
                :time/updated-at]))

;; Database root node.
(s/def ::root-node
  (s/keys :req [::tables]
          :opt [:data/title
                :data/description
                ::data/metadata
                :time/updated-at]))

;; Description of the database.
(s/def ::description
  (s/merge ::version-info
           ::root-node))



;; ## Database Protocols

(defprotocol IDatabase
  "Protocol for interacting with a database at a specific version."

  (describe-db
    [db]
    "Retrieve descriptive information about a database, including any user-set
    metadata.")

  (alter-db-meta
    [db f]
    "Update the user metadata attached to a database. The function `f` will be
    called with the current value of the metadata, and the result will be used
    as the new metadata."))


(defprotocol ITables
  "..."

  (create-table
    [db table-name opts]
    "...")

  (describe-table
    [db table-name]
    "...")

  (alter-table-meta
    [db table-name f]
    "...")

  (alter-families
    [db table-name new-families]
    "...")

  (drop-table
    [db table-name]
    "...."))


(defprotocol IRecords
  "..."

  (scan
    [db table-name opts]
    "...")

  (get-records
    [db table-name primary-keys fields]
    "...")

  (write
    [db table-name records]
    "...")

  (delete
    [db table-name primary-keys]
    "..."))


(defprotocol IPartitions
  "..."

  (list-partitions
    [db table-name]
    "...")

  (read-partition
    [db partition-id fields]
    "...")

  (build-table
    [db table-name partition-ids]
    "..."))



;; ## Database Type

(deftype Database
  [store db-name version root-id _meta]

  IDatabase

  (describe-db
    [this]
    (when-let [db-root (node/get-data store root-id)]
      (->
        (assoc db-root
               :merkledag.node/id root-id
               ::name db-name)
        (cond->
          (::data/metadata db-root)
            (assoc ::data/metadata (node/get-data store (::data/metadata db-root)))))))


  (alter-db-meta
    [this f]
    (let [db-root (node/get-data store root-id)
          db-meta (some->> (::data/metadata db-root) (node/get-data store))
          db-meta' (f db-meta)]
      (if (= db-meta db-meta')
        ; Nothing to change.
        this
        ; Store updated metadata node.
        (let [meta-node (node/store-node! store db-meta')
              meta-link (link/create "meta" (:id meta-node) (:size meta-node))
              db-node (node/store-node! store (assoc db-root ::data/metadata meta-link))]
          (assoc this :root-id (:id db-node)))))))


(alter-meta! #'->Database assoc :private true)
