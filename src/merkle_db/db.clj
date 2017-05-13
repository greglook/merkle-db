(ns merkle-db.db
  (:require
    [clojure.spec :as s]
    [merkledag.link :as link]
    [merkle-db.data :as data]))


;; Map of table names to node links.
(s/def ::tables (s/map-of string? link/merkle-link?))

;; Database root node.
(s/def :merkle-db/db-root
  (s/keys :req [::tables]
          :opt [:data/title
                :data/description
                ::data/metadata
                :time/updated-at]))



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
    as the new metadata.")

  (commit!
    [db]
    "Ensure all data has been written to the backing block store and update the
    database's root value in the ref manager."))


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
  [store tracker db-name root-id])


(extend-type Database

  IDatabase

  (describe-db
    [this]
    (when-let [db-root (node/get-data store root-id)]
      (->
        (assoc db-root
               :merkledag.node/id root-id
               ::db/name db-name)
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
          (assoc this :root-id (:id db-node))))))


  (commit!
    [this]
    ; TODO: lock db
    ; TODO: check if current version is the same as the version opened at?
    (refs/set-ref! tracker db-name root-id)
    this))


(alter-meta! #'->Database assoc :private true)