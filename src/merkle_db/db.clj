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
