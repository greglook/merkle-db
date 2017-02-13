(ns merkle-db.core)


;; ## Data Types

; TODO: should probably use deftypes here
(defrecord Connection
  [block-store
   ref-tracker
   key-codec
   data-codec])


(alter-meta! #'->Connection assoc :private true)
(alter-meta! #'map->Connection assoc :private true)


(defrecord Database
  [connection
   ,,,])


(alter-meta! #'->Database assoc :private true)
(alter-meta! #'map->Database assoc :private true)



;; ## Connections

(defn connect
  [& {:as opts}]
  (map->Connection opts))


(defn create-db!
  "Initialize a new database. An initial `:root` value may be provided,
  allowing for cheap database copy-on-write cloning."
  [conn db-name & {:keys [root metadata]}]
  (throw (UnsupportedOperationException. "NYI")))


(defn open-db
  "Open a database for use."
  [conn db-name]
  (throw (UnsupportedOperationException. "NYI")))


(defn drop-db!
  "Drop a database from the backing tracker. Note that this will not remove the
  block data, as it may be shared."
  [conn db-name]
  (throw (UnsupportedOperationException. "NYI")))



;; ## Databases

,,,
