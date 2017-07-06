(ns merkle-db.patch
  "Patches are applied on top of tables in order to efficiently store changes
  while re-using old indexed data."
  (:require
    [clojure.future :refer [pos-int?]]
    [clojure.spec :as s]
    (merkle-db
      [key :as key])))


(def data-type
  "Value of `:data/type` that indicates a tablet node."
  :merkle-db/patch)

(def tombstone
  "Value which marks the deletion of a record."
  ::tombstone)


(defn tombstone?
  "Returns true if the value x is a tombstone."
  [x]
  (identical? ::tombstone x))



;; ## Specs

;; Maximum number of changes to allow in a patch tablet.
(s/def ::limit pos-int?)

;; Records are stored as a key/data-or-tombstone pair.
(s/def ::change-entry
  (s/tuple key/bytes? (s/or :record map? :tombstone tombstone?)))

;; Sorted vector of record entries.
(s/def ::changes
  (s/coll-of ::change-entry :kind vector?))

;; Tablet node.
(s/def :merkle-db/patch
  (s/keys :req [::changes]))
