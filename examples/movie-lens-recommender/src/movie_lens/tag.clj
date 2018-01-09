(ns movie-lens.tag
  (:require
    [clojure.spec.alpha :as s]
    [clojure.string :as str]
    [movie-lens.movie :as movie]
    [movie-lens.user :as user])
  (:import
    java.time.Instant))


;; Tag attributes.
(s/def ::value string?)
(s/def ::time inst?)


(s/def ::record
  (s/keys :req [::user/id
                ::movie/id
                ::score]
          :opt [::time]))


(def table-parameters
  {:merkle-db.table/primary-key [::user/id ::movie/id ::value]
   :merkle-db.key/lexicoder [:tuple :integer :integer :string]
   :merkle-db.index/fan-out 256
   :merkle-db.partition/limit 10000})


(def csv-header
  "userId,movieId,tag,timestamp")


(defn parse-row
  "Parse a row from the `tags.csv` data file."
  [[user-id movie-id tag timestamp]]
  {::user/id (Long/parseLong user-id)
   ::movie/id (Long/parseLong movie-id)
   ::value tag
   ::time (Instant/ofEpochSecond (Long/parseLong timestamp))})
