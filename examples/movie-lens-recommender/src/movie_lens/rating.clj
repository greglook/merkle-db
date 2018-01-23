(ns movie-lens.rating
  (:require
    [clojure.spec.alpha :as s]
    [clojure.string :as str]
    [movie-lens.movie :as movie]
    [movie-lens.user :as user])
  (:import
    java.time.Instant))


;; Rating attributes.
(s/def ::score float?)
(s/def ::time inst?)


(s/def ::record
  (s/keys :req [::user/id
                ::movie/id
                ::score]
          :opt [::time]))


(def table-parameters
  {:merkle-db.table/name "ratings"
   :merkle-db.table/primary-key [::user/id ::movie/id]
   :merkle-db.key/lexicoder [:tuple :integer :integer]
   :merkle-db.index/fan-out 256
   :merkle-db.partition/limit 25000})


(def csv-header
  "userId,movieId,rating,timestamp")


(defn parse-row
  "Parse a row from the `ratings.csv` data file."
  [[user-id movie-id rating timestamp]]
  {::user/id (Long/parseLong user-id)
   ::movie/id (Long/parseLong movie-id)
   ::score (Double/parseDouble rating)
   ::time (Instant/ofEpochSecond (Long/parseLong timestamp))})
