(ns merkle-db.record
  "Core record specs and functions."
  (:require
    [clojure.future :refer [any? nat-int? simple-keyword?]]
    [clojure.spec :as s]
    [merkle-db.key :as key]))


;; ## Record Keys

;; Count of the records contained under a node.
(s/def ::count nat-int?)

;; Data size in bytes.
(s/def ::size nat-int?)

;; Record key bytes.
(s/def ::key key/key?)

;; Marker for the first key value present in a range.
(s/def ::first-key ::key)

;; Marker for the last key value present in a range.
(s/def ::last-key ::key)



;; ## Record Entries

;; Unique identifier for the record, after decoding.
(s/def ::id any?)

;; Valid field key values.
(s/def ::field-key
  (s/or :n number?
        :s string?
        :k keyword?
        :y symbol?))

;; Projection of the record key to identity field values.
(s/def ::id-field
  (s/or :field ::field-key
        :tuple (s/coll-of ::field-key
                          :kind vector?
                          :min-count 1
                          :distinct true)))

;; Map of record field data.
(s/def ::data (s/map-of ::field-key any?))

;; Record key/data tuple.
(s/def ::entry (s/tuple ::key ::data))


(defn project-id
  "Projects a record into its uniquely-identifying value."
  [id-field record]
  (if (vector? id-field)
    (mapv record id-field)
    (get record (or id-field ::id))))


(defn encode-entry
  "Project a map of record data into an entry with the encoded key and a data
  map with the key fields removed."
  [lexicoder id-field record]
  (let [id-field (or id-field ::id)
        id (project-id id-field record)]
    [(key/encode lexicoder id)
     (if (vector? id-field)
       (apply dissoc record ::id id-field)
       (dissoc record ::id id-field))]))


(defn decode-entry
  "Merge a record entry with an encoded key into a record data map with the
  decoded id in associated key fields."
  [lexicoder id-field [rkey data]]
  (let [id-field (or id-field ::id)
        id (key/decode lexicoder rkey)]
    (vary-meta
      (if (vector? id-field)
        (merge data (zipmap id-field id))
        (assoc data id-field id))
      assoc ::id id)))



;; ## Family Functions

;; Valid family keys.
(s/def ::family-key simple-keyword?)

;; Map of family keywords to sets of contained fields.
(s/def ::families
  (s/and
    (s/map-of
      ::family-key
      (s/coll-of ::field-key :kind set?))
    #(= (reduce + (map count (vals %)))
        (count (distinct (apply concat (vals %)))))))


(defn- family-lookup
  "Build a lookup function for field families. Takes a map from family keys to
  collections of field keys in that family. Returns a function which accepts a
  field key and returns either the corresponding family or `:base` if it is not
  assigned one."
  [families]
  (let [lookup (into {}
                     (mapcat #(map vector (second %) (repeat (first %))))
                     families)]
    (fn [field-key] (lookup field-key :base))))


(defn family-groups
  "Build a map from family keys to maps which contain the field data for the
  corresponding family. Fields not grouped in a family will be added to
  `:base`. Families which had no data will have an entry with a `nil` value,
  except `:base` which will be an empty map."
  [families data]
  (let [field->family (family-lookup families)
        init (assoc (zipmap (keys families) (repeat nil))
                    :base {})]
    (reduce-kv
      (fn split-data
        [groups field value]
        (update groups (field->family field) assoc field value))
      init data)))


(defn split-data
  "Split new record values into collections grouped by family. Each configured
  family and `:base` will have an entry in the resulting map, containing a
  vector of record keys to new values. The values may be `nil` if the record
  had no data for that family, an empty map if the family is `:base` and all
  the record's data is in other families, or a map of field data belonging to
  that family.

  ```
  {:base [[#merkle-db/key \"00\" {:a 123}] ...]
   :bc [[#merkle-db/key \"00\" {:b true, :c \"cat\"}] ...]
   ...}
  ```"
  [families records]
  (reduce
    (fn append-updates
      [updates [record-key data]]
      (reduce-kv
        (fn assign-updates
          [updates family fdata]
          (update updates family (fnil conj []) [record-key fdata]))
        updates
        (family-groups families data)))
    {} records))



;; ## Updating Functions

(defn field-merger
  "Construct a new record updating function from the given merge spec. The
  resulting function accepts three arguments, the record id, the existing
  record data (or nil), and the new record data map.

  If the merge spec is a function, it will be called with
  `(f field-key old-val new-val)` for each field in the record and the
  field-key will be set to the resulting value.

  If the merge spec is a map, the keys will be used to look up a merge function
  for each field in the record, which will be called with `(f old-val new-val)`.
  If a field has no entry in the spec, the new value is always used."
  [spec]
  {:pre [(or (fn? spec) (map? spec))]}
  (let [merger (if (map? spec)
                 (fn merger
                   [fk l r]
                   (if-let [f (get spec fk)]
                     (f l r)
                     r))
                 spec)]
    (fn merge-fields
      [id old-data new-data]
      (reduce
        (fn [data field-key]
          (let [left (get old-data field-key)
                right (get new-data field-key)
                value (merger field-key left right)]
            (assoc data field-key value)))
        (empty old-data)
        (distinct (concat (keys old-data) (keys new-data)))))))
