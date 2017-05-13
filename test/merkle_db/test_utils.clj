(ns merkle-db.test-utils
  (:require
    [merkle-db.key :as key])
  (:import
    blocks.data.PersistentBytes))


(defmethod print-method PersistentBytes
  [x w]
  (print-method
    (tagged-literal
      'data/bytes
      (apply str (map (partial format "%02x") (seq x))))
    w))
