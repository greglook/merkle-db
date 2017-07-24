(ns merkle-db.index-test
  (:require
    [clojure.set :as set]
    [clojure.spec :as s]
    [clojure.test :refer :all]
    [clojure.test.check.generators :as gen]
    [com.gfredericks.test.chuck.clojure-test :refer [checking]]
    [merkledag.core :as mdag]
    (merkle-db
      [generators :as mdgen]
      [index :as index]
      [key :as key])))


; Index tree properties:
; - Root node is nil if tree is empty
; - Root is a partition if tree has fewer than p-1 records
; - Root node has between [2, b] children
; - Index nodes have one fewer key than child links
; - All keys in a subtree are within the correct bounds
; - ::record/count attributes are accurate
; - Index nodes with height = 1 have partitions as children
; - All internal index nodes have height one less than their parent
; - All internal index nodes have between [ceil(b/2), b] children
; - All node data passes spec
; - Partition nodes have at least ceil(p/2) records if the total number of records is at least that much.
