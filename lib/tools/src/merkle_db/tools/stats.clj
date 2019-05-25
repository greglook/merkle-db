(ns merkle-db.tools.stats
  "Tools for collecting and displaying statistics about a database."
  (:require
    [clojure.string :as str]
    [merkle-db.index :as index]
    [merkle-db.partition :as part]
    [merkle-db.patch :as patch]
    [merkle-db.record :as record]
    [merkle-db.table :as table]
    [merkle-db.tools.graph :as graph]
    [merkledag.link :as link]
    [merkledag.node :as node]))


(defn collect-table-stats
  "Calculate statistics about the structure of the given table."
  [^merkle_db.table.Table table]
  ;; TODO: pending/dirty info?
  (reduce
    (fn [stats data]
      (let [count-node (fn [s] (update s :count (fnil inc 0)))
            track (fn [m k new-data]
                    (let [s (get m k)
                          v (get new-data k)]
                      (assoc m
                             k
                             (-> s
                                 (update :min (fnil min v) v)
                                 (update :max (fnil max v) v)
                                 (update :sum (fnil + 0) v)))))]
        (case (:type data)
          :patch
          (assoc stats :patch (select-keys data [:size :changes]))

          :index
          (-> stats
              (update (:type data) count-node)
              (update (:type data) track :size data)
              (update (:type data) track :children data)
              (update-in [(:type data) :heights (:height data)] (fnil inc 0)))

          :partition
          (-> stats
              (update (:type data) count-node)
              (update (:type data) track :size data)
              (update (:type data) track :records data))

          :tablet
          (-> stats
              (update-in [(:type data) (:family data)] count-node)
              (update-in [(:type data) (:family data)] track :size data))

          ;; Unknown node type.
          (-> stats
              (update :unknown count-node)
              (update :unknown track :size data)
              (update-in [:unknown :types] (fnil conj #{}) (:type data))))))
    {}
    (graph/find-nodes-2
      (.store table)
      #{}
      (conj (clojure.lang.PersistentQueue/EMPTY)
            (::table/data table)
            (::table/patch table))
      (fn [node]
        (let [data (::node/data node)]
          (case (:data/type data)
            :merkle-db/patch
            [[{:type :patch
               :size (::node/size node)
               :changes (count (::patch/changes data))}]
             nil]

            :merkle-db/index
            [[{:type :index
               :size (::node/size node)
               :height (::index/height data)
               :children (count (::index/children data))}]
             (::index/children data)]

            :merkle-db/partition
            [(cons
               {:type :partition
                :size (::node/size node)
                :records (::record/count data)}
               (mapv
                 (fn [[family-key link]]
                   {:type :tablet
                    :size (::link/rsize link)
                    :family family-key})
                 (::part/tablets data)))
             ;; Don't visit tablets, we don't want to load the data.
             nil]

            ;; Some other node type.
            [[{:type (:data/type data)
               :size (node/reachable-size node)
               :external? true}]
             ;; Don't visit links from unknown node types.
             nil]))))))


(defn print-table-stats
  "Render the stats for a table."
  [stats]
  (let [patch-size (get-in stats [:patch :size])
        index-size (get-in stats [:index :size :sum])
        part-size (get-in stats [:partition :size :sum])
        tablet-sizes (map (comp :sum :size) (vals (:tablet stats)))
        unknown-size (get-in stats [:unknown :size :sum])
        table-size (reduce (fnil + 0 0) 0 (list* patch-size
                                                 index-size
                                                 part-size
                                                 unknown-size
                                                 tablet-sizes))
        table-pct #(* (/ (double %) table-size) 100.0)
        unit-str (fn unit-str
                   [scale units n]
                   (if (number? n)
                     (-> (->> (map vector (iterate #(/ (double %) scale) n) units)
                              (drop-while #(< scale (first %)))
                              (first))
                         (as-> [n u]
                           (if (integer? n)
                             (format "%d%s" n (or u ""))
                             (format "%.2f%s" (double n) (or u "")))))
                     (pr-str n)))
        byte-str (partial unit-str 1024 [" B" " KB" " MB" " GB" " TB" " PB"])
        count-str (partial unit-str 1000 ["" " K" " M" " B" " T"])
        stats-str (fn [data k f]
                    (format "min %s, mean %s, max %s"
                            (f (get-in data [k :min]))
                            (f (/ (get-in data [k :sum]) (:count data)))
                            (f (get-in data [k :max]))))]
    (println "Records:" (count-str (get-in stats [:partition :records :sum])))
    (println "Total size:" (byte-str table-size))
    (when-let [patch (:patch stats)]
      (printf "%d patch changes (%s, %.1f%%)\n"
              (:changes patch)
              (byte-str patch-size)
              (table-pct patch-size)))
    (when-let [index (:index stats)]
      (printf "%d index nodes (%s, %.1f%%)\n"
              (:count index)
              (byte-str index-size)
              (table-pct index-size))
      (printf "    Layers: %s\n"
              (->> (range)
                   (map #(get-in index [:heights (inc %)]))
                   (take-while some?)
                   (str/join " > ")))
      (println "    Sizes:" (stats-str index :size byte-str))
      (println "    Children:" (stats-str index :children count-str)))
    (when-let [part (:partition stats)]
      (printf "%d partitions (%s, %.1f%%)\n"
              (:count part)
              (byte-str part-size)
              (table-pct part-size))
      (println "    Sizes:" (stats-str part :size byte-str))
      (println "    Records:" (stats-str part :records count-str)))
    (doseq [family (cons :base (sort (clojure.core/keys (dissoc (:tablet stats) :base))))
            :let [tablet (get-in stats [:tablet family])]
            :when tablet]
      (printf "%d %s tablets (%s, %.1f%%)\n"
              (:count tablet)
              (name family)
              (byte-str (get-in tablet [:size :sum]))
              (table-pct (get-in tablet [:size :sum])))
      (println "    Sizes:" (stats-str tablet :size byte-str)))))
