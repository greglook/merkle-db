(ns movie-lens.util
  (:require
    [clojure.string :as str]
    [merkledag.link :as link]
    [multihash.core :as multihash]
    [puget.printer :as puget]))


(defmethod print-method multihash.core.Multihash
  [x w]
  (print-method (tagged-literal 'data/hash (multihash.core/base58 x)) w))


(def pprinter
  (-> (puget/pretty-printer
        {:print-color true
         :width 200})
      (update :print-handlers
              puget.dispatch/chained-lookup
              {multihash.core.Multihash
               (puget/tagged-handler 'data/hash multihash/base58)

               merkledag.link.MerkleLink
               (puget/tagged-handler 'merkledag/link link/link->form)})))


(defn pprint
  "Pretty print something."
  [x]
  (puget/render-out pprinter x))


(defn duration-str
  "Convert a duration in seconds into a human-readable duration string."
  [elapsed]
  (if (< elapsed 60)
    (format "%.2f sec" (double elapsed))
    (let [hours (int (/ elapsed 60 60))
          minutes (mod (int (/ elapsed 60)) 60)
          seconds (mod (int elapsed) 60)]
      (str (if (pos? hours)
             (format "%d:%02d" hours minutes)
             minutes)
           (format ":%02d" seconds)))))
