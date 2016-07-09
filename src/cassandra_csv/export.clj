(ns cassandra-csv.export
  (:require [clojure.java.io :as io]
            [clj-progress.core :as progress]
            [clojure.data.csv :as csv]))

(defn write-csv
  "converts a sequence of maps to csv
   ([writer map-sequencei & {:as opts}])
  passes options to clojure.data.csv"
  [writer map-sequence & {:as opts}]
  (let [opts (vec (reduce concat (vec opts)))
        header (vec (keys (first map-sequence)))
        data (map (fn [line]
                    (vec (map (fn [item]
                                (get line item))
                              header)))
                  map-sequence)]
    (apply csv/write-csv writer (cons (map name header)
                                      (map progress/tick data)) opts)))

(defn export-csv-file [file-name map-sequence]
  "writes a csv file given a csv sequence"
  (with-open [out-file (io/writer file-name)]
    (write-csv out-file map-sequence)))