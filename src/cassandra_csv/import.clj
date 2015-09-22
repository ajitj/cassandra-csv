(ns cassandra-csv.import
  (:require [qbits.alia :as alia]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clj-time.coerce :as tc]
            [clj-progress.core :as progress]
            [qbits.alia.codec.joda-time :refer :all]
            [clojure.tools.logging :as log])
  (:use [cassandra-csv.cassandra])
  (:import (com.datastax.driver.core DefaultPreparedStatement DataType DataType$Name)
           (java.net InetAddress)
           (java.util UUID)))

(defn prepare-statement [session cql]
  (alia/prepare session cql))

(defn convert-datatype [str-val ^DataType type]
  "Type convert string value"
  (let [type-name (.getName type)]
    (condp = type-name
      DataType$Name/ASCII     str-val
      DataType$Name/BIGINT    (Long/parseLong str-val)
      DataType$Name/BOOLEAN   (Boolean/parseBoolean str-val)
      ;DataType$Name/BLOB
      DataType$Name/COUNTER   (Long/parseLong str-val)
      DataType$Name/DECIMAL   (bigdec str-val)
      DataType$Name/DOUBLE    (Double/parseDouble str-val)
      DataType$Name/FLOAT     (Float/parseFloat str-val)
      DataType$Name/INET      (InetAddress/getByName str-val)
      DataType$Name/INT       (Integer/parseInt str-val)
      DataType$Name/TEXT      str-val
      DataType$Name/TIMESTAMP (tc/from-string str-val)
      DataType$Name/UUID      (UUID/fromString str-val)
      DataType$Name/VARCHAR   str-val
      DataType$Name/VARINT    (bigint str-val)
      DataType$Name/TIMEUUID  (UUID/fromString str-val))))

(defn process-csv [reader session cql]
  (let [ps (prepare-statement session cql)
        cdefs (.getVariables ^DefaultPreparedStatement ps)
        data (csv/read-csv reader)
        headers (map keyword (first data))
        params (vec (map (comp keyword #(.getName %)) (iterator-seq (.iterator cdefs))))
        len (count params)
        excess-param (clojure.set/difference (set params) (set headers))]
    (if (pos? (count excess-param))
      (log/error "Cql has extra param(s) not available as CSV column" (seq excess-param))
      (dorun (->> (rest data)
                 (map
                   (fn [row]
                     (let [raw-map (zipmap headers row)]
                       (loop [idx (int 0)
                              row-map (transient {})]
                         (if (= idx len)
                           (persistent! row-map)
                           (recur (unchecked-inc-int idx)
                                  (let [key (params idx)
                                        str-val (raw-map key)
                                        type (.getType cdefs idx)]
                                    (assoc! row-map key (convert-datatype str-val type)))))))))
                 (map progress/tick)
                 (map #(alia/execute-async session ps {:values % :error-handler (fn [r] (log/error r))})))))))

(defn import-csv-file [file-name session cql]
  "imports a csv file into cassandra"
  (with-open [in-file (io/reader file-name)]
    (process-csv in-file session cql)))




