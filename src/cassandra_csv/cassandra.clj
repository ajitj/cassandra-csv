(ns cassandra-csv.cassandra
  (:require [qbits.alia :as alia]))

(defn get-cluster [host] (alia/cluster {:contact-points [host]
                                        :query-options {:consistency :quorum}
                                        :socket-options {:read-timeout-millis 1000000}}))

(defn get-session [cluster] (alia/connect cluster))

