(ns cassandra-csv.core
  (:require
    [qbits.alia :as alia]
    [clojure.string :as string]
    [clojure.tools.cli :refer [parse-opts]]
    [clj-progress.core :as progress]
    [clojure.tools.logging :as log])
  (:use [cassandra-csv.cassandra]
        [cassandra-csv.import]
        [cassandra-csv.export])
  (:gen-class))

(def cli-options
  [["-H" "--hostname HOST" "Cassandra host"
    :default "localhost"]
   ["-f" "--file FILE" "CSV file name"
    :default "data.csv"]
   ["-s" "--fetch-size SIZE" "Fetch size for exporting large result sets to avoid request timeouts"
    :default 5000                                           ;; same as driver default
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 % 5001) "Must be a number between 1 and 5000"]]
   ["-q" "--cql CQL" "Cql query to run"]
   ["-i" "--import" "import or if not specified export"]
   ["-h" "--help"]])

(defn usage [options-summary]
  (string/join
    \newline
    ["This utility exports / imports cassandra records to / from csv file."
     ""
     "Options:"
     options-summary
     ""]))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (string/join \newline errors)))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn progress-init []
  (progress/set-progress-bar! "Processing [:wheel] :done done in :elapsed seconds")
  (progress/init 0))

(defn progress-done []
  (progress/done))

(defn -main
  "Export / import cassandra records to / from csv file"
  [& args]

  (log/info "Program arguments: " args)
  (let [{:keys [options errors summary]} (clojure.tools.cli/parse-opts args cli-options)]
    (cond
      (:help options) (exit 0 summary)
      (not-every? options [:hostname :cql]) (exit 1 (usage summary))
      errors (exit 1 (error-msg errors)))
    (let [cluster (get-cluster (:hostname options))
          session (get-session cluster)
          file-name (:file options)
          cql (:cql options)
          fetch-size (:fetch-size options)]
      (progress-init)
      (try
        (if (:import options)
          (do
            (log/info "Importing" file-name "in to cassandra")
            (import-csv-file file-name session cql))
          (do
            (log/info "Exporting cassandra in to" file-name)
            (export-csv-file file-name (alia/execute session cql {:fetch-size fetch-size}))))
        (finally
          (alia/shutdown cluster)
          (progress-done))))))
