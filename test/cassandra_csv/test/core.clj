(ns cassandra-csv.test.core
  (:require
    [clojure.test :refer :all]
    [clojure.data :refer :all]
    [qbits.alia :refer :all]
    [qbits.alia.codec :refer :all]
    [qbits.alia.codec.joda-time :refer :all]
    [cassandra-csv.export :refer :all]
    [cassandra-csv.import :refer :all]
    [cassandra-csv.core :refer :all])
  (:import (java.io StringWriter StringReader)))

;; mostly borrowed from alia tests

(def ^:dynamic *cluster*)
(def ^:dynamic *session*)

;; some test data
(def items-csv-data "id,si,text\n1,1,prout\n0,0,prout\n")

(def imported-items [{:id   1
                      :si   1
                      :text "prout"}
                     {:id   0
                      :si   0
                      :text "prout"}])

(def counts-csv-data "id,count\n1,1\n0,0\n")

(def counts-csv-data2 "id,cnt\n5,5\n1,1\n8,8\n0,0\n2,2\n4,4\n7,7\n6,6\n9,9\n3,3\n")

(def imported-counts [{:count 1
                       :id    1}
                      {:count 0
                       :id    0}])

;; helpers

(use-fixtures
  :once
  (fn [test-runner]
    ;; prepare the thing
    (binding [*cluster* (cluster {:contact-points ["127.0.0.1"]})]
      (binding [*session* (connect *cluster*)]
        (try (execute *session* "DROP KEYSPACE cassandracsv;")
             (catch Exception _ nil))
        (execute *session* "CREATE KEYSPACE cassandracsv WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")
        (execute *session* "USE cassandracsv;")

        (execute *session* "CREATE TABLE items (
                    id int,
                    si int,
                    text varchar,
                    PRIMARY KEY (id)
                  );")

        (execute *session* "CREATE INDEX ON items (si);")

        (dotimes [i 2]
          (execute *session* (format "INSERT INTO items (id, text, si) VALUES(%s, 'prout', %s);" i i)))

        (execute *session* "CREATE TABLE counts (
                    id int,
                    count counter,
                    PRIMARY KEY (id)
                  );")

        (dotimes [i 2]
          (execute *session* (format "UPDATE counts SET count = count + %s WHERE id = %S;" i i)))

        (execute *session* "CREATE TABLE importeditems (
                    id int,
                    si int,
                    text varchar,
                    PRIMARY KEY (id)
                  );")

        (execute *session* "CREATE TABLE importedcounts (
                    id int,
                    count counter,
                    PRIMARY KEY (id)
                  );")

        ;; do the thing
        (test-runner)

        ;; destroy the thing
        (try (execute *session* "DROP KEYSPACE cassandracsv;") (catch Exception _ nil))
        (shutdown *session*)
        (shutdown *cluster*)
        (try (clojure.java.io/delete-file "test/resources/test-counts-export.csv") (catch Exception _ nil))
        (try (clojure.java.io/delete-file "test/resources/test-items-export.csv") (catch Exception _ nil))
        ))))

(use-fixtures

  :each

  (fn [test-runner]
    (progress-init)

    (test-runner)

    (execute *session* "TRUNCATE importeditems")
    (execute *session* "TRUNCATE importedcounts")

    (try (progress-done) (catch Exception _ nil))))

(deftest test-items-export-command-line
  (is (= items-csv-data
         (do
           (-main "-f" "test/resources/test-items-export.csv" "-q" "SELECT * FROM cassandracsv.items")
           (slurp "test/resources/test-items-export.csv")))))

(deftest test-items-import-command-line
  (is (= imported-items
         (do
           (-main "-f" "test/resources/items.csv" "-i" "-q" "INSERT INTO cassandracsv.importeditems (id, text, si) VALUES (:id, :text, :si)")
           (doall (execute *session* "SELECT * FROM importeditems"))))))

(deftest test-counts-export-command-line
  (is (= counts-csv-data
         (do
           (-main "-f" "test/resources/test-counts-export.csv" "-q" "SELECT * FROM cassandracsv.counts")
           (slurp "test/resources/test-counts-export.csv")))))

(deftest test-counts-import-command-line
  (is (= imported-counts
         (do
           (-main "-f" "test/resources/counts.csv" "-i" "-q" "UPDATE cassandracsv.importedcounts SET count = count + :count WHERE id = :id")
           (doall (execute *session* "SELECT * FROM importedcounts"))))))

(deftest test-simple-table-export
  (is (= items-csv-data
         (let [writer (StringWriter.)]
           (write-csv writer (execute *session* "SELECT * FROM items"))
           (.toString writer)))))

(deftest test-counter-table-export
  (is (= counts-csv-data
         (let [writer (StringWriter.)]
           (write-csv writer (execute *session* "SELECT * FROM counts"))
           (.toString writer)))))

(deftest test-simple-table-import
  (is (= imported-items
         (let [reader (StringReader. items-csv-data)]
           (process-csv reader *session* "INSERT INTO cassandracsv.importeditems (id, text, si) VALUES(:id, :text, :si)")
           (doall (execute *session* "SELECT * FROM importeditems"))))))

(deftest test-counter-table-import
  (is (= imported-counts
         (let [reader (StringReader. counts-csv-data)]
           (process-csv reader *session* "UPDATE cassandracsv.importedcounts SET count = count + :count WHERE id = :id")
           (doall (execute *session* "SELECT * FROM importedcounts"))))))

;; need better assertion or may be we should just raise error
(deftest test-counter-table-import-with-non-match-param
  (is (= '()
         (let [reader (StringReader. counts-csv-data2)]
           (process-csv reader *session* "UPDATE cassandracsv.importedcounts SET count = count + :count WHERE id = :id")
           (doall (execute *session* "SELECT * FROM importedcounts"))))))

