(defproject cassandra-csv "0.3.0-SNAPSHOT"
  :description "Cassandra export / import to / from csv"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [cc.qbits/alia "2.8.0"]
                 [cc.qbits/hayt "3.0.0-rc2"]
                 [clj-time "0.11.0"]
                 [org.clojure/tools.cli "0.3.3"]
                 [org.clojure/data.csv "0.1.3"]
                 [intervox/clj-progress "0.2.1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.slf4j/slf4j-log4j12 "1.7.5"]
                 [log4j "1.2.17"]]
  :main ^:skip-aot cassandra-csv.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
