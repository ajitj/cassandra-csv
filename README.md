# cassandra-csv

Cassandra export / import to / from csv utility

I wrote this since I did not find anything that allowed me to

* Export cassandra records with cql where clause filtering
* Export cassandra records with large result set with specific fetch size to avoid timeout
* Import records with counter data type

I have used this with text, bigint, int, counter, timestamp data types. As long as the data type can be stored
in CSV format it should work

## Installation

* Install leiningen (http://leiningen.org/)
* Clone this repository
* From project folder run "lein uberjar"

## Usage

Run the jar as follows and it will list the supported options

    $ java -jar cassandra-csv-<version>-SNAPSHOT-standalone.jar

## Examples

See the tests for some examples