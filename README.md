## ad-analytics-spark

Sample application that demostrates the programming model of Apache Spark with a practical application that combines user motion data with physical display ad location to build both impression statistics and statistics about the linkages between pairs of advertising "frames".

### Installing Spark

    $ wget http://www.apache.org/dyn/closer.cgi/spark/spark-1.4.0/spark-1.4.0-bin-hadoop2.6.tgz
    $ tar xvf spark-1.4.0-bin-hadoop2.6.tgz
    $ cd spark-1.4.0.bin-hadoop2.6

### Provide Data

1. Fill locations.csv with location traces in CSV format with columns user_id, latitude, longitude, and timestamp.
2. Fill frames.csv with ad frame locations in CSV format with columns latitude, longitude, ad_id

### Submit the Spark Job

    $ path/to/spark/bin/spark-submit --master local main.py

Results are saved to the directories 'impressions' and 'connections'