# FlaskWebService
A simple flask web service to store and retrieve data

## Current Status of the WebService

* Spark
  * Local Spark Cluster is set up at UCLA.
  * Total RAM available to the cluster is ~60GB and 24 cores each of 2.40GHz.
  * pyspark is used to submit the jobs from the Webservice to the Spark Cluster.

* Python Code
  * Currently flask is used to set up a simple server.
  * The code initializes the spark environment variables and binds to the spark master at runtime.
  * For a simple example, when the request arrives, it opens the csv file from building data and counts the number of datapoints(lines) in it.

