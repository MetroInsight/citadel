
import os
import sys

# Set the path for spark installation
# this is the path where you have built spark using sbt/sbt assembly
os.environ['SPARK_HOME'] = "/usr/local/spark"
# Append to PYTHONPATH so that pyspark could be found
sys.path.append("/usr/local/spark/python")
sys.path.append("/usr/local/spark/python/lib")


# Now we are ready to import Spark Modules
try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    print "Hey nice"
except ImportError as e:
    print ("Error importing Spark Modules", e)


from flask import Flask
app = Flask(__name__)

#Few Spark Stuff
conf = SparkConf()
conf.setMaster("spark://172.17.5.168:7077")
conf.setAppName("MetroInsights")
conf.set("spark.executor.memory", "1g")
sc = SparkContext(conf=conf)


@app.route('/')
def hello_world():
   lines = sc.textFile("/sparkData/520_0_3001375.csv")
   return str(lines.count()) #return the lines count

if __name__ == '__main__':
   app.run()
