import os
from pyspark.sql import SparkSession
# from pyspark.conf import SparkConf
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext


# Spark session: streamlines the number of configuration and helper classes you need to
#instantiate before writing Spark applications.
def start_session(master, app, memory_id, memory_size):
    # Was using first-->SparkSession.builder \
    # configurations = SparkConf()\
    #         .setMaster(host)\
    #         .setAppName(app) \
    #         .set(memory_id, memory_size)
    # Creating the resilient distributed dataset (RDD)
    #I use this combination with 'getOrCreate() to avoid conflict with a running SparkContext(conf=conf)
    
    _session = SparkSession \
                .builder.master(master) \
                .appName(app) \
                .config("spark.some.config.option", "some-value") \
                .getOrCreate()
    
    
    #_session = SparkContext.getOrCreate(conf=configurations)
    session = SQLContext(_session)
    #logs
    logs_dir = os.getcwd()
    logs_file = "Spark_logs.md"
    #_session.textFile(logs_dir+"/"+logs_file, 4)
    print "Spark session started ..."
    #print logs_dir+"/"+logs_file
    
    #session.read.format(sample)
    
    return session

def stop_session(host, app,  memory_id, memory_size):
    start_session(host, app, memory_id, memory_size).stop()


def spark_configurations(app):
    
    spark_server(web_port)
    conf = SparkConf()\
        .setMaster(master)\
        .setAppName(app)\
        .set(memory_config, memory_size)
    # Creating the resilient distributed dataset (RDD) 
    sc = SparkContext.getOrCreate(conf=conf)
    #I use this combination with 'getOrCreate() to avoid conflict with a running SparkContext(conf=conf)
    #sqlCtx = SQLContext(sc)
    
    return sc