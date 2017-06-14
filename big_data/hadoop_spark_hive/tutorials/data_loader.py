#!/usr/local/Cellar/spark/1.0.1/bin/pyspark
import commands
#---------------- Sparks
import pyspark as sparks
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

import data_explorer
import spark_sessions
from parameter_vault import *


#parameters
# web_port = "--webui-port 4041" # sparks port
# master = "local" # local host
# memory_id = "spark.executor.memory"
# memory_size = "500m" # use size of 500MB, minimum limit should be above ~ 450MB
# config_id = "spark.driver.cores"
# config_value = 1

# name = "test"
# config = SparkConf
# runing sparks
def spark_server(port): #runs the spark server
    # first stop the spark-master to free the port 44040
    commands.getstatusoutput("stop-spark-master")   
    commands.getstatusoutput("start-spark-master"+" "+port)
    


def spark_configurations(app):
    # run server
    spark_server(web_port)
    # call to start spark session using the paramters in 'parameter_vault'
    sc = spark_sessions.start_session(master, app, memory_id, memory_size)
    print "context initiated ..."
    
    return sc



def sparking_data(app_name):
    data = spark_configurations(app_name).textFile(data_explorer.hdfs_channel+data_explorer.raw_data).cache()
    sample = data.takeSample(False, 10, 2) # show randomly 10 lines
    print "A sample before mapreduce operations:"
    print "------------------------------------------"
    print sample  
    
    return data

def spark_kill(app):
    spark_sessions.stop_session(master, app, memory_id, memory_size)
    print "session ", app, " killed."


# self execution/testing
if __name__ == '__main__':
    app_name = "test"
    sparking_data(app_name)
    spark_kill(app_name)
    print "session stop"
    

