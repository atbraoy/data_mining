#!/usr/local/Cellar/spark/1.0.1/bin/pyspark
import commands
#import time
from timeit import default_timer as timer
import pyspark as sparks
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext


#------ Hadoop services
# cleaning the output data first
def hdfs_cleaner():
    commands.getstatusoutput("hdfs dfs -ls hdfs://localhost:9000/user/")
    print "Removing old data files from data warhouse ... "
    commands.getstatusoutput("hdfs dfs -rm /user/output_data/_SUCCESS")
    print "data file --> removed"
    commands.getstatusoutput("hdfs dfs -rm /user/ahmed/book-output/part-00000")
    print "data file  --> removed"
    commands.getstatusoutput("hdfs dfs -rm -R /user/output_data/")
    print "data folder --> removed"
    print "Output data is removed"
    commands.getstatusoutput("hdfs dfs -ls hdfs://localhost:9000/user/")

# timer
def timing(start, end):
    elapsed_time = end - start
    elapsed_time = round(elapsed_time, 3)
    print "time taken to execute hadoop opertation: ",  elapsed_time, " seconds"
    
# excuting map-reduce through hadoop
def hadoop(command):
    commands.getstatusoutput(command)

# excuting sparks
def spark_server(port):
    #------ run teh spark server
    commands.getstatusoutput("stop-spark-master") # stop the spark-master to free the port 44040  
    commands.getstatusoutput("start-spark-master"+" "+port)
    #commands.getstatusoutput(command)


if __name__ == '__main__':
    start_time = timer()
    hdfs_cleaner()
    hadoop_command = "hadoop jar /usr/local/Cellar/hadoop/2.7.3/libexec/share/hadoop/tools/lib/hadoop-streaming-2.7.3.jar -file count_mapper.py -mapper count_mapper.py -file count_reducer.py -reducer count_reducer.py -input /user/hive/warehouse/master/*.csv -output /user/output_data"
    hadoop(hadoop_command)
    # time it takes to execute the hadoop opertation
    end_time = timer()
    timing(start_time, end_time)

    web_port = "--webui-port 4041"  
    spark_server(web_port)
    conf = SparkConf().setAppName("building a warehouse")
    sc = SparkContext(conf=conf)
    #sqlCtx = SQLContext(sc)
    
    start_time = timer()
    data = sc.textFile("hdfs://localhost:9000/user/output_data/part-00000").cache()
    lines = data.count()
    sample = data.takeSample(False, 10, 2) # show randomly 10 lines
    end_time = timer()
    timing(start_time, end_time)
    
    numAs = data.filter(lambda s: 'a' in s).count()
    numBs = data.filter(lambda s: 'b' in s).count()

    print "Number of lines in this dataset: %i " % lines, " lines"
    print "Lines with a: %i, lines with b: %i" % (numAs, numBs)
    print "Random 10 lines : ", sample

    sc.stop()
    commands.getstatusoutput("stop-spark-master"+" "+web_port)
