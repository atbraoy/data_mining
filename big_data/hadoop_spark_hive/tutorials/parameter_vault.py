from hadoop_report import *


def validat_hadoop():
    command = "hadoop dfsadmin -report"
    report_file = 'hadoop_report.txt'
    try:
        filing(report_file, command)
        Name, skip, HDFS_channel, Host = channels(report_file)
        print "1: HDFS Channel ", HDFS_channel
        print "2: Host ", Host
        
    except OSError:
        print "Hdoop server is not running"
        print "Atempt to run hadoop server ..."
        serve = "hadoop"
        pipe(serve)
        
        

#HDFS parameters -------
hadoop_channel = "hdfs://localhost:9000/user"
raw_data = "/user/hive/warehouse/master/*.csv"

# command = "hadoop dfsadmin -report"
# report_file = 'hadoop_report.txt'
# filing(report_file, command)
# Name, skip, HDFS_channel, Host = channels(report_file)
# print "1: HDFS Channel ", HDFS_channel
# print "2: Host ", Host

#Spark parameters -------
web_port = "--webui-port 4041" # sparks port
master = "local" # local host
memory_id = "spark.executor.memory"
memory_size = "500m" # use size of 500MB, minimum limit should be above ~ 450MB
config_id = "spark.driver.cores"
config_value = 1