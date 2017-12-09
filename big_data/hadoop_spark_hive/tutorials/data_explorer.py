import commands


# parameters
hdfs_channel = "hdfs://localhost:9000"
raw_data = "/user/hive/warehouse/master/*.csv"
def warehouse_explore():
    print "HDFS lists:" 
    print commands.getstatusoutput("hdfs dfs -ls "+hdfs_channel+"/user/")
    
    print "These are the heads of the existed raw data ... "
    print "-------------------------------------------------"
    print commands.getstatusoutput("hadoop fs -cat "+hdfs_channel+raw_data+" | head")
    print "-------------------------------------------------"
    
    print "These are the tails of the existed raw data ... "
    print "-------------------------------------------------"
    print commands.getstatusoutput("hadoop fs -cat "+hdfs_channel+raw_data+" | tail")
    print "-------------------------------------------------"

# self execution/testing    
if __name__ == '__main__':
    data_list = warehouse_explore()
    