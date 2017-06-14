import data_cleaner
from data_explorer import *
from data_loader import sparking_data, spark_configurations, spark_kill
from hdfs_remover import *
# from mapreducer import mapper, counter, app_name
from spark_tools import frequency, mapper, counter, partitions_counts

app_name = "experiment-0"
# controlling the number of partitions generated by the reduce stage
number_of_partitions = 5 # number of partions spark will use
number_of_files = 2 # number of output data files, part-00000, part-00001


def warehouse_inspect():
    warehouse_explore()
    
def fitch_data():
    data = sparking_data(app_name)
    
    return data
    
if __name__ == '__main__':

    data = fitch_data()
    output_folder = "experiment-0/"
    path = hdfs_channel+"/user/output_data/"
    try:
        data_list = ["_SUCCESS", "part-00000", "part-00001"]
        data_cleaner.clean(path, output_folder, data_list)
    except OSError:
        pass

    sample = mapper(data, number_of_partitions, number_of_files).take(50)
    print "A sample after mapreduce operations:"
    print "------------------------------------------"
    print sample
    
    # saving the output data as a hdfs 
    mapper(data, number_of_partitions, number_of_files).saveAsTextFile(path+output_folder)
    
    #try:
    #    data_list = ["part-00000"]
    #    for sparks_out in data_list:
    #        data = spark_contxt(app_name).textFile(path+output_folder+sparks_out)
    #        print "Number of lines in mapreduced data: ", counter(data)
    #except OSError:       
        #pass
    #    print "No output data found!"
    output_folder = "experiment-0/frequency"
    path = hdfs_channel+"/user/output_data/"
    try:
        data_list = ["_SUCCESS", "part-00000", "part-00001"]
        data_cleaner.clean(path, output_folder, data_list)
    except OSError:
        pass
    frequency(data, number_of_files).saveAsTextFile(path+output_folder)
    
    partitions_counts(data, number_of_partitions, number_of_files)
    spark_kill(app_name)
    
    print "\n ********** job executed successfully.************"
