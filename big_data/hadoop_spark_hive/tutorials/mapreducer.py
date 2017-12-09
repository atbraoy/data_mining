import data_explorer
from data_loader import sparking_data
from hdfs_remover import *

app_name = "experiment-0"

# spark mapper
def mapper(data):
    #pass
    # Use flatMpa(), spark function, to retun a flat lines, not a list as in map()
    wordcounts = data\
        .map(lambda x: x.replace(',',' ').replace('.',' ').replace('-',' ').lower()) \
        .flatMap(lambda x: x.split()).coalesce(1)\
        .map(lambda x: (x, 1))\
        .reduceByKey(lambda x,y:x+y)\
        .map(lambda x:(x[1],x[0]))\
        .sortByKey(False)
    
    return wordcounts

# spark counter
def counter(data):
    #pass
    linescount = data.count()

# self execution/testing
if __name__ == '__main__':
    
    output_folder = "experiment-0"
    path = data_explorer.hdfs_channel+"/user/output_data/"+output_folder
    try:
        data_list = ["_SUCCESS", "part-00000", "part-00001", output_folder]
        for sparks_out in data_list:
            hdfs_cleaner(path, sparks_out) 
    except OSError:       
        pass
        print "no output data stored yet!"
    
    sample = counter().take(50)
    print "A sample after mapreduce operations:"
    print "------------------------------------------"
    print sample
    
    # saving the output data as a hdfs 
    mapper().saveAsTextFile(path)