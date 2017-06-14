import data_explorer
from data_loader import sparking_data
from hdfs_remover import *

app_name = "experiment-0"

# mapper
def mapper(data, n, m):
    #pass
    # Use flatMpa(), spark function, to retun a flat lines, not a list as in map()
    wordcounts = data\
        .map(lambda x: x.replace(',',' ').replace('.',' ').replace('-',' ').lower()) \
        .flatMap(lambda x: x.split()).coalesce(m)\
        .map(lambda x: (x, 1))\
        .reduceByKey(lambda x,y:x+y,numPartitions=n)\
        .map(lambda x:(x[1],x[0]))\
        .sortByKey(True)
    
    return wordcounts

# counter
def counter(data):
    #pass
    linescount = data.count()
    
# word frequency counter: Finding frequent word bigrams
def frequency(data, m):
    # pass
    map_data = data.glom().map(lambda x: " ".join(x)).flatMap(lambda x: x.split(".")).coalesce(m)
    
    bigrams = map_data.map(lambda x:x.split()) \
        .flatMap(
            lambda x: [((x[i],x[i+1]),1) for i in range(0,len(x)-1)]
            )
    
    frequent_bigrams = bigrams.reduceByKey(lambda x,y:x+y) \
        .map(lambda x:(x[1],x[0])) \
        .sortByKey(True)
    
    sample = frequent_bigrams.take(20)
    print "Frequency is: -------- \n", sample
    
    return frequent_bigrams

# displaying the number of partition used in spark operations
def partitions_counts(data, n, m):
    def partitions_counter(id,iterator): 
         c = 0 
         for _ in iterator: 
              c += 1 
         yield (id,c)
    #counts = data.mapPartitionsWithSplit(partitions_counter).collectAsMap()
    counts = mapper(data, n, m).mapPartitionsWithSplit(partitions_counter).collectAsMap()
    
    print "Number of partitions: --- \n", counts
    
    return counts

    

    
