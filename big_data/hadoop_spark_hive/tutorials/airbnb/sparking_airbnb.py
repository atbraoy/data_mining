import os
import sys
import pandas as pd
from pyspark import SparkContext, sql, SparkConf
import nltk
import __builtin__
import numpy as np
from ast import literal_eval

#import pyspark.sql as sparksql
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row

#-------------------------------
# Importing from other folders, appending the path
sys.path.append('../')
from spark_sessions import start_session, stop_session



#-------- manipulating the data 
class Data_manipulate(object):
    def __init__(self):
        
        self.sample = sample
        self.keys = keys
        self.types = types
        self.schema = schema
        #self.framed = framed
        
        # --- functions:
        self.panda_framing()
        self.spark_framing()
        self.create_schema()
        self.prettySummary()
        self.spark_local_session()
    
    #----------------
    def structure_field(self, key, item): # item = StringType() ...
        key = str
        item = str
        structure = []
        structure.append(StructField(key, item, True))
        
        return structure
    
    #----------------
    def prettySummary(self):
        """ Neat summary statistics of a Spark dataframe
        Args:
            pyspark.sql.dataframe.DataFrame (df): input dataframe
        Returns:
            pandas.core.frame.DataFrame: a pandas dataframe with the summary statistics of df
        """
        #import pandas as pd
        temp = self.framed.describe().toPandas()
        temp.iloc[1:3,1:] = temp.iloc[1:3,1:].convert_objects(convert_numeric=True)
        pd.options.display.float_format = '{:,.2f}'.format
        
        return temp
    
    
    #----------------
    def panda_framing(self):
        # Frame the data with panda
        _panda_framed = pd.read_csv(sample)
        panda_framed = pd.DataFrame(_panda_framed)
        
        return panda_framed
    
    #----------------
    def spark_local_session(self):
        # conf = (SparkConf()
        #         .setAppName("experiment-airbnb")
        #         .set("spark.executor.instances", "10")
        #         .set("spark.executor.cores", 2)
        #         .set("spark.dynamicAllocation.enabled", "false")
        #         .set("spark.shuffle.service.enabled", "false")
        #         .set("spark.executor.memory", "500MB"))
        #         #.setMaster("local[2]"))
        # sc = SparkContext.getOrCreate(conf = conf)
        # 
        spark = SparkSession \
                .builder.master("spark://AhmedAbelrahman.attlocal.net:7077") \
                .appName("Word Count") \
                .config("spark.some.config.option", "some-value") \
                .getOrCreate()
        # spark = SparkSession.builder \
        #     .master("yarn") \
        #     .appName("experiment-airbnb") \
        #     .enableHiveSupport() \
        #     .getOrCreate()
        # 
        sc = SQLContext(spark) # Double check this!!!
        
        return sc
    #----------------
    def spark_framing(self):    
        host = "spark://AhmedAbelrahman.attlocal.net:7077" #"spark://localhost:8080" #
        app = "experiment-airbnb"
        memory_id = "spark.executor.memory"
        memory_size = "500m"      
        spark = self.spark_local_session()
        #start_session(host, app,  memory_id, memory_size) #spark is sc here
        #sc = SparkContext.getOrCreate()
        
        
        if sample.lower().endswith(('.csv', '.txt')):
            sparked_data = SQLContext.textFile(sample)
            # sparked_data = spark.read.format('com.databricks.spark.csv')\
            # .options(header='true', delimiter=',')\
            # .load(sample, inferSchema='true').cache() ## You need to check 'schema' here! 
            # sparked_data = spark.read.csv(sample, 
            #                   format='com.databricks.spark.csv', 
            #                   header='true', 
            #                  inferSchema='true').cache()
            print "Found data file with extension '.csv'/'txt'"
            print "Using '.load(data).cache()' method."
        else:
            sparked_data = SQLContext.read \
                        .format("com.databricks.spark.csv") \
                        .option("header", "true") \
                        .option("inferSchema", "true") \
                        .load(sample)
            print "Found format undetermined!"
        
        raw_data = spark.textFile(sample)
        
        
        _raw_schema = raw_data.first().replace('"','') # Creating a schema
        _schema = [str(key) for key in _raw_schema.split(',')] # mapping
        
        fields = []
        for key in range(len(self.keys)):
            if self.types[key] == str:
                data_type = StringType()
                str_struct = StructField(_schema[key], data_type, True)
                fields.append(str_struct)
                #print self.types[key]
            elif self.types[key] == int:
                data_type = DoubleType()
                int_struct = StructField(_schema[key], data_type, True)
                fields.append(int_struct) 
                #print self.types[key]
        
        headers = raw_data.filter(lambda l: "_id" in l)
        # NoHeaders = raw_data.subtract(headers)
        # schema = StructType(fields)
        # spark_dframed = NoHeaders.toDF(schema)    # Spark dataframe
        
        #panda_dframed = spark_dframed.toPandas() # also to pandas
        ## register data frame as a temporary table
        
        # spark_dframed.createOrReplaceTempView("test")
        # re=sqlContext.sql("select max_seq from test")
        # print(re.show())
        
        #results = sqlContext.sql("select first(name), name, first(host_id), concat_ws(';', collect_list(aspect)) as aspect from temp group by host_id")
        #spark_dframed.printSchema()
#         results = sqlContext.sql("SELECT product, count(*) AS total_count FROM id GROUP BY product ORDER BY total_count DESC")

#         for x in results.collect():
#             print x
        
        #-----------------------------------
        #_items = {}
        #_rows = []
        #for key in range(len(self.keys)):
        #    item = self.keys[key]
        #    _items[self.keys[key]] = key
        #_rows.append(_items)
        #rdd = sc.parallelize(_rows)
        
        #sqlContext = SQLContext(self.spark_local_session())
        #sqlContext.createDataFrame(rdd, schema).collect()
        #-----------------------------------
        
        #print df.describe().dtypes #df.describe().show()
        #print "Schema for this data is: \n", sparked_data.printSchema()
        #print df.select("host_name").show()
        #print df.groupBy("host_name").count().show()
        #print df.groupBy("neighbourhood").count().show()
        
        # self.framed = sparked_data
        
        #print self.prettySummary()
        #return sparked_data
    
    #----------------
    def create_schema(self):
        # Frame the data with panda, take first row of data
        first_row = self.panda_framing().iloc[0]
        #print "Number of lines in your 'sample' file:", len(self.panda_framing().index) 
        _keys = []
        for key in first_row.index:#dict(framed_data.dtypes):
            #if dict(framed_data.dtypes)[key] in ['float64', 'int64']:
            _keys.append(key)
        self.keys = _keys

        _types = []
        for _type in first_row.values:
            value = np.array(_type).tolist()
            #if not type(x) == str:
            _types.append(type(value))
  
        self.types = _types 
        
        struct_list = []
        for key in range(len(self.keys)):
                #keys = self.keys[key]
                if self.types[key] == str:
                    data_type = StringType()
                    str_struct = StructField(keys, data_type, True)
                    struct_list.append(str_struct)
                    #print self.types[key]
                elif self.types[key] == int:
                    data_type = DoubleType()
                    int_struct = StructField(keys, data_type, True)
                    struct_list.append(int_struct) 
                    #print self.types[key]
        
        self.schema = StructType(struct_list)
        #print self.schema
        
             
        return self.schema
    
    #----------------
    def spark_schema(self):
        self.spark_framing()
        

# self execution/testing
if __name__ == '__main__':
    sample = "/Users/Ahmed/Documents/DataMining_Stuff/Hadoop/Spark/PySpark/data/airbnb/sample/sample.csv"
    keys = ""
    types = ""
    schema = ""
    #schema = Data_manipulate(sample, keys).create_schema()
    manipulate = Data_manipulate()
    schema = manipulate.create_schema()# map(manipulate.create_schema(sample), (keys))
    #print schema
    manipulate.spark_schema()
    #print "Schema:", schema
