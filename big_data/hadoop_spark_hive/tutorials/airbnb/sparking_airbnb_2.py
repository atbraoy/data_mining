import os
import sys
import pandas as pd
from pyspark import SparkContext, sql, SparkConf
import nltk
import re
from itertools import islice
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


spark_configs = (SparkConf()\
            .setAppName("experiment-airbnb")\
            .set("spark.executor.instances", "10")\
            .set("spark.executor.cores", 2)\
            .set("spark.dynamicAllocation.enabled", "false")\
            .set("spark.shuffle.service.enabled", "false")\
            .set("spark.executor.memory", "500MB"))

#----------------
def spark_session(version):
    if version == 0:
        conf = spark_configs
        return conf
    
    elif version == 1:
        spark = SparkSession.builder \
            .master("yarn") \
            .appName("experiment-airbnb") \
            .enableHiveSupport() \
            .getOrCreate()
            
        return spark
            

#----------------
def spark_data_v0(sample):
    # Creating "pyspark.context.SparkContext object"
    conf = spark_session(version=0)
    sc = SparkContext.getOrCreate(conf = conf)
    data = sc.textFile(sample)
    #print 'type of context for session v 0:', sc
    return data
    
#----------------
def spark_data_v1(sample):
    # Creating "pyspark.sql.context.SQLContext object"
    sc = SQLContext(spark_session(version=1))
    data = sc.read.load(sample,
                    format='com.databricks.spark.csv', 
                    header='true', 
                    inferSchema='true').cache()
    #print 'type of context for session v 1:', sc
        
    return data

#----------------
def spark_schema_v0(sample):
    data = spark_data_v0(sample)
    _keys = data.take(2)
#     keys_list = []
#     for _key in _keys[1:]:
#         print _key.split(',')
#     keys_list.append(_key)
    
#         #print "keys:", nltk.word_tokenize(_key)#_key.replace('"','')#.replace(',','')
        #print  u', '.join(_keys[1:])
            
    #print "token", nltk.word_tokenize(_keys[1:])#.replace('"','')
    schemaString = data.first().replace('"','') # Creating a schema
    #print schemaString
    #for i in schemaString.split(','): print i
    fields = [StructField(field_name, StringType(), True) 
              for field_name in schemaString.split(',')
             ]   
    _schema = [str(key) for key in schemaString.split(',')] # mapping
    #print fields

#----------------
def spark_schema_v1(sample):
    data = spark_data_v1(sample)
    
    _schema = data.columns # Creating a schema
    _keys = data.take(2)
    #print _schema
    _types = []
    keys = []
    for _key in _schema:
        keys.append(_key)#
    #print keys, _schema 
#         print key, _keys[0].minimum_nights
#         if type(_keys[0].key) == int:
#             data_type = DoubleType()
#             _types.append(key)
#             print 'hi, v1', data_type
#         elif type(_keys[0].key) == str:
#             data_type = StringType()
#             _types.append(key)
#             print 'hi, v1', data_type
#     print _keys

    #_schema = [str(key) for key in _raw_schema] # mapping
    schema = data.columns#.printSchema()
    #for i in schema: print i
#     print "from version (1):", map(str.split(), _keys[1:])
    
#     regexp = re.compile(r'name.*?([0-9.-]+)')
#     for line in _keys:
#         line = line.split()
#         match = regexp.match(line)
#         if match:
#             print match.group(1)


sample = "/Users/Ahmed/Documents/DataMining_Stuff/Hadoop/Spark/PySpark/data/airbnb/sample/sample.csv"
# spark_data_v0(sample)
# spark_data_v1(sample)

spark_schema_v0(sample)
spark_schema_v1(sample)
