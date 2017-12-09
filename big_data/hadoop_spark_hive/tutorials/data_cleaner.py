import os
from hdfs_remover import *

def clean(path, folder, data):
    try:
        data_list = data #["_SUCCESS", "part-00000", "part-00001"]
        for entries in data_list:
            hdfs_data_remover(path, folder, entries)
        hdfs_folder_remover(path, folder)
    except OSError:       
        pass
        print "no output data stored yet!"