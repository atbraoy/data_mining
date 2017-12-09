import os
import zipfile
from os import listdir
from os.path import isfile, join
import pandas as pd
import numpy as np
import csv

# loading from local model(s)
from data_handler import string_float_convert, string_int_convert, normalization, minima_maxima

#------------------------------------
def files_handler(data_dir, new_dir):
    """
    A function design specifcally to hadle the zipped files
    [creates new folder, unzip files, get files names, returns dir and files]
    """
    print 'current data directory:', data_dir
    files = [_file for _file in listdir(data_dir) if isfile(join(data_dir, _file))]

    dir_name = data_dir+new_dir+'/'
    unzip_dir = os.path.dirname(dir_name)
    if not os.path.exists(unzip_dir):
        print 'creating directory for unzipped files ...'
        os.makedirs(unzip_dir)
        print unzip_dir
        for _file in files:
            filename, file_extension = os.path.splitext(_file)
            if file_extension == '.zip':
                with zipfile.ZipFile(data_dir+_file,"r") as zip_file:
                    zip_file.extractall(unzip_dir)
        print 'Unzipped files:', files
    else: 
        print 'directory exists for unzipped files!'
        print unzip_dir
        files = [_file for _file in listdir(unzip_dir) if isfile(join(unzip_dir, _file))]
        print 'unzipped files:', files
        
    return unzip_dir, files


def header_handler(_dir,files):
    headers = []
    os.chdir(_dir)
    #base=os.path.basename('/root/dir/sub/file.ext')
    for _file in files:
        entity, ext = os.path.splitext(_file)
        headers.append(entity)
        
    return headers


# Load a CSV file
def csv_loader(_dir, _file):
    """
    Will look for a 'csv' files and load them
    Convert strings to float and int
    Creates a dataset (list)
    Measure the max and min values in the datastet
    [returns dataset, its length]
    """
    data = list()
    path_file = _dir+'/'+_file
    with open(path_file, 'r') as file:
        print 'data file found at:', _dir+'/'+_file
        csv_reader = csv.reader(file, delimiter='\t')
        for row in csv_reader:
            if not row:
                continue
            data.append(row)
    
    for i in range(len(data[0])-1):
        string_float_convert(data, i)

    string_int_convert(data, len(data[0])-1)

    min_max_values = minima_maxima(data)
    normalization(data, min_max_values)
    
    return data, min_max_values, len(data)

def get_data_dir():
    #pass
    """
    Get out of current 'src' directory,
    Move to train/test data directory to pull out the data
    [returns data directory]
    """
    print "current directory:", os.path.abspath(os.curdir)
    os.chdir("..") # get out current directory. We are certain it is '/src'

    # Move out to '/data' directroy
    os.chdir(os.path.abspath(os.curdir)+"/data/") # get insdie '/data' directory to save data files
    data_dir = os.path.abspath(os.curdir)
    print 'moved to directory:', data_dir
    print 'pulling data files ...'
    files = [_file for _file in os.listdir(data_dir) if os.path.isfile(os.path.join(data_dir, _file))]
    headers = header_handler(data_dir, files)
    
    for i in range(len(headers)):
        if files[i].lower().endswith(('.csv', '.json')):
            print files[i]
    
    # Get back to '/src' directory
    os.chdir("..")
    os.chdir(os.path.abspath(os.curdir)+"/src/")
    src_dir = os.path.abspath(os.curdir)
    print "back to working '/sr' directory:", src_dir
    
    return data_dir



