from random import randrange
from random import random


#-----------------------------------------
def string_float_convert(dataset, column):
    """
    Takes a dataset and one of its columns and covert its string entries to float
    [returns float(string)]
    """
    for row in dataset:
        #print row
        row[column] = float(row[column].strip())

 
def string_int_convert(dataset, column):
    """
    Convert strings to int in dataset, if any
    [returns dictionary contains all integer values]
    """
    entries = [row[column] for row in dataset]
    unique = set(entries)
    entries_dict = dict()
    for i, value in enumerate(unique):
        entries_dict[value] = i
    for row in dataset:
        row[column] = entries_dict[row[column]]
    
    return entries_dict


def minima_maxima(dataset):
    """
    Measure all the minima and maxima of each column in a given dataset
    [return [max(column), min(column)] ]
    """
    #dataset_range = list()
    dataset_minmax = [[min(column), max(column)] for column in zip(*dataset)]
    
    return dataset_minmax


def normalization(dataset, minmax_values):
    """
    Normalization: put everything in the range of 0-1 
    Narmalization = (x-max(x))/(max(x)-min(x))
    """
    for row in dataset:
        for i in range(len(row)-1):
            row[i] = (row[i] - minmax_values[i][0]) / (minmax_values[i][1] - minmax_values[i][0])

# -----------------------------


