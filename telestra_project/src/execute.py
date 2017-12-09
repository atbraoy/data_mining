import os
import pandas as pd
import numpy as np

# loading from local model(s)
from file_handler import files_handler, header_handler
from jupyter_to_pdf import jupyter_convertor
from file_handler import csv_loader, header_handler, get_data_dir
from data_handler import string_float_convert, string_int_convert, minima_maxima, normalization
from backpropagation import back_propagation
from evaluator import score_measure
from neural_parameters import neural_parameters_read, neural_parameters_create


def execute_model(_dir, _file, json_file):
    print '-'*20, '\n', 'starting process ...'
    """
     A main execution function:
    - step (1): load the 'train/test' csv file, do some measurements on dataset, return dataset
    - step (2): load the neural network model tuning parameters.
        Note: To modify the neural network parameters (e.g., folds, learning rate, ..., etc) please refer to
        ../src/neural_parameters.py
    - step (3): calling function: 'score_measure()' triggers a series of models that all compiles towards
    a complete 'back-propagation deep learning' model
    
    'Enjoy :-)'
    By Ahmed Abdelrahman (atbraoy@gmail.com)
    """
    # step (1)
    dataset, min_max_values, dataset_length = csv_loader(_dir, _file)
    
    # step (2)
    json_file_path = _dir+json_file
    if os.path.isfile(json_file_path) and os.access(json_file_path, os.R_OK): # os.R_OK to make sure it is readable
        neural_data = neural_parameters_read(_dir, json_file)
        n_folds = neural_data['parameters'][0]['n_folds']
        l_rate = neural_data['parameters'][0]['learning_rate']
        n_epoch =  neural_data['parameters'][0]['n_epoch']
        hidden_layers = neural_data['parameters'][0]['hidden_layers']
        #print "File exists and is readable"
        
    else:
        print "no new 'neural' model paramters found, will use 'default/existing' paramters."
        print "to modify/tune the model parameters please refer to '../src/neural_parameters.py'"
        neural_data = neural_parameters_create(_dir, json_file)
        #neural_data = neural_parameters_read(_dir, json_file)
        n_folds = neural_data['parameters'][0]['n_folds']
        l_rate = neural_data['parameters'][0]['learning_rate']
        n_epoch =  neural_data['parameters'][0]['n_epoch']
        hidden_layers = neural_data['parameters'][0]['hidden_layers']
    
    #step (3)
    scores = score_measure(dataset, back_propagation, n_folds, l_rate, n_epoch, hidden_layers)
    print('All scores (measured accuracies): %s' % scores)
    print('Mean accuracy: %.3f%%' % (sum(scores)/float(len(scores))))
    
    # end of process
    print 'process complete :).'

# 

if __name__ == '__main__':
    """
    For a faster execution (to test thte model), please use 'dumy_data.csv' stored in '/model_data'
    First run the model with 'train_data.csv' and then try testing with 'test_data.csv'
    """
    _dir = get_data_dir()
    json_file = 'neural_parameters.json'
    data_dir = _dir+'/model_data'
    _file = 'train_data.csv'
    execute_model(data_dir, _file, json_file)
        
        
