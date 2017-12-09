import os
import json
from pprint import pprint

# loading from local model(s)
from file_handler import header_handler, get_data_dir

"""
I use the json format below assuming that there will be more than just one 'parameters',
as well as more than one 'json' file.
If need to change parameters:
    - Directly edit the json file, or 
    - Edit data{} as below,
    - Execute the function neural_parameters_create()
"""
# Below is a sample data (can modifiy): ------

data = {}  
data['parameters'] = []  
data['parameters'].append({
    'hidden_layers':8,
    'n_folds': 4,
    'learning_rate': 0.75,
    'n_epoch': 200
})

#---------------------------------------------
def neural_parameters_create(_dir, json_file):
    os.chdir("..") # get out current directory. We are certain it is '/src'

    # Move out to '/data' directroy
    os.chdir(os.path.abspath(os.curdir)+"/data/") # get insdie '/data' directory to save data files
    data_dir = os.path.abspath(os.curdir)

    files = [_file for _file in os.listdir(data_dir) if os.path.isfile(os.path.join(data_dir, _file))]
    headers = header_handler(data_dir, files)
    json_path = _dir+'/'+json_file
    for i in range(len(headers)):
        if files[i].lower().endswith(('.json')) and files[i] == json_file:
            print "reading neural parameters from existing json file:", files[i]
            with open(json_file) as data_file:    
                json_data = json.load(data_file)
                pprint(json_data)
            
            with open(json_file, 'w') as outfile:  
                json.dump(data, outfile)
                json_path = _dir+'/'+json_file
                print 'new neural parameters dumped (modified) at file:', json_path
        
        else:
            with open(json_file, 'w') as outfile:  
                json.dump(data, outfile)
                json_path = _dir+'/'+json_file
                print 'neural parameters dumped at file:', json_path
                
            with open(json_file) as data_file:    
                json_data = json.load(data_file)
                pprint(json_data)
    
                return json_data
        
def neural_parameters_read(_dir, json_file):
    os.chdir("..") # get out current directory. We are certain it is '/src'

    # Move out to '/data' directroy
    os.chdir(os.path.abspath(os.curdir)+"/data/") # get insdie '/data' directory to save data files
    data_dir = os.path.abspath(os.curdir)

    files = [_file for _file in os.listdir(data_dir) if os.path.isfile(os.path.join(data_dir, _file))]
    headers = header_handler(data_dir, files)
    
    for i in range(len(headers)):
        if files[i].lower().endswith(('.json')) and files[i] == json_file: # 'and' locks it to read only the given json file
            print "reading neural parameters from json file:", files[i]
            #json_path = _dir+'/'+json_file
            with open(json_file) as data_file:    
                json_data = json.load(data_file)
            #print 'neural parameters found at file:', data_dir+json_file
            pprint(json_data)
            
            return json_data