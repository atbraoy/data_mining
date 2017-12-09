#---------------- standard functions
import subprocess
import os
import sys
import errno
import time

from pathlib import Path
from fabric.api import local
from fabric.context_managers import settings

#---------------- custom functions
from hadoop_report import *
from parameter_vault import *
from data_logs import *


class Push_HDFS(object):
    #code
    def __init__(self, local_path, hdfs_path, local_data, hdfs_channel, logs, log_file):
        #pass
        self.local_path = local_path
        self.hdfs_path  = hdfs_path
        self.local_data = local_data
        self.hdfs_cahnnale = hdfs_channel
        self.logs = logs
        self.log_file = log_file
        
        
        #self.local_path_validator()
        #self.local_data_validator()
        #self.hdfs_builder()
        self.local_data_loader()
    
    #******************************* Local side
    def local_path_validator(self):
        #pass
        _local_path = raw_input("Enter the local data path: ../path/ ")
        try:
            _local = Path(_local_path)
            _local.resolve()
            self.local_path = _local_path
            print "Your data local path is: ", self.local_path
            
        except EnvironmentError as e:      # OSError or IOError...
            print os.strerror(e.errno), "Please enter a valid local path!"
            #construct = self.warehouse_constructor()
        
        return self.local_path
    
    #------------------------------        
    def local_data_validator(self):
        #pass
        _local_data = raw_input("Enter the local data file: ")
        try:
            _local_file = Path(_local_data)
            _local_file.resolve()
            self.local_data = _local_data
            local_data_path = self.local_path+"/"+ self.local_data
            print "Your data local data file is:", self.local_data
            print "Your data local path is:", local_data_path
            logs = [
                    "Local data file: " + _local_data,
                    "Local data path: " + local_data_path,
                    ]
            Logs_tools(self.log_file, logs).append_logs()
            
        except EnvironmentError as e:      # OSError or IOError...
            print os.strerror(e.errno), "Please enter a valid local data file!"
            #construct = self.warehouse_constructor()
            
        return self.local_data
    
    
    #******************************* HDFS side
    # def hdfs_path_locater(self):
    #     #pass
    #     user_hdfs_path = raw_input("Please enter the HDFS path: ")
    #     self.hdfs_path = user_hdfs_path
    #     print "HDFS path is: ", self.hdfs_path
    #     
    #     return self.hdfs_path
    
    #-------------------------------
    def hdfs_path_check(self, condition):
        _hdfs_path = False
        with settings(warn_only=True):
            if condition == False:
                result = local('hadoop fs -stat '+self.hdfs_path, capture=True)
            elif condition == True:
                result = local('hadoop fs -stat '+self.hdfs_path+"/"+self.local_data, capture=True)
            _hdfs_path = result.succeeded
            print "HDFS path check result:", _hdfs_path
            
            return _hdfs_path
    
    
    #-------------------------------
    def warehouse_constructor(self):
        try:
            decision = raw_input("Create a new data warehouse? (yes/no) ").lower()
            if decision.startswith('y'):                
                user_data_warehouse = raw_input("Enter data warehouse name: ")
                data_warehouse_path = self.hdfs_cahnnale+"/"+user_data_warehouse   
                logs = [
                    "Warehouse: " + user_data_warehouse,
                    "Pathe to warehouse: " + data_warehouse_path
                    ]
                Logs_tools(self.log_file, logs).save_logs()
                print data_warehouse_path
            
            elif decision.startswith('n'):
                skip, saved_warehouse, skip, saved_folder = Logs_tools(self.log_file, self.logs).data_logger()
                data_warehouse_path = self.hdfs_cahnnale+"/"+saved_warehouse
                print "Using an existing data warehouse: ",  saved_warehouse
                print "Path to warehouse at HDFS: ",  data_warehouse_path
                    
            elif (not decision.startswith('n')) and (not decision.startswith('y')) :
                print "Please answer 'yes' or 'no'!"
                pass
                
        except:
                pass
            
        try:
            decision = raw_input("Create a new data folder? (yes/no) ").lower()
            if decision.startswith('y'):                
                user_data_folder = raw_input("Enter data folder name: ")
                data_folder_path = self.hdfs_cahnnale+"/"+user_data_warehouse+"/"+user_data_folder
                logs = [
                    "Folder: " + user_data_folder,
                    "Pathe to folder: " + data_folder_path
                    ]
                Logs_tools(self.log_file, logs).append_logs()
                print data_folder_path
            
            elif decision.startswith('n'):
                skip, saved_warehouse, skip, saved_folder = Logs_tools(self.log_file, self.logs).data_logger()
                data_folder_path = self.hdfs_cahnnale+"/"+saved_warehouse+"/"+saved_folder
                print "Using an existing data folder: ",  saved_folder
                print "Path to data folder at HDFS: ",  data_folder_path
                    
            elif (not decision.startswith('n')) and (not decision.startswith('y')) :
                print "Please answer 'yes' or 'no"
                pass
                
        except:
                pass
        self.hdfs_path = data_folder_path
        #print "semi-final", self.hdfs_path

        return self.hdfs_path
    
    #----------------------------
    def resolve_logs(self):
        if not os.path.isfile(self.log_file):
            #os.path.isfile(path)
            pass
        elif os.path.isfile(self.log_file):
            log_files = Path(self.log_file)
            resolve = log_files.resolve()
            
            return resolve
    
            
    #----------------------------
    def hdfs_path_constructor(self, condition):
        """
        Construct an HDFS pass from the saved 'logs' file (if found one), otherwise a call
        for the 'warehouse_constructor()' function will be made which will create a new warehouse
        and a new data folder at HDFS.
        """
        #try:
            # log_files = Path(self.log_file)
            # log_files.resolve()
        _construted = ''
        if self.resolve_logs():
            print "Found HDFS log records stored at:", self.log_file, "contains the followings:" 
            skip, saved_warehouse, skip, saved_folder = Logs_tools(self.log_file, self.logs).data_logger()
            print '-----------------------------------------------------------'
            print "Warehouse at", saved_warehouse, ", path:", self.hdfs_cahnnale+"/"+saved_warehouse
            print "Data folder at", saved_folder, ", path:", self.hdfs_cahnnale+"/"+saved_warehouse+"/"+saved_folder
            print '-----------------------------------------------------------'
                #---------------------------------------
            _construted = self.warehouse_constructor()
            
        elif not self.resolve_logs():                
            if condition == True:
                _construted = self.warehouse_constructor()
            elif condition == False: pass
        
        #print _construted        
        return _construted
            
        # except EnvironmentError as e:      # OSError or IOError...
        #     print os.strerror(e.errno),
        #     "Please create a log file for your hadoop parameters, answer 'yes' to the followings:"
        #     if condition == True:
        #         construct = self.warehouse_constructor()
        #     elif condition == False: pass
        #    
        #print "final", construct    
        
            
            
    #*************************** HDFS build
    def hdfs_builder(self):
        #try:
        decision = raw_input("Create a new HDFS directory? (yes/no)").lower()
        if decision.startswith('y'):
            #--------------------------
            if not self.resolve_logs():
                print "There is no HDFS log records found for warehouse neither data folder!"
                #self.hdfs_path_constructor(True)
                #------- create a new warehouse and new data folder
                _hdfs_path = self.hdfs_path_constructor(True)
                    
                _check = self.hdfs_path_check(False)
                print "Checking:", _check
                if _check == True:
                    print "HDFS path already exist at:", self.hdfs_path
                    print "Exit with log records stored at:", self.log_file
                    raise SystemExit
                elif _check == False:
                    command ='hdfs dfs -mkdir -p '+_hdfs_path
                    pipes(command)
                    while self.hdfs_path_check(False):
                            print "......."
                            time.sleep(3)
                    print "HDFS path has been built at: ", _hdfs_path
                    self.hdfs_path_check(False)
                                
            elif self.resolve_logs(): 
                skip, saved_warehouse, skip, saved_folder = Logs_tools(self.log_file, self.logs).data_logger()
                _destroy_folder = self.hdfs_cahnnale+"/"+saved_warehouse+"/"+saved_folder
                _destroy_warehouse = self.hdfs_cahnnale+"/"+saved_warehouse
                self.hdfs_path = _destroy_folder
                    
                print "Found an HDFS log records with the following details:"
                print "Warehouse record:", saved_warehouse
                print "Data folder record:", saved_folder
                print "HDFS path record: ", _destroy_folder
                    
                _check = self.hdfs_path_check(False) # Double check in HDFS storage
                print "Checking:", _check
                if _check == True:
                    print "HDFS path already exist at:", self.hdfs_path
                    decision = raw_input("Destroy this HDFS directory? (yes/no)").lower()
                    if decision.startswith('y'):
                    #------- destroy the existing hdfs warehouse and data folder    
                        commands = [
                            'hdfs dfs -rm -r '+_destroy_folder,
                            'hdfs dfs -rm -r '+_destroy_warehouse
                            ]
                        for command in commands:
                            print "Destroying HDFS path"
                            pipes(command)
                            # print "Destroying HDFS path"
                            while self.hdfs_path_check(False):
                                print "......."
                                time.sleep(2)
                        print "Raw data folder has been destroyed:", _destroy_folder
                        print "Data warehouse has been destroyed:", _destroy_warehouse
                            
                        decision = raw_input("Create a new HDFS directory? (yes/no) ").lower()
                        if decision.startswith('y'):         
                            _hdfs_path = self.hdfs_path_constructor(True)
                            self.hdfs_path_check(False)
                            command ='hdfs dfs -mkdir -p '+_hdfs_path
                            pipes(command)
                            while self.hdfs_path_check(False):
                                print "......."
                                time.sleep(5)
                                    
                            print "HDFS path has been built at: ", _hdfs_path
                            self.hdfs_path_check(False)

                        elif decision.startswith('n'):
                            raise SystemExit
                            
                        else: pass
                    
                    elif decision.startswith('n'):
                        print "No actions were taken."
                        raise SystemExit
                        
                elif _check == False:
                    decision = raw_input("No HDFS path has been found! Create from saved logs records? (yes/no)").lower()
                    if decision.startswith('y'):
                        self.hdfs_path_check(False)
                        command ='hdfs dfs -mkdir -p '+_hdfs_path
                        pipes(command)
                        while self.hdfs_path_check(False):
                            print "......."
                            time.sleep(5)
                                
                        print "HDFS path has been built at: ", _hdfs_path
                        self.hdfs_path_check(False)
                            
                    elif decision.startswith('n'):
                        print "Creating new HDFS log record ..."
                        _hdfs_path = self.hdfs_path_constructor(True)
                        self.hdfs_path = _hdfs_path
                        # print "No actions were taken."
                        # raise SystemExit

        elif decision.startswith('n'):
            raise SystemExit
            
        else: 
            print "Process terminated: log error (1)!"
            raise SystemExit
        
        return self.hdfs_path
            
    #---------------------------------
    def local_data_loader(self):
        
            _local_path = self.local_path_validator()+"/"
            _local_data = self.local_data_validator()
            _path_build_local = _local_path+_local_data
            
            if not self.resolve_logs():
                    print "There is no HDFS log records found for warehouse neither data folder!"
                    #self.hdfs_path_constructor(True)
                    #------- create a new warehouse and new data folder
                    _hdfs_path = self.hdfs_path_constructor(True)
                    _check = self.hdfs_path_check(False)
                    
                    if _check == True:
                        print "HDFS path already exist at:", self.hdfs_path
                        decision = raw_input("Store data at this HDFS path? (yes/no)").lower()
                        if decision.startswith('y'):               
                            command = "hadoop fs -copyFromLocal "+_path_build_local+" "+self.hdfs_path
                            pipes(command)
                            while self.hdfs_path_check(False):
                                print "storing data ..."
                                time.sleep(2)
                            print "Data stored at:", self.hdfs_path
                            self.hdfs_path_check(False)
                        
                        elif decision.startwith('n'):
                            print "Please consider creating new data warehouse."
                            self.hdfs_path_constructor(True)
                            #raise SystemExit

                    elif _check == False:
                        print "No HDFS path found, please create new warehouse and data folder."
                        _hdfs_path = self.hdfs_builder(False)
                        command ='hdfs dfs -mkdir -p '+_hdfs_path
                        pipes(command)
                        while self.hdfs_path_check(False):
                                    print "......."
                                    time.sleep(3)
                        print "HDFS path has been created at: ", _hdfs_path
                        self.hdfs_path_check(False)
                        
                        command = "hadoop fs -copyFromLocal "+_path_build_local+" "+_hdfs_path
                        pipes(command)
                        while self.hdfs_path_check(True):
                            print "storing data ..."
                            time.sleep(2)
                            
                        print "Data stored at:", _hdfs_path
                        self.hdfs_path_check(True)
                        print "Raw data is located in HDFS ...."
                        command = "hadoop fs -cat "+self.hdfs_path+"/"+_local_data+" | head"
                        pipes(command)
                                
            elif self.resolve_logs(): 
                    skip, saved_warehouse, skip, saved_folder = Logs_tools(self.log_file, self.logs).data_logger()
                    _destroy_folder = self.hdfs_cahnnale+"/"+saved_warehouse+"/"+saved_folder
                    _destroy_warehouse = self.hdfs_cahnnale+"/"+saved_warehouse
                    _hdfs_stored = self.hdfs_path+"/"+self.local_data
                    self.hdfs_path = _destroy_folder
                    
                    print "Found an HDFS log records with the following details:"
                    print "Warehouse record:", saved_warehouse
                    print "Data folder record:", saved_folder
                    print "HDFS path record: ", _destroy_folder
                    
                    _check = self.hdfs_path_check(False) # Double check in HDFS storage
                    if _check == True:
                        print "Storing data at the HDFS path (pulled from logs record):", self.hdfs_path
                        print "Executing: hadoop fs -copyFromLocal "+_path_build_local+" "+self.hdfs_path
                        command = "hadoop fs -copyFromLocal "+_path_build_local+" "+self.hdfs_path
                        pipes(command)
                        
                        while self.hdfs_path_check(True):
                            print "storing data in HDFS storage ..."
                            time.sleep(0.2)
                        print "Data stored at:", self.hdfs_path
                        self.hdfs_path_check(True)
                        
                        print "Raw data is located in HDFS:", self.hdfs_path+"/"+self.local_data
                        
                        #-------- Sending a sample data to local: not working propoerly!! 
                        # command = "hadoop fs -cat "+self.hdfs_path+"/"+self.local_data+" | head -100"+" "+self.hdfs_path+"/"+"hdfs_sample.csv"
                        # print "Executing: hadoop fs -cat "+self.hdfs_path+"/"+self.local_data+" | head -100"
                        # pipes(command)
                        # 
                        # while (_local_path+"hdfs_sample.csv" == True):
                        #     print "creating local sample data ..."
                        #     time.sleep(0.2)
                        
                        # command = "hadoop fs -put "+self.hdfs_path+"/"+self.local_data+" "+_local_path+ "hdfs_sample.csv"
                        # print "Transfering sample data form HDFS storage to local storage."
                        # pipes(command)
                        # 
                        # while (_local_path+"hdfs_sample.csv" == False):
                        #     print "creating local sample data ..."
                        #     time.sleep(0.2)
                        #     
                        # print "Samlpe data is stored at:", _local_path+ "hdfs_sample.csv"
                        # -----------------------------------------------------------------
                        
                        raise SystemExit
                        
                    elif _check == False:
                        print "No HDFS path found, please create new warehouse and data folder."
                        _hdfs_path = self.hdfs_builder(False)
                        command ='hdfs dfs -mkdir -p '+_hdfs_path
                        pipes(command)
                        while self.hdfs_path_check(False):
                                    print "......."
                                    time.sleep(3)
                        print "HDFS path has been created at: ", _hdfs_path
                        self.hdfs_path_check(False)
                        
                        command = "hadoop fs -copyFromLocal "+_path_build_local+" "+_hdfs_path
                        pipes(command)
                        while self.hdfs_path_check(True):
                            print "storing data ..."
                            time.sleep(2)
                            
                        print "Data stored at:", _hdfs_path
                        self.hdfs_path_check(True)
                        print "Raw data is located in HDFS ...."
                        command = "hadoop fs -cat "+self.hdfs_path+"/"+_local_data+" | head"
                        pipes(command)
                        
            
# self execution/testing
if __name__ == '__main__':
    local_path = ""
    hdfs_path  = ""
    local_data = ""
    hdfs_channel = hadoop_channel
    logs = ["Warehouse", "Folder"]
    log_file = "log_file.txt"
    #Push_HDFS(local_path, hdfs_path, local_data, hdfs_channel, logs, log_file).hdfs_path_constructor()
    Push_HDFS(local_path, hdfs_path, local_data, hdfs_channel, logs, log_file)#.data_logger()
    
# 
# hdfs_path = ""
# command = "hadoop fs -copyFromLocal"+local_path+" "+hdfs_path