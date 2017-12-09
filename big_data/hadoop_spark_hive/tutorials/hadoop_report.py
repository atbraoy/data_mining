import subprocess
import nltk
import rawData_loader
from rawData_loader import * #Push_HDFS
# import Push_HDFS

class Hadoop_reporter(object):
    #code
    def __init__(self, file_name, command):
        self.file_name = file_name
        self.command = command
        
    #------------------    
    def pipes(self):
        pipe = subprocess.Popen(self.command, shell=True, stdout=subprocess.PIPE).stdout
        
        return pipe
    
    #------------------
    def filing(self):
        with open(self.file_name, 'w') as file:
            pipe_output = self.pipes().read()
            file.write(pipe_output)
            file.close()
            #print file
            
    #------------------
    def channels(self): # this might work only in this computer
        with open(self.file_name, 'r') as file:
            report = file.read()
            channel_report = ""
            for Name in report.splitlines():
                if Name == "Name: 127.0.0.1:50010 (localhost)":
                    channel_report = Name
                    print "Live datanodes (1): serving ..."
                    #break
                if Name == "Hostname: 172.16.101.191":
                    print Name
                    break
                     
            parameters = channel_report.translate(None, "()")  
            keys = nltk.word_tokenize(parameters) # put parameters in a list
            file.close()
         
        return keys
    
    #------------------
    def jps_check(self): # not used
        jps_report = self.pipes()
        print jps_report
        
        return jps_report
    

    
# self execution/testing
if __name__ == '__main__':
    
    #-----------------------------------------------------------------
    # try:
    #     command = "hadoop dfsadmin -report"
    #     report_file = 'hadoop_report.txt'
    #     hadoop_report = Hadoop_reporter(report_file, command)
    #     hadoop_report.filing()
    #     Name, skip, HDFS_channel, Host = hadoop_report.channels()
    #     print "1: HDFS Channel ", HDFS_channel
    #     print "2: Host ", Host
    # 
    # except OSError:
    #     print "Hadoop server is not running!"
    #
    #-----------------------------------------------------------------
    local_path = ""
    hdfs_path  = ""
    local_data = ""
    hdfs_channel = hadoop_channel
    logs = ["Warehouse", "Folder"]
    log_file = "log_file.txt"
    Push_HDFS(local_path, hdfs_path, local_data, hdfs_channel, logs, log_file).hdfs_path_constructor()
    
    
