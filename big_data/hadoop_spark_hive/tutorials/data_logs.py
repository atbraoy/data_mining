# data logs tools
#---------------- standard functions
import subprocess
import signal
import nltk

class Logs_tools(object):
    #code
    def __init__(self, log_file, catalog):
        self.log_file = log_file
        self.catalog = catalog
        
    #---------------------------
    def save_logs(self):
        with open(self.log_file, 'wb') as file:
            i=0
            while i<len(self.catalog):
                file.write(self.catalog[i] + "\n")
                i=i+1
            file.close()
    
    #---------------------------
    def append_logs(self):
        with open(self.log_file, 'ab') as file:
            i=0
            while i<len(self.catalog):
                file.write(self.catalog[i] + "\n")
                i=i+1
            file.close()
               
    #---------------------------
    def data_logger(self):
        parameters = ''
        with open(self.log_file, 'r') as file:
            for line in file:
                line = line.rstrip()  # remove '\n' at end of line
                # logs = ["Warehouse", "Folder"]
                i=0
                while i<len(self.catalog):
                    if self.catalog[i] in line:
                        structure = line.translate(None, ":")
                        parameters += ' ' + structure
                    i=i+1
            file.close()
            
        keys = nltk.word_tokenize(parameters) # put parameters in a list
        #print "keys: ", keys
        
        return keys
    

def clear_logs(log_file):
    command = 'rm '+log_file
    pipes(command)
    
def create_logs(log_file):
    command = 'touch '+log_file
    pipes(command)
        
#------------------    
def pipes(command):
    
    print "executing ..."
    pipe = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE).stdout
    # pipe = subprocess.Popen(command, stdout=subprocess.PIPE, 
    #                    shell=True, preexec_fn=os.setsid).stdout 
    #print "executed."
        
    return pipe

#-----------------
def kill_pipes(command):
    os.killpg(os.getpgid(pipes(command).pid), signal.SIGTERM)
    print "pipe killed."




# self execution/testing
if __name__ == '__main__':
    names = ['warehouse_1', 'path_1', 'something_1']
    file_name = 'test.txt'
    Logs_tools(file_name, names).save_logs()
    names = ['warehouse_2', 'path_2', 'something_2']
    Logs_tools(file_name, names).append_logs()

