import subprocess
import nltk 


def pipes(command):
    pipe = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE).stdout
    
    return pipe

def filing(file_name, command):
    with open(file_name, 'w') as file:
        pipe_output = pipes(command).read()
        file.write(pipe_output)
        file.close()
        
def channels(file_name):
    file = open(file_name, 'r')
    report = file.read()
    channel_report = ""
    for Name in report.splitlines():
        if Name == "Name: 127.0.0.1:50010 (localhost)":
            channel_report = Name
            break
        
    parameters = channel_report.translate(None, "()")  
    keys = nltk.word_tokenize(parameters) # put parameters in a list
    file.close()
    
    return keys
    

if __name__ == '__main__':
    command = "hadoop dfsadmin -report"
    report_file = 'hadoop_report.txt'
    filing(report_file, command)
    Name, skip, HDFS_channel, Host = channels(report_file)
    print "1: HDFS Channel ", HDFS_channel
    print "2: Host ", Host
