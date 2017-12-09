import commands


#------ Hadoop services
# removing the hdfs output data files first
def hdfs_data_remover(path, folder, data):
    
    commands.getstatusoutput("hdfs dfs -ls "+path)
    print "Removing old data files from data warehouse ... "
    commands.getstatusoutput("hdfs dfs -rm "+path+folder+data)
    print "data file --> removed"
    
# removing the hdfs output data folder(s)
def hdfs_folder_remover(path, folder):
    
    print "Removing old data folder(s) from data warehouse ... "
    commands.getstatusoutput("hdfs dfs -rm -R "+path+folder)
    print "folder(s) removable successfull." 