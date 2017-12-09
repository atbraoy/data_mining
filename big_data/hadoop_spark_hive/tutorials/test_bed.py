import commands

#commands.getstatusoutput("hadoop jar /usr/local/Cellar/hadoop/2.7.3/libexec/share/hadoop/tools/lib/hadoop-streaming-2.7.3.jar -file conunter_map/count_mapper.py -mapper count_mapper.py -file counter_reduce/count_reducer.py -reducer count_reducer.py -input /user/ahmed/* -output /user/ahmed/book-output")
#commands.getstatusoutput("hdfs dfs -l /user/ahmed/")
#commands.getstatusoutput("ls")

commands.getstatusoutput("hdfs dfs -ls /user")
print "Removing old output data from data warhouse ... \n"
commands.getstatusoutput("hdfs dfs -rm /user/output_data/_SUCCESS")
print "data file --> removed"
commands.getstatusoutput("hdfs dfs -rm /user/ahmed/book-output/part-00000")
print "data file  --> removed"
commands.getstatusoutput("hdfs dfs -rm -R /user/output_data/")
print "data folder --> removed"
print "Output data is removed"

commands.getstatusoutput("hadoop jar /usr/local/Cellar/hadoop/2.7.3/libexec/share/hadoop/tools/lib/hadoop-streaming-2.7.3.jar -file count_mapper.py -mapper count_mapper.py -file count_reducer.py -reducer count_reducer.py -input /user/hive/warehouse/master/*.csv -output /user/output_data")

commands.getstatusoutput("hdfs dfs -ls /user")
