#!/usr/bin/env python
import pyhs2
from thrift import Thrift

print "Test (1) -----------"
try:
    with pyhs2.connect(host='localhost',
                       port=9083,
                       authMechanism="PLAIN",
                       user='hiveuser',
                       password='user1',
                       database='metastore') as conn:
        with conn.cursor() as client:
            #Show databases
            print client.getDatabases()

        #Execute query
        client.execute("CREATE TABLE r(a STRING, b INT, c DOUBLE)")
        client.execute("LOAD TABLE LOCAL INPATH '/path' INTO TABLE r")
        client.execute("SELECT * FROM r")
        # cur.execute("select * from raw_stats")

        #Return column info from query
        print client.getSchema()

        #Fetch table results
        for i in client.fetch():
           	print i
except Thrift.TException, tx:
    print '%s' % (tx.message)

print "\n Test (2) ------------"

from pyhive import presto, hive

# cursor = hive.Connection(host='localhost',
#                        port=9083,
#                        user='hiveuser',
#                        password='user1',
#                        database='metastore')

cursor = presto.connect(host="localhost", port=9083, username="hiveuser").cursor()
# cursor.execute("SELECT * FROM metastore LIMIT 10")
cursor.execute("CREATE TABLE r(a STRING, b INT, c DOUBLE)")
cursor.execute("LOAD TABLE LOCAL INPATH '/path' INTO TABLE r")
cursor.execute("SELECT * FROM r")
print cursor.fetchone()
print cursor.fetchall()
