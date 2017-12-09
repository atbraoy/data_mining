import logging
import pyhs2

# create logs
logging.basicConfig(level=logging.DEBUG)

with pyhs2.connect(host='localhost',
                   port=9083,
                   authMechanism="PLAIN",
                   user='hiveuser',
                   password='user1',
                   database='metastore') as conn:
    with conn.cursor() as cur:
        #Show databases
        print cur.getDatabases()
        #Execute query
        cur.execute("select * from TBLS")

        #Return column info from query
        print cur.getSchema()

        #Fetch table results
        for i in cur.fetch():
            print i
