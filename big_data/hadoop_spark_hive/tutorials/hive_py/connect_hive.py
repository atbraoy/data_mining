#!/usr/bin/env python

import logging
logging.basicConfig(level=logging.DEBUG)

# from pyhive import hive
# conn = hive.Connection(host="localhost", port=9083, username="hiveuser")

from pyhive import presto

cursor = presto.connect(host="localhost", port=10000, username="hive").cursor()
sql = 'select * from default limit 10'
cursor.execute(sql)

print(cursor.fetchone())
print(cursor.fetchall())

