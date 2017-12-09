import sys

from hive_service import ThriftHive
from hive.types import HiveServerException

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

try:
  transport = TSocket.TSocket('localhost', 10000)
  transport = TTransport.TBufferedTransport(transport)
  protocol = TBinaryProtocol.TBinaryProtocol(transport)
  client = ThriftHive.Client(protocol)
  transport.open()
  client.execute("CREATE TABLE r(a STRING, b INT, c DOUBLE)")
  client.execute("LOAD TABLE LOCAL INPATH '/path' INTO TABLE r")
  client.execute("SELECT * FROM r")
  while (1):
    row = client.fetchOne()
    if (row == None):
       break
    print row

  client.execute("SELECT * FROM r")
  print client.fetchAll()
  transport.close()
except Thrift.TException, tx:
  print '%s' % (tx.message)
