
import MySQLdb
import mysql.connector


#DataBase_Name = StringVar()
#Host_Name = StringVar()
#User_Name = StringVar()
#Pass_Code = StringVar()


def _init_connect_(Host_Name, User_Name, Pass_Code, DataBase_Name):
    Connection = MySQLdb.connect(host=Host_Name,#"localhost", # your host, usually localhost
                             user=User_Name,#"3rdDB_User1", # your username
                             passwd=Pass_Code,#"3rdDB_User1_Pass", # your password
                             db=DataBase_Name)#name of the data base: 3rdDataBase
