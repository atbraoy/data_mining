#!/usr/bin/python
# -*- coding: utf-8 -*-

from Tkinter import *

#class Application(Frame):
def show_entry_fields():
	print("First Name: %s\n Last Name: %s" % (First_Name.get(), Last_Name.get()))

master = Tk()
Label(master, text="First Name").grid(row=0)
Label(master, text="Last Name").grid(row=1)

First_Name = Entry(master)
Last_Name = Entry(master)

First_Name.grid(row=0, column=1)
Last_Name.grid(row=1, column=1)

Button(master, text='Quit', command=master.quit).grid(row=3, column=0, sticky=W, pady=4)
Button(master, text='Show', command=show_entry_fields).grid(row=3, column=1, sticky=W, pady=4)

mainloop( )

#------------------------------------------


import MySQLdb as mdb

con = mdb.connect('localhost', '3rdDB_User1', '3rdDB_User1_Pass', '3rdDataBase');

with con:
    
    cur = con.cursor()
    dropping = ("DROP TABLE IF EXISTS 3rdDB_Table_4")
    cur.execute(dropping)
    TDetails =("CREATE TABLE 3rdDB_Table_4"
            "(Id INT PRIMARY KEY AUTO_INCREMENT, \
                 Name VARCHAR(25), \
                 Age int(13))")
    cur.execute(TDetails)
    #---- Table entries:
    #First_Name.First_Name
    Namy=First_Name.get()
    Entry = ("INSERT INTO 3rdDB_Table_4" 
             "(Name, Age)"
             "VALUES(?,?)",(Namy,'34')) # Here you create the entry 
    cur.execute(Entry) # Now you excute to palce the entry in your table
    #----
    Entry = ("INSERT INTO 3rdDB_Table_4"
             "(Name, Age)"
             "VALUES('Ahmed','23')")
    cur.execute(Entry)
    #----
    Entry = ("INSERT INTO 3rdDB_Table_4"
             "(Name, Age)"
             "VALUES('Ahmed','2550000')")
    cur.execute(Entry)
