#!/usr/bin/env python
# coding: utf-8



import sqlite3

#Deletes the contents of the table	
def truncate():
    c.execute("delete from Student")

if __name__=='__main__':
    conn = sqlite3.connect('/Users/senorpete/Desktop/Flatiron Solution/Data/students.db')
    c = conn.cursor()
    truncate()
    conn.commit()
    c.close()
    conn.close()

