#!/usr/bin/env python
# coding: utf-8

import sqlite3
import datetime
import time
import random
import names

#Creates a new table if does not exists
def new_table():
    c.execute('Create table if not exists Student(id int, name text NOT NULL,age int,class_no int,lesson_no int,result int, created_at datetime)')



#Populates the table with random values
def gen_data():
    sid = random.randint(1, 1000) 
    name = names.get_full_name()
    age = random.randint(19,25)
    
    unix = time.time()
    date= str(datetime.datetime.fromtimestamp(unix).strftime('%Y-%m-%d %H:%M:%S'))
    no_of_lesson = random.randint(2,10)
    class_no = random.randint(1,10)
    lesson_no= random.sample(range(100, 110), no_of_lesson)

    result = random.sample(range(35, 100), no_of_lesson)
    for i in range(no_of_lesson): 
        c.execute("insert into Student (id,name, age,class_no,lesson_no,result,created_at) values(?,?,?,?,?,?,?)", (sid,name,age,class_no,lesson_no[i],result[i],date))
    



if __name__ == '__main__':
	# Starts the connection 
	conn = sqlite3.connect('/Users/senorpete/Desktop/Flatiron Solution/Data/students.db')
	c = conn.cursor()
	new_table()
for i in range(1000):
    gen_data()
    conn.commit()
    time.sleep(1)
c.close()
conn.close()





