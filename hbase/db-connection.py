#!/usr/bin/env python

import csv
import happybase
from happybase import connection

def connection_db():
    try:
        conn = happybase.Connection()
        conn.open()
        return conn
    except Exception as e:
        print("Error: " + e)

def create_table():
    try:
        connection = connection_db()
<<<<<<< HEAD
        connection.create_table('TRANSACTION', {'Cust_Data':dict(),
        'Store_Data':dict(),
        'Transaction_Data':dict()})
=======
        connection.create_table('CUSTOMER', {'cf1':dict(max_versions=1) })
>>>>>>> a0ef36b552a98759d4cc72f3d82c36d4365ecef1
        print("Table created")
    except Exception as e:
        print("Error " + e)
        connection.close()

def push_data():
    try:
        connection = connection_db()
<<<<<<< HEAD
        table = connection.table('TRANSACTION')
        csvfile = open("dataset.csv", "r")
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            table.put(row[0], {'Cust_Data:CUSTOMER_NAME':row[1],
            'Store_Data:STORE_NAME':row[2],
            'Transaction_Data:AMOUNT':row[3],
            'Transaction_Data:DATE':row[4],
            'Transaction_Data:STATUS':row[5]})
=======
        table = connection.table('CUSTOMER')
        csvfile = open("dataset.csv", "r")
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            table.put(row[0], {'cf1:CUST_NAME':row[1] })
>>>>>>> a0ef36b552a98759d4cc72f3d82c36d4365ecef1
    except Exception as e:
        print("Error " + e)
        connection.close()

def scan_table():
    try:
        connection = connection_db()
<<<<<<< HEAD
        table = connection.table('TRANSACTION')
=======
        table = connection.table('CUSTOMER')
>>>>>>> a0ef36b552a98759d4cc72f3d82c36d4365ecef1
        for key, data in table.scan():
            no = key
            for value in data.items():
                cf1 = value
                print(no, cf1)
    except Exception as e:
        print("Error " + e)

# Must be left uncommented
connection_db()

# Uncomment this to create table
#create_table()

# Uncomment this to push data into database
#push_data()

# Uncomment this to read all rows in the table
<<<<<<< HEAD
scan_table()
=======
#scan_table()
>>>>>>> a0ef36b552a98759d4cc72f3d82c36d4365ecef1
