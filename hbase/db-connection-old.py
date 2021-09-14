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
        connection.create_table('CUSTOMER', {'cf1':dict() })
        print("Table created")
    except Exception as e:
        print("Error " + e)
        connection.close()

def push_data():
    try:
        connection = connection_db()
        table = connection.table('CUSTOMER')
        csvfile = open("dataset.csv", "r")
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            table.put(row[0], {'cf1:CUST_NAME':row[1] })
    except Exception as e:
        print("Error " + e)
        connection.close()

def scan_table():
    try:
        connection = connection_db()
        table = connection.table('CUSTOMER')
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
#scan_table()