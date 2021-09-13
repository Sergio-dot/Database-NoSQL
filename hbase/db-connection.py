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
        connection.create_table('CUSTOMER', {'cf1':dict(max_versions=1) })
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
        #input = csv.DictReader(open('dataset.csv'))
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

connection_db()
#create_table()
#push_data()
scan_table()



"""
def insert_customer(conn, customer, row_key):
    table = conn.table('customer')
    table.put(str(row_key), 'customer')


with open('dataset.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            print(f'Column name are {", ".join(row)}')
            line_count +=1
        else:
            conn = connection_db()
            insert_customer(conn, {row[line_count]}, line_count+1)
            line_count +=1
    print(f'Processed {line_count} lines.')
    
"""