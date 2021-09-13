#!/usr/bin/env python

import csv
import happybase

def connection_db():
    conn = happybase.Connection('127.0.0.1', 9090)
    conn.open()
    return conn

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
    
