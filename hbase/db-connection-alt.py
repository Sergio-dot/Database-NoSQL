#!/usr/bin/env python

import csv
import happybase
import time

batch_size = 1000
host = "127.0.0.1"
file_path = "dataset.csv"
row_count = 0
start_time = time.time()
table_name = "customer"

def connect_to_hbase():
    """Connect to HBase Server.
    
    This will use the host, table name and batch size as defined in
    the global variables above.
    """

    conn = happybase.Connection(host = host)
    conn.open()
    table = conn.table(table_name)
    batch = table.batch(batch_size = batch_size)
    return conn, batch

def insert_row(batch, row):
    """Insert a row into HBase
    
    Write the row to the batch. When the batch size is reached, rows will be
    sent to the database.
    """
    batch.put(row[0], {"data:name": row[1] })

def read_csv():
    csvfile = open(file_path, "r")
    csvreader = csv.reader(csvfile)
    return csvreader, csvfile

# After everything has been defined, run the script.
conn, batch = connect_to_hbase()
print ("Connect to HBase. table name: %s, batch size: %i" % (table_name, batch_size))
csvreader, csvfile = read_csv()
print ("Connected to file. name: %s" % (file_path))

try:
    # Loop through the rows. The first row contains column headers, so skip that
    # row. Insert all remaining rows into the database.
    for row in csvreader:
        row_count += 1
        if row_count == 1:
            pass
        else:
            insert_row(batch, row)

    # If there are any leftover rows in the batch, send them now.
    batch.send()
finally:
    # No matter what happens, close the file handle.
    csvfile.close()
    conn.close()

duration = time.time() - start_time
print ("Done. row count: %i, duration: %.3f s" % (row_count, duration))