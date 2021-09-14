# Importing necessary libraries
import csv
import happybase
from happybase import connection

# This function provides connection to HBase database through happybase library
def connection_db():
    try:
        conn = happybase.Connection()
        conn.open()
        return conn
    except Exception as e:
        print("Error: " + e)

# This function creates a table in the database with the data provided.
# In connection.create_table(), replace 'TRANSACTION_Qx' with the desidered table name,
# after that, between curly braces insert the desidered column family name
def create_table():
    try:
        connection = connection_db()
        connection.create_table('TRANSACTION_Q1', { 'Transaction_Data':dict() })
        print("Table created")
    except Exception as e:
        print("Error " + e)
        connection.close()

# This function is used to fill the HBase database with data retrieved from .csv files.
# In table.put(), replace 'Transaction_Data' with the desidered column family, and replace
# what comes after the colon with the desidered column name, 
# after the column name, you must specify the csv row from which to take the data
def push_data():
    try:
        connection = connection_db()
        table = connection.table('TRANSACTION_Q1')
        csvfile = open("dataset.csv", "r")
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            table.put(row[0], {
            'Transaction_Data:CUSTOMER_NAME':row[1],
            'Transaction_Data:STORE_NAME':row[2],
            'Transaction_Data:AMOUNT':row[3],
            'Transaction_Data:DATE':row[4],
            'Transaction_Data:STATUS':row[5]
            })
    except Exception as e:
        print("Error " + e)
        connection.close()

# This function scan the table specified in connection.table() 
# and shows how many rows there are in the specified table
def scan_table():
    try:
        row_count = 0
        connection = connection_db()
        table = connection.table('TRANSACTION_Q1')
        for key, data in table.scan():
            no = key
            row_count += 1
            for value in data.items():
                cf1 = value
                print(no, cf1)
        print("########################")
        print("# " + str(row_count) + " rows fetched. #")
        print("########################")
    except Exception as e:
        print("Error " + e)

# Must be left uncommented, the next line provides connection to HBase database
connection_db()

# Uncomment the next line to create table
# create_table()

# Uncomment the next line to push data into database
# push_data()

# Uncomment the next line to read all rows in the table
scan_table()
