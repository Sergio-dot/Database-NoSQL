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
def create_table_q1():
    try:
        conn = connection_db()
        print("\n#####################################")
        print("# Creating table TRANSACTION_Q1_75 #")
        print("#####################################\n")
        conn.create_table('TRANSACTION_Q1_75', { 'Transaction_Data':dict() })
        print("Table created.")
    except Exception as e:
        print("Error " + e)
    conn.close()

def create_table_q2():
    try:
        conn = connection_db()
        print("\n#####################################")
        print("# Creating table TRANSACTION_Q2_75 #")
        print("#####################################\n")
        conn.create_table('TRANSACTION_Q2_75', { 'Transaction_Data':dict() })
        print("Table created")
    except Exception as e:
        print("Error " + e)
    conn.close()

def create_table_q3():
    try:
        conn = connection_db()
        print("\n#####################################")
        print("# Creating table TRANSACTION_Q3_75 #")
        print("#####################################\n")
        conn.create_table('TRANSACTION_Q3_75', { 'Transaction_Data':dict() })
        print("Table created")
    except Exception as e:
        print("Error " + e)
    conn.close()

# This function is used to fill the HBase database with data retrieved from .csv files.
# In table.put(), replace 'Transaction_Data' with the desidered column family, and replace
# what comes after the colon with the desidered column name, 
# after the column name, you must specify the csv row from which to take the data
def push_data_q1():
    try:
        conn = connection_db()
        print("\n###################################################")
        print("# Importing dataset into table TRANSACTION_Q1_75 #")
        print("###################################################\n")
        table = conn.table('TRANSACTION_Q1_75')
        csvfile = open("dataset_75.csv", "r")
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            table.put(row[0], {
            'Transaction_Data:CUSTOMER_NAME':row[1],
            'Transaction_Data:STORE_NAME':row[2],
            'Transaction_Data:AMOUNT':row[3]
            })
    except Exception as e:
        print("Error " + e)
    conn.close()

def push_data_q2():
    try:
        conn = connection_db()
        print("\n###################################################")
        print("# Importing dataset into table TRANSACTION_Q2_75 #")
        print("###################################################\n")
        table = conn.table('TRANSACTION_Q2_75')
        csvfile = open("dataset_75.csv", "r")
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            table.put(row[0], {
            'Transaction_Data:CUSTOMER_NAME':row[1],
            'Transaction_Data:STORE_NAME':row[2],
            'Transaction_Data:AMOUNT':row[3],
            'Transaction_Data:DATE':row[4]
            })
    except Exception as e:
        print("Error " + e)
    conn.close()

def push_data_q3():
    try:
        conn = connection_db()
        print("\n###################################################")
        print("# Importing dataset into table TRANSACTION_Q3_75 #")
        print("###################################################\n")
        table = conn.table('TRANSACTION_Q3_75')
        csvfile = open("dataset_75.csv", "r")
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
    conn.close()

# This function scan the table specified in connection.table() 
# and shows how many rows there are in the specified table
def scan_table_q1():
    try:
        row_count = 0
        conn = connection_db()
        print("\n##############################################")
        print("# Scanning content: table TRANSACTION_Q1_75 #")
        print("##############################################\n")
        table = conn.table('TRANSACTION_Q1_75')
        for key, data in table.scan():
            no = key
            row_count += 1
            for value in data.items():
                cf1 = value
                print(no, cf1)
        print("\n########################")
        print("# " + str(row_count) + " rows fetched. #")
        print("########################\n")
    except Exception as e:
        print("Error " + e)
    conn.close()

def scan_table_q2():
    try:
        row_count = 0
        conn = connection_db()
        print("\n##############################################")
        print("# Scanning content: table TRANSACTION_Q1_75 #")
        print("##############################################\n")
        table = conn.table('TRANSACTION_Q2_75')
        for key, data in table.scan():
            no = key
            row_count += 1
            for value in data.items():
                cf1 = value
                print(no, cf1)
        print("\n########################")
        print("# " + str(row_count) + " rows fetched. #")
        print("########################\n")
    except Exception as e:
        print("Error " + e)
    conn.close()

def scan_table_q3():
    try:
        row_count = 0
        conn = connection_db()
        print("\n##############################################")
        print("# Scanning content: table TRANSACTION_Q1_75 #")
        print("##############################################\n")
        table = conn.table('TRANSACTION_Q3_75')
        for key, data in table.scan():
            no = key
            row_count += 1
            for value in data.items():
                cf1 = value
                print(no, cf1)
        print("\n########################")
        print("# " + str(row_count) + " rows fetched. #")
        print("########################\n")
    except Exception as e:
        print("Error " + e)
    conn.close()

# table TRANSACTION_Q1_75
# create_table_q1()
# push_data_q1()
# scan_table_q1()

# table TRANSACTION_Q2_75
# create_table_q2()
# push_data_q2()
# scan_table_q2()

# table TRANSACTION_Q3_75
# create_table_q3()
# push_data_q3()
# scan_table_q3()