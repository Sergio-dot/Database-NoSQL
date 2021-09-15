# Importing necessary libraries
import time
from happybase import connection, table
import happybase

def query1():
    start = time.time()
    connection = happybase.Connection()
    table = connection.table("TRANSACTION_Q1")
    filter = "SingleColumnValueFilter('Transaction_data', 'AMOUNT', >=, 'binary:1500')"
    table.scan(filter=filter)
    executiontime = "%.2f" % round((time.time()-start) * 1000, 2)
    print(str(executiontime) + "ms")
    connection.close()

for i in range(31):
    query1()
