# Importing necessary libraries
import time
import csv
from happybase import connection, table
import happybase

def query1():
    start = time.time()
    connection = happybase.Connection()
    table = connection.table("TRANSACTION_Q1_100")
    filter = "SingleColumnValueFilter('Transaction_Data', 'AMOUNT', >=, 'binary:1500')"
    table.scan(filter=filter)
    executiontime = "%.2f" % round((time.time()-start) * 1000, 2)
    print(str(executiontime) + "ms")
    connection.close()
    return executiontime

def query2():
    start = time.time()
    connection = happybase.Connection()
    table = connection.table("TRANSACTION_Q2_100")
    filter_a = "SingleColumnValueFilter('Transaction_Data', 'AMOUNT', >=, 'binary:1500')"
    filter_b = "SingleColumnValueFilter('Transaction_Data', 'DATE', >, 'binary:2015-12-31')"
    filter = filter_a + " AND " + filter_b
    table.scan(filter=filter)
    executiontime = "%.2f" % round((time.time()-start) * 1000, 2)
    print(str(executiontime) + "ms")
    connection.close()
    return executiontime

def query3():
    start = time.time()
    connection = happybase.Connection()
    table = connection.table("TRANSACTION_Q3_100")
    filter_a = "SingleColumnValueFilter('Transaction_Data', 'AMOUNT', >=, 'binary:1500')"
    filter_b = "SingleColumnValueFilter('Transaction_Data', 'DATE', >, 'binary:2015-12-31')"
    filter_c = "SingleColumnValueFilter('Transaction_Data', 'STATUS', =, 'binary:Disputed')"
    filter = filter_a + " AND " + filter_b + " AND " + filter_c
    table.scan(filter=filter)
    executiontime = "%.2f" % round((time.time()-start) * 1000, 2)
    print(str(executiontime) + "ms")
    connection.close()
    return executiontime

def query4():
    start = time.time()
    connection = happybase.Connection()
    table = connection.table("TRANSACTION_Q3_100")
    filter_a = "SingleColumnValueFilter('Transaction_Data', 'AMOUNT', >=, 'binary:1500')"
    filter_b = "SingleColumnValueFilter('Transaction_Data', 'DATE', >, 'binary:2015-12-31')"
    filter_c = "SingleColumnValueFilter('Transaction_Data', 'STATUS', =, 'binary:Disputed')"
    filter_d = "SingleColumnValueFilter('Transaction_Data', 'STORE_NAME', =, 'binary:Panzera SPA')"
    filter = filter_a + " AND " + filter_b + " AND " + filter_c + " AND " + filter_d
    table.scan(filter=filter)
    executiontime = "%.2f" % round((time.time()-start) * 1000, 2)
    print(str(executiontime) + "ms")
    connection.close()
    return executiontime

def query5():
    start = time.time()
    connection = happybase.Connection()
    table = connection.table("TRANSACTION_Q3_100")
    filter_a = "SingleColumnValueFilter('Transaction_Data', 'AMOUNT', >=, 'binary:1500')"
    filter_b = "SingleColumnValueFilter('Transaction_Data', 'DATE', >, 'binary:2015-12-31')"
    filter_c = "SingleColumnValueFilter('Transaction_Data', 'STATUS', =, 'binary:Disputed')"
    filter_d = "SingleColumnValueFilter('Transaction_Data', 'STORE_NAME', =, 'binary:Panzera SPA')"
    filter_e = "SingleColumnValueFilter('Transaction_Data', 'CUSTOMER_NAME', =, 'binary:Amanda Pietrangeli')"
    filter = filter_a + " AND " + filter_b + " AND " + filter_c + " AND " + filter_d + " AND " + filter_e
    table.scan(filter=filter)
    executiontime = "%.2f" % round((time.time()-start) * 1000, 2)
    print(str(executiontime) + "ms")
    connection.close()
    return executiontime

print("\n#######################")
print("### Query execution ###")
print("#######################\n")

# Replace results/time_qx_100.csv where x matches the number of the query
# example when assigning temp = query5() replace with results/time_q5_100.csv
# this will execute query5() and save the results under the results folder, in the time_q5_100.csv file

with open('results/time_q1_100.csv', mode='w', newline='') as csv_file:
    fieldnames =['Iterazione', 'Tempo']

    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)


    writer.writeheader()
    for i in range(31):
        temp = query1()
        writer.writerow({
            'Iterazione': i+1,
            'Tempo': temp,
        })

    print("\n##########################")
    print("### Data saved in csv  ###")
    print("##########################\n")