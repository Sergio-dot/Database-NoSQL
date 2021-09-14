# Importing necessary libraries
import time
from happybase import connection, table
import happybase

def query(i):
    conn = happybase.Connection()
    table = conn.table('TRANSACTION_Q1')
    start = time.time()
    for (key, data) in table.scan():
        var = 1
        #print('\t{}: {}'.format(key, data))
    exec_time = "%.2f" % round((time.time() - start) * 1000)
    print("Executing query #" + str(i) + " |-----------> " + str(exec_time) + "ms")
    return exec_time

time_arr = []
total = 0

print("\n#######################")
print("### Query execution ###")
print("#######################\n")

for i in range(31):
    time_arr.append(query(i))

print("\n#######################")
print("### \tResults     ###")
print("#######################\n")

for i in range(31):
    total += float(time_arr[i])
    print("Query #" + str(i) + " |-----------> " + str(time_arr[i] + "ms"))

average = total / 31
print("\nTotal |-----------> {:.2f}".format(total) + "ms")
print("\nAverage |-----------> {:.2f}".format(average) + "ms\n")