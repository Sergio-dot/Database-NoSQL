import time
from neo4j import GraphDatabase
import csv
 
uri = "bolt://localhost:7687"
user = "neo4j"
psw = "sdp"
 
driver = GraphDatabase.driver(uri, auth=(user, psw))
session = driver.session()

def query1():
    start = time.time()
    with driver.session() as session:
        session.run("""
        MATCH (c:CUSTOMER_NAME)-[r:HAS_BOUGHT]->(s:STORE_NAME) 
        WHERE r.AMOUNT >= '1500' 
        RETURN c.CUSTOMER_NAME, s.STORE_NAME, r.AMOUNT""")
    exec_time = "%.2f" % round((time.time()-start) * 1000, 2)
    return exec_time

def query2():
    start = time.time()
    with driver.session() as session:
        session.run("""
        MATCH (c:CUSTOMER_NAME)-[r:HAS_BOUGHT]->(s:STORE_NAME) 
        WHERE r.AMOUNT >= '1500' AND r.DATE > '2015-12-31'
        RETURN c.CUSTOMER_NAME, s.STORE_NAME, r.AMOUNT""")
    exec_time = "%.2f" % round((time.time()-start) * 1000, 2)
    return exec_time
    
def query3():
    start = time.time()
    with driver.session() as session:
        session.run("""
        MATCH (c:CUSTOMER_NAME)-[r:HAS_BOUGHT]->(s:STORE_NAME) 
        WHERE r.AMOUNT >= '1500' AND r.DATE > '2015-12-31' AND r.STATUS = 'Disputed'
        RETURN c.CUSTOMER_NAME, s.STORE_NAME, r.AMOUNT""")
    exec_time = "%.2f" % round((time.time()-start) * 1000, 2)
    return exec_time

def query4():
    start = time.time()
    with driver.session() as session:
        session.run("""
        MATCH (c:CUSTOMER_NAME)-[r:HAS_BOUGHT]->(s:STORE_NAME {STORE_NAME: 'Coop, Milano'}) 
        WHERE r.AMOUNT >= '1500' AND r.DATE > '2015-12-31' AND r.STATUS = 'Disputed' 
        RETURN c.CUSTOMER_NAME, s.STORE_NAME, r.AMOUNT, r.DATE, r.STATUS""")
    exec_time = "%.2f" % round((time.time()-start) * 1000, 2)
    return exec_time

def query5():
    start = time.time()
    with driver.session() as session:
        session.run("""
        MATCH (c:CUSTOMER_NAME {CUSTOMER_NAME: 'Armanda Pietrangeli'})-[r:HAS_BOUGHT]->(s:STORE_NAME {STORE_NAME: 'Panzera SPA'}) 
        WHERE r.AMOUNT >= '1500' AND r.DATE > '2015-12-31' AND r.STATUS = 'Disputed' 
        RETURN c.CUSTOMER_NAME, s.STORE_NAME, r.AMOUNT, r.DATE, r.STATUS""")
    exec_time = "%.2f" % round((time.time()-start) * 1000, 2)
    return exec_time

#SCRITTURA FILE
with open('time5.csv', mode='w', newline='') as csv_file:
    fieldnames =['Iterazione', 'Tempo']

    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    writer.writeheader()
    for i in range(31):
        temp = query5()
        writer.writerow({
            'Iterazione': i+1,
            'Tempo': temp,
        })

#query1()
#query2()
#query3()
#query4()
#query5()
