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
        MATCH (c:CUSTOMER_NAME)-[r:HAS_BOUGHT]->(s:STORE_NAME {STORE_NAME: 'Panzera SPA'}) 
        WHERE r.AMOUNT >= '1500' AND r.DATE > '2015-12-31' AND r.STATUS = 'Disputed' 
        RETURN c.CUSTOMER_NAME, s.STORE_NAME, r.AMOUNT, r.DATE, r.STATUS""")
    exec_time = "%.2f" % round((time.time()-start) * 1000, 2)
    return exec_time

def query5():
    start = time.time()
    with driver.session() as session:
        session.run("""
        MATCH (c:CUSTOMER_NAME {CUSTOMER_NAME: 'Amanda Pietrangeli'})-[r:HAS_BOUGHT]->(s:STORE_NAME {STORE_NAME: 'Panzera SPA'}) 
        WHERE r.AMOUNT >= '1500' AND r.DATE > '2015-12-31' AND r.STATUS = 'Disputed' 
        RETURN c.CUSTOMER_NAME, s.STORE_NAME, r.AMOUNT, r.DATE, r.STATUS
        ORDER BY r.AMOUNT DESC""")
    exec_time = "%.2f" % round((time.time()-start) * 1000, 2)
    return exec_time

print("Query 1:")
for i in range(31):
    print(query1())

print("Query 2:")
for i in range(31):
    print(query2())

print("Query 3:")
for i in range(31):
    print(query3())

print("Query 4:")
for i in range(31):
    print(query4())

print("Query 5:")
for i in range(31):
    print(query5())
