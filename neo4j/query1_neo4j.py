import time
from neo4j import GraphDatabase
import csv
 
uri = "bolt://localhost:7687"
user = "neo4j"
psw = "sdp"
 
driver = GraphDatabase.driver(uri, auth=(user, psw))
session = driver.session()

def query1():
    with driver.session() as session:
        session.run("""
        MATCH (c:CUSTOMER_NAME)-[r:HAS_BOUGHT]->(s:STORE_NAME) 
        WHERE r.AMOUNT >= '1500' 
        RETURN c.CUSTOMER_NAME, s.STORE_NAME, r.AMOUNT""")

with open('timeQuery1.csv', mode='w', newline='') as csv_file:
    fieldnames =['Iterazione', 'Tempo']

    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    writer.writeheader()
    for i in range(31):
        start = time.time()
        query1()
        exec_time = "%.2f" % ((time.time() - start) * 1000)
        writer.writerow({
            'Iterazione': i+1,
            'Tempo': exec_time,
        })