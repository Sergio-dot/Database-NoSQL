from neo4j import GraphDatabase

uri = "bolt://localhost:7687"
user = 'neo4j'
psw = 'sdp'

driver = GraphDatabase.driver(uri, auth=(user, psw))
session = driver.session()

##### CUSTOMER #####
session.run("CREATE (c:CUSTOMER_NAME);")
session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset.csv" AS line
FIELDTERMINATOR ','
WITH line
WHERE line.CUSTOMER_NAME IS NOT NULL
MERGE (c: CUSTOMER_NAME {CUSTOMER_NAME: line.CUSTOMER_NAME})""")

##### STORE #####
session.run("CREATE (s:STORE_NAME);")
session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset.csv" AS line
FIELDTERMINATOR ','
WITH line
WHERE line.STORE_NAME IS NOT NULL
MERGE (s: STORE_NAME {STORE_NAME: line.STORE_NAME})""")

##### AMOUNT #####
session.run("CREATE (a:AMOUNT);")
session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset.csv" AS line
FIELDTERMINATOR ','
WITH line
WHERE line.AMOUNT IS NOT NULL
MERGE (a:AMOUNT {AMOUNT: line.AMOUNT})""")

##### TRANSACTION DATE #####
session.run("CREATE (t:TRANSACTION_DATE);")
session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset.csv" AS line
FIELDTERMINATOR ','
WITH line
WHERE line.TRANSACTION_DATE IS NOT NULL
MERGE (t: TRANSACTION_DATE {TRANSACTION_DATE: line.TRANSACTION_DATE})""")

##### STATUS #####
session.run("CREATE (s:STATUS);")
session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset.csv" AS line
FIELDTERMINATOR ','
WITH line
WHERE line.STATUS IS NOT NULL
MERGE (s: STATUS {STATUS: line.STATUS})""")

#######################
###### RELAZIONI ######
#######################

##### CUSTOMER - STORE #####
session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset.csv" AS line
FIELDTERMINATOR ','
MATCH (c:CUSTOMER_NAME {CUSTOMER_NAME: line.CUSTOMER_NAME})
MATCH (s:STORE_NAME {STORE_NAME: line.STORE_NAME})
MERGE (c)-[r:BOUGHT_FROM]->(s);""")

##### CUSTOMER - AMOUNT #####
session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset.csv" AS line
FIELDTERMINATOR ','
MATCH (c:CUSTOMER_NAME {CUSTOMER_NAME: line.CUSTOMER_NAME})
MATCH (a:AMOUNT {AMOUNT: line.AMOUNT})
MERGE (c)-[r:SPENT]->(a);""")

##### STORE - AMOUNT #####
session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset.csv" AS line
FIELDTERMINATOR ','
MATCH (s:STORE_NAME {STORE_NAME: line.STORE_NAME})
MATCH (a:AMOUNT {AMOUNT: line.AMOUNT})
MERGE (s)-[r:HAS_GAINED]->(a);""")

##### STORE - TRANSACTION_DATE #####
session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset.csv" AS line
FIELDTERMINATOR ','
MATCH (s:STORE_NAME {STORE_NAME: line.STORE_NAME})
MATCH (t:TRANSACTION_DATE {TRANSACTION_DATE: line.TRANSACTION_DATE})
MERGE (s)-[r:REGISTERED]->(t);""")

##### CUSTOMER - TRANSACTION DATE #####
session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset.csv" AS line
FIELDTERMINATOR ','
MATCH (c:CUSTOMER_NAME {CUSTOMER_NAME: line.CUSTOMER_NAME})
MATCH (t:TRANSACTION_DATE {TRANSACTION_DATE: line.TRANSACTION_DATE})
MERGE (c)-[r:HAS_MADE]->(t);""")

##### TRANSACTION_DATE - AMOUNT #####
session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset.csv" AS line
FIELDTERMINATOR ','
MATCH (t:TRANSACTION_DATE {TRANSACTION_DATE: line.TRANSACTION_DATE})
MATCH (a:AMOUNT {AMOUNT: line.AMOUNT})
MERGE (t)-[r:IMPORT]->(a);""")

##### TRANSACTION_DATE - STATUS #####
session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset.csv" AS line
FIELDTERMINATOR ','
MATCH (t:TRANSACTION_DATE {TRANSACTION_DATE: line.TRANSACTION_DATE})
MATCH (s:STATUS {STATUS: line.STATUS})
MERGE (t)-[r:HAS_STATUS]->(s);""")