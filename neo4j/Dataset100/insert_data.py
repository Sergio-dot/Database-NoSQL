from neo4j import GraphDatabase
import neo4j
from neo4j.work import result

uri = "bolt://localhost:7687"
user = 'neo4j'
psw = 'sdp'

driver = GraphDatabase.driver(uri, auth=(user, psw))
pathCSV="file:///dataset_100.csv"

def CSVImportCustomer(self, pathCSV):
    session = driver.session()
    try:
        session.run("CREATE (c:CUSTOMER_NAME);")
    except:
        print("Errore")
    print("Importing customer...")
    result = session.run("""USING PERIODIC COMMIT 2000
    LOAD CSV WITH HEADERS FROM \"""" + str(pathCSV) + """\" AS line
    FIELDTERMINATOR ','
    MERGE (c:CUSTOMER_NAME {CUSTOMER_NAME: line.CUSTOMER_NAME});""")
    print("added " + str(result.consume().counters.nodes_created) + " nodes.")
    session.close()

def CSVImportStore(self, pathCSV):
    session = driver.session()
    try:
        session.run("CREATE (s:STORE_NAME);")
    except:
        print("Errore")
    print("Importing store...")
    result = session.run("""USING PERIODIC COMMIT 2000
    LOAD CSV WITH HEADERS FROM \"""" + str(pathCSV) + """\" AS line
    FIELDTERMINATOR ','
    MERGE (s:STORE_NAME {STORE_NAME: line.STORE_NAME});""")
    print("added " + str(result.consume().counters.nodes_created) + " nodes.")
    session.close()

def CSVImportTransaction(self, pathCSV):
    session = driver.session()
    try:
        print("Importing transactions to store from customer...")
        result = session.run("""USING PERIODIC COMMIT 1000
        LOAD CSV WITH HEADERS FROM \""""+ str(pathCSV) + """\" AS line
        FIELDTERMINATOR ','
        MATCH (c:CUSTOMER_NAME {CUSTOMER_NAME: line.CUSTOMER_NAME})
        MATCH (s:STORE_NAME {STORE_NAME: line.STORE_NAME})
        CREATE (c)-[:HAS_BOUGHT {
            TRANSACTION_ID: line.ID,
            DATE: line.TRANSACTION_DATE,
            AMOUNT: line.AMOUNT,
            STATUS: line.STATUS
        }]->(s);""")
        print("added " + str(result.consume().counters.relationships_created) + " relations.")
    except:
        print('errore')
    session.close()


def main():
    CSVImportCustomer(driver, pathCSV)
    CSVImportStore(driver, pathCSV)
    CSVImportTransaction(driver, pathCSV)

if __name__ == "__main__":
    main()
