from neo4j import GraphDatabase


uri = "bolt://localhost:7687"
user = 'neo4j'
psw = 'sdp'

driver = GraphDatabase.driver(uri, auth=(user, psw))
session = driver.session()

# CREAZIONE ENTITA'

# +++++++++++++++++++++++++++NAZIONE+++++++++++++++++++++++++++++++++++

session.run("CREATE CONSTRAINT ON (a:NAZIONE) ASSERT a.NAZIONE IS UNIQUE;")
session.run("""LOAD CSV WITH HEADERS FROM "file:///dati1000.csv" AS line
FIELDTERMINATOR ','
WITH line
WHERE line.NAZIONE IS NOT NULL
MERGE (a: NAZIONE {NAZIONE: line.NAZIONE})
ON CREATE SET 
a.ID = toInteger(line.ID),
a.BUDGET = toInteger(line.BUDGET)""")


# ++++++++++++++++++++++++++++CONTRATTO++++++++++++++++++++++++++++++++++

session.run("CREATE CONSTRAINT ON (a:CONTRATTO) ASSERT a.CONTRATTO IS UNIQUE;")
session.run("""LOAD CSV WITH HEADERS FROM "file:///dati1000.csv" AS line
FIELDTERMINATOR ','
WITH line
WHERE line.CONTRATTO IS NOT NULL
MERGE (a: CONTRATTO {CONTRATTO: line.CONTRATTO})
ON CREATE SET 
a.ID = toInteger(line.ID),
a.ANNO_LIMITE_CONSEGNA = toInteger(line.ANNO_LIMITE_CONSEGNA)""")


# +++++++++++++++++++++++++++++++ORGANO+++++++++++++++++++++++++++++++

session.run("CREATE CONSTRAINT ON (a:ORGANO) ASSERT a.ORGANO IS UNIQUE;")
session.run("""LOAD CSV WITH HEADERS FROM "file:///dati1000.csv" AS line
FIELDTERMINATOR ','
WITH line
WHERE line.ORGANO IS NOT NULL
MERGE (a: ORGANO {ORGANO: line.ORGANO})
ON CREATE SET 
a.ID = toInteger(line.ID),
a.ANNI_GARANZIA = toInteger(line.ANNI_GARANZIA) """)


# ++++++++++++++++++++++++++++++++OPERATORE++++++++++++++++++++++++++++++

session.run("CREATE CONSTRAINT ON (a:OPERATORE) ASSERT a.OPERATORE IS UNIQUE;")
session.run("""LOAD CSV WITH HEADERS FROM "file:///dati1000.csv" AS line
FIELDTERMINATOR ','
WITH line
WHERE line.OPERATORE IS NOT NULL
MERGE (a: OPERATORE {OPERATORE: line.OPERATORE})
ON CREATE SET 
a.ID = toInteger(line.ID),
a.ANNI_ESPERIENZA = toInteger(line.ANNI_ESPERIENZA)""")


# +++++++++++++++++++++++++++++++++DELEGATO+++++++++++++++++++++++++++++

session.run("CREATE CONSTRAINT ON (a:DELEGATO) ASSERT a.DELEGATO IS UNIQUE;")
session.run("""LOAD CSV WITH HEADERS FROM "file:///dati1000.csv" AS line
FIELDTERMINATOR ','
WITH line
WHERE line.DELEGATO IS NOT NULL
MERGE (a: DELEGATO {DELEGATO: line.DELEGATO})
ON CREATE SET 
a.ID = toInteger(line.ID),
a.N_DELEGHE = toInteger(line.N_DELEGHE)""")


# CREIAMO LE RELAZIONI


# +++++++++++++++++++++++++++CONTRATTO//NAZIONE+++++++++++++++++++++++++++++++++++

session.run("""LOAD CSV WITH HEADERS FROM "file:///dati1000.csv" AS line
FIELDTERMINATOR ','
MATCH (b:CONTRATTO {CONTRATTO: line.CONTRATTO})
MATCH (a:NAZIONE {NAZIONE: line.NAZIONE})
MERGE (a)-[r:IS_AUTHORITY_OF]->(b);""")


# +++++++++++++++++++++++++++CONTRATTO//ORGANO+++++++++++++++++++++++++++++++++++

session.run("""LOAD CSV WITH HEADERS FROM "file:///dati1000.csv" AS line
FIELDTERMINATOR ','
MATCH (b:CONTRATTO {CONTRATTO: line.CONTRATTO})
MATCH (a:ORGANO {ORGANO: line.ORGANO})
MERGE (a)-[r:IS_APPEAL_BODY_OF]->(b);""")


# +++++++++++++++++++++++++++CONTRATTO//OPERATORE+++++++++++++++++++++++++++++++++++

session.run("""LOAD CSV WITH HEADERS FROM "file:///dati1000.csv" AS line
FIELDTERMINATOR ','
MATCH (b:CONTRATTO {CONTRATTO: line.CONTRATTO})
MATCH (a:OPERATORE {OPERATORE: line.OPERATORE})
MERGE (a)-[r:IS_OPERATOR_OF]->(b);""")


# +++++++++++++++++++++++++++CONTRATTO//DELEGATO+++++++++++++++++++++++++++++++++++

session.run("""LOAD CSV WITH HEADERS FROM "file:///dati1000.csv" AS line
FIELDTERMINATOR ','
MATCH (b:CONTRATTO {CONTRATTO: line.CONTRATTO})
MATCH (a:DELEGATO {DELEGATO: line.DELEGATO})
MERGE (a)-[r:IS_DELEGATE_OF]->(b);""")


# +++++++++++++++++++++++++++NAZIONE//DELEGATO+++++++++++++++++++++++++++++++++++

session.run("""LOAD CSV WITH HEADERS FROM "file:///dati1000.csv" AS line
FIELDTERMINATOR ','
MATCH (b:NAZIONE {NAZIONE: line.NAZIONE})
MATCH (a:DELEGATO {DELEGATO: line.DELEGATO})
MERGE (a)-[r:ACTS_ON_BEHALF_OF]->(b);""")


# +++++++++++++++++++++++++++NAZIONE//OPERATORE+++++++++++++++++++++++++++++++++++

session.run("""LOAD CSV WITH HEADERS FROM "file:///dati1000.csv" AS line
FIELDTERMINATOR ','
MATCH (b:NAZIONE {NAZIONE: line.NAZIONE})
MATCH (a:OPERATORE {OPERATORE: line.OPERATORE})
MERGE (a)-[r:CASH_FLOW]->(b);""")


# +++++++++++++++++++++++++++NAZIONE//ORGANO+++++++++++++++++++++++++++++++++++

session.run("""LOAD CSV WITH HEADERS FROM "file:///dati1000.csv" AS line
FIELDTERMINATOR ','
MATCH (b:NAZIONE {NAZIONE: line.NAZIONE})
MATCH (a:ORGANO {ORGANO: line.ORGANO})
MERGE (a)-[r:IS_APPEAL_BODY_OF]->(b);""")
