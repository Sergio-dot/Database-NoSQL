# Database-NoSQL
Lo scopo del progetto è quello di metterci nei panni di una società di carte di credito che cerca di rilevare le frodi sulle carte di credito dei consumatori, confrontando le prestazioni di Neo4j ed HBase. 
Il progetto è stato realizzato in locale e senza l'utilizzo di Hadoop per HBase.

## **Soluzioni database considerate:**
- **Neo4j**
- **HBase**

## **Data model:**
- ***Date:*** data in cui la transazione è stata effettuata
- ***Amount:*** valore del pagamento
- ***Status:*** stato della transazione, per distinguere quelle fraudolente da quelle legittime

## **Dataset:**
- Il ***Dataset*** è stato generato utilizzando Python e Faker (libreria per Python 3.6+). I dati generati (nomi, cognomi, città) non sono reali ma verosimili.
- Per una maggiore precisione dei tempi di risposta, lo stesso dataset è stato suddiviso in dimensioni diverse; in sostanza avremo quattro dataset: un dataset "originale" con 100.000 record, tre dataset dipendenti dal primo aventi rispettivamente il 75%, 50% e 25% dei record.

## Svolgimento degli esperimenti
Sono state definite 5 query da iterare 31 volte per ciascun dataset. L'iterazione è necessaria vista la funzionalità di caching dei database, pertanto la prima iterazione sarà lenta, mentre le successive avranno tempi di esecuzione decisamente inferiori. Per ogni esperimento, salviamo i tempi di esecuzione espressi in millisecondi (*ms*).
Infine, i risultati vengono graficati considerando il **valore medio** e gli **intervalli di confidenza al 95%**, rappresentando i tempi di risposta in **ms**.

## Conclusione
Dagli esperimenti si nota che HBase è nettamente superiore nelle prestazioni in quasi tutte le query, ad eccezione della quinta che comprende una funzione di ordinamento dei risultati. Per un'analisi più dettagliata si consiglia di consultare la relazione. 

### Link utili:
**Relazione:** [https://github.com/Sergio-dot/Database-NoSQL/blob/main/Relazione_DePietro_Siclari.pdf](url)

**Idea progettuale**: [https://linkurio.us/blog/stolen-credit-cards-and-fraud-detection-with-neo4j/](url)
