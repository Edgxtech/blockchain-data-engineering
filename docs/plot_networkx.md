Basic example of networkx plot using data from Neo4j.

```python
from neo4j import GraphDatabase
import networkx as nx

driver = GraphDatabase.driver("neo4j://127.0.0.1:7687", auth=("neo4j", "mypassword"))
driver.verify_connectivity()
session = driver.session()

# Query all SENT_TO relationships using the following Cypher query
nodes = session.run("MATCH p=()-[r:SENT_TO]->() RETURN p LIMIT 50")
results = [record for record in nodes.data()]

g = nx.Graph()

for res in results:
    sender_addr = res['p'][0].get('address')
    rx_addr = res['p'][2].get('address')
    g.add_edge(sender_addr,rx_addr)
 
nx.draw_random(g, with_labels = False)
```