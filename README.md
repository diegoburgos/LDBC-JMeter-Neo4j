# LDBC-JMeter-Neo4j
## Diego Burgos Sancho [diego.bursan@gmail.com](diego.bursan@gmail.com)
Benchmark implemented using Apache JMeter.
This benchmhmark implements the queries of [the LDBC benchmark](https://github.com/ldbc) using the interactive workload
- [Datagen](https://github.com/ldbc/ldbc_snb_datagen)
- [Neo4j Queries](https://github.com/PlatformLab/ldbc-snb-impls/tree/master/snb-interactive-neo4j)

To change from Neo4j-SI to Neo4j-OR you've to:
- Neo4j database (blade131):
	- config/neo4j-server.properties
	- data/graph.db & data/graph.db_v
	- lib/neo4j-kernel-2.2.0.jar & lib/neo4j-kernel-2.2.0-v1.6.1
- runJMeter.sh -> recovery

## Executed tests:
| Neo4j version | Target throughputs   | Workloads   | Clients             |
|---------------|----------------------|-------------|---------------------|
| SI OR         | 10 30 50 100 150 200 | 90,10 50,50 | 1 2 4 8 12 24 36 48 |
