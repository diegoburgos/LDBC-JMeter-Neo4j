package ldbc.queries.largeQueries;

import ldbc.driver.Operation;
import ldbc.driver.neo4j.Neo4jDbQueries;

public class LargeQueriesFactory {
	public static Operation getNewOperator (String queryName) {
		Operation op = null;
		switch (queryName){
			case "query_1":
				op = new Neo4jDbQueries.LdbcQuery1Handler();
				break;
			case "query_2":
				op = new Neo4jDbQueries.LdbcQuery2Handler();
				break;
			case "query_3":
				op = new Neo4jDbQueries.LdbcQuery3Handler();
				break;
			case "query_4":
				op = new Neo4jDbQueries.LdbcQuery4Handler();
				break;
			case "query_5":
				op = new Neo4jDbQueries.LdbcQuery5Handler();
				break;
			case "query_6":
				op = new Neo4jDbQueries.LdbcQuery6Handler();
				break;
			case "query_7":
				op = new Neo4jDbQueries.LdbcQuery7Handler();
				break;
			case "query_8":
				op = new Neo4jDbQueries.LdbcQuery8Handler();
				break;
			case "query_9":
				op = new Neo4jDbQueries.LdbcQuery9Handler();
				break;
			case "query_10":
				op = new Neo4jDbQueries.LdbcQuery10Handler();
				break;
			case "query_11":
				op = new Neo4jDbQueries.LdbcQuery11Handler();
				break;
			case "query_12":
				op = new Neo4jDbQueries.LdbcQuery12Handler();
				break;
			case "query_13":
				op = new Neo4jDbQueries.LdbcQuery13Handler();
				break;
			case "query_14":
				op = new Neo4jDbQueries.LdbcQuery14Handler();
				break;
			default:
				break;
		}
		return op;
	}
}
