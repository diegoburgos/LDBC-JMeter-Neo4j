package ldbc.queries.updateQueries;

import ldbc.driver.UpdateOperation;
import ldbc.driver.neo4j.Neo4jDbQueries;

public class UpdateQueriesFactory {

	public static UpdateOperation getNewOperator (String queryName) {
		UpdateOperation op;
		switch (queryName){
			case "update_query1":
				op = new Neo4jDbQueries.LdbcUpdate1AddPersonHandler();
				break;
			case "update_query2":
				op = new Neo4jDbQueries.LdbcUpdate2AddPostLikeHandler();
				break;
			case "update_query3":
				op = new Neo4jDbQueries.LdbcUpdate3AddCommentLikeHandler();
				break;
			case "update_query4":
				op = new Neo4jDbQueries.LdbcUpdate4AddForumHandler();
				break;
			case "update_query5":
				op = new Neo4jDbQueries.LdbcUpdate5AddForumMembershipHandler();
				break;
			case "update_query6":
				op = new Neo4jDbQueries.LdbcUpdate6AddPostHandler();
				break;
			case "update_query7":
				op = new Neo4jDbQueries.LdbcUpdate7AddCommentHandler();
				break;
			case "update_query8":
				op = new Neo4jDbQueries.LdbcUpdate8AddFriendshipHandler();
				break;
			default:
				op = null;
				break;
		}
		return op;
	}
	public static String[] getPropertiesUpdateQuert (String queryName) {
		String[] op;
		switch (queryName){
			case "update_query1":
				op = Neo4jDbQueries.LdbcUpdate1AddPersonHandler.properties;
				break;
			case "update_query2":
				op = Neo4jDbQueries.LdbcUpdate2AddPostLikeHandler.properties;
				break;
			case "update_query3":
				op = Neo4jDbQueries.LdbcUpdate3AddCommentLikeHandler.properties;
				break;
			case "update_query4":
				op = Neo4jDbQueries.LdbcUpdate4AddForumHandler.properties;
				break;
			case "update_query5":
				op = Neo4jDbQueries.LdbcUpdate5AddForumMembershipHandler.properties;
				break;
			case "update_query6":
				op = Neo4jDbQueries.LdbcUpdate6AddPostHandler.properties;
				break;
			case "update_query7":
				op = Neo4jDbQueries.LdbcUpdate7AddCommentHandler.properties;
				break;
			case "update_query8":
				op = Neo4jDbQueries.LdbcUpdate8AddFriendshipHandler.properties;
				break;
			default:
				op = null;
				break;
		}
		return op;
	}
}
