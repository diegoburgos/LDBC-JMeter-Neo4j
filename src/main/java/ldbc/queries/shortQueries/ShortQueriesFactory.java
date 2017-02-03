package ldbc.queries.shortQueries;

import ldbc.driver.ShortOperation;
import ldbc.driver.neo4j.Neo4jDbQueries;

public class ShortQueriesFactory {
	public static ShortOperation getNewOperator (String queryName) {
		ShortOperation op = null;
		switch (queryName){
			case "shortQuery1":
				op = new Neo4jDbQueries.LdbcShortQuery1PersonProfileHandler();
				break;
			case "shortQuery2":
				op = new Neo4jDbQueries.LdbcShortQuery2PersonPostsHandler();
				break;
			case "shortQuery3":
				op = new Neo4jDbQueries.LdbcShortQuery3PersonFriendsHandler();
				break;
			case "shortQuery4_post":
				op = new Neo4jDbQueries.LdbcShortQuery4MessageContentHandlerPost();
				break;
			case "shortQuery4_comment":
				op = new Neo4jDbQueries.LdbcShortQuery4MessageContentHandlerComment();
				break;
			case "shortQuery5_post":
				op = new Neo4jDbQueries.LdbcShortQuery5MessageCreatorHandlerPost();
				break;
			case "shortQuery5_comment":
				op = new Neo4jDbQueries.LdbcShortQuery5MessageCreatorHandlerComment();
				break;
			case "shortQuery6_post":
				op = new Neo4jDbQueries.LdbcShortQuery6MessageForumHandlerPost();
				break;
			case "shortQuery6_comment":
				op = new Neo4jDbQueries.LdbcShortQuery6MessageForumHandlerComment();
				break;
			case "shortQuery7_post":
				op = new Neo4jDbQueries.LdbcShortQuery7MessageRepliesHandlerPost();
				break;
			case "shortQuery7_comment":
				op = new Neo4jDbQueries.LdbcShortQuery7MessageRepliesHandlerComment();
				break;
			default:
				break;
		}
		return op;
	}
}
