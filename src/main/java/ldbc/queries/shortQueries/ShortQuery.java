package ldbc.queries.shortQueries;

import ldbc.driver.ShortOperation;
import ldbc.driver.neo4j.Neo4jDbConnectionState;
import ldbc.queries.JMeterQuery;
import ldbc.queries.QueriesParametersGen;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import com.sun.jersey.api.client.ClientResponse;

public class ShortQuery extends JMeterQuery {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Neo4jDbConnectionState connection = null;
	private ShortOperation query = null;
	private ShortQueryParameterProvider personIds, commentIds, postIds;

	@Override
	public Arguments getDefaultParameters() {
		Arguments defaultParameters = new Arguments();
		defaultParameters.addArgument(JMeterQuery.ARGUMENTS_SERVER_URL, "localhost");
		defaultParameters.addArgument(JMeterQuery.ARGUMENTS_SERVER_PORT, "8080");
		defaultParameters.addArgument("QueryName", "shortQueryXX");
		return defaultParameters;
	}

	@Override
	public SampleResult runTest(JavaSamplerContext context) {
		String queryName = context.getParameter("QueryName");
		String serverUrl = context.getParameter(JMeterQuery.ARGUMENTS_SERVER_URL);
		String serverPort = context.getParameter(JMeterQuery.ARGUMENTS_SERVER_PORT);
		if (connection == null) {
			connection = new Neo4jDbConnectionState (serverUrl, serverPort);
		}
		if (personIds == null) {
			personIds = QueriesParametersGen.getPersonIds();
		}
		if (commentIds == null) {
			commentIds = QueriesParametersGen.getCommentIds();
		}
		if (postIds == null) {
			postIds = QueriesParametersGen.getPostIds();
		}
		if (query == null) {
			query = ShortQueriesFactory.getNewOperator(queryName);
		}
		// Get the right arguments depending on the query
		String[] arguments = query.getNewArguments(personIds, commentIds, postIds);
		String queryGenerated = query.getMessage(arguments);
		ClientResponse obtainedResult;
		this.startTest();
		obtainedResult = connection.execAndCommit(queryGenerated);
		this.stopTest();
		this.setResult(obtainedResult);
		return this.getResults();
	}
}