package ldbc.queries.largeQueries;

import ldbc.driver.Operation;
import ldbc.driver.neo4j.Neo4jDbConnectionState;
import ldbc.queries.JMeterQuery;
import ldbc.queries.QueriesParametersGen;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import com.sun.jersey.api.client.ClientResponse;

public class LargeQuery extends JMeterQuery {
	private Neo4jDbConnectionState connection = null;
	private LargeQueryParametersProvider qHandler = null;
	private Operation query = null;

	@Override
	public Arguments getDefaultParameters() {
		Arguments defaultParameters = new Arguments();
		defaultParameters.addArgument(JMeterQuery.ARGUMENTS_SERVER_URL, "localhost");
		defaultParameters.addArgument(JMeterQuery.ARGUMENTS_SERVER_PORT, "8080");
		defaultParameters.addArgument("QueryName", "queryXX");
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
		if (qHandler == null) {
			qHandler = QueriesParametersGen.getParams().get(queryName);
		}
		if (query == null) {
			query = LargeQueriesFactory.getNewOperator(queryName);
		}
		String[] arguments = qHandler.getNewArguments();
		String queryGenerated = query.getMessage(arguments);
		ClientResponse obtainedResult;
		this.startTest();
		obtainedResult = connection.execAndCommit(queryGenerated);
		this.stopTest();
		this.setResult(obtainedResult);
		return this.getResults();
	}
}