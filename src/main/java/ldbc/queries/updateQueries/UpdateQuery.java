package ldbc.queries.updateQueries;

import ldbc.driver.UpdateOperation;
import ldbc.driver.neo4j.Neo4jDbConnectionState;
import ldbc.queries.JMeterQuery;
import ldbc.queries.QueriesParametersGen;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import com.sun.jersey.api.client.ClientResponse;

import java.util.Arrays;

public class UpdateQuery extends JMeterQuery {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Neo4jDbConnectionState connection = null;
	private UpdateOperation query = null;
	private UpdateQueryParametersProvider parametersProvider;
	private String endUri = null;

	@Override
	public Arguments getDefaultParameters() {
		Arguments defaultParameters = new Arguments();
		defaultParameters.addArgument(JMeterQuery.ARGUMENTS_SERVER_URL, "localhost");
		defaultParameters.addArgument(JMeterQuery.ARGUMENTS_SERVER_PORT, "8080");
		defaultParameters.addArgument("QueryName", "update_queryXX");
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
		if (parametersProvider == null) {
			parametersProvider = QueriesParametersGen.getUpdateParameters(queryName);
		}
		if (query == null) {
			query = UpdateQueriesFactory.getNewOperator(queryName);
		}
		if (endUri == null) {
            if (queryName.equals("update_query1")
                    || queryName.equals("update_query6")
                    || queryName.equals("update_query7")) {
                endUri = "transaction/commit/";
            } else {
                endUri = "cypher/";
            }
        }
		Object[] ob = parametersProvider.getNewArguments();
		// Get the right arguments depending on the query
        String queryGenerated = query.getMessage(ob);
        ClientResponse obtainedResult;
		this.startTest();
        obtainedResult = connection.execAndCommit(queryGenerated, endUri);
		this.stopTest();
		this.setResult(obtainedResult);
        return this.getResults();
	}

    public static void printProperties (String[] properties, Object[] ob) {
        for (int i=0; i<ob.length; i++) {
            if (ob[i] instanceof Object[])
                System.out.println("\t[" + i + "] " + properties[i] + " : " + Arrays.toString((Object[])ob[i]));
            else
                System.out.println("\t[" + i + "] " + properties[i] + " : " + ob[i]);
        }
    }
}
