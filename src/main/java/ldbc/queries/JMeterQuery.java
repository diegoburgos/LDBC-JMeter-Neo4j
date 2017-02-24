package ldbc.queries;

import java.io.Serializable;

import com.sun.jersey.api.client.ClientResponse;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

public abstract class JMeterQuery extends AbstractJavaSamplerClient implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static final String ARGUMENTS_SERVER_URL = "Server";
	public static final String ARGUMENTS_SERVER_PORT = "Port";
	private SampleResult results;

	public SampleResult getResults() {
		return results;
	}

	public void startTest () {
		results = new SampleResult();
		results.sampleStart();
	}

	private void setSuccessful () {
		results.setSuccessful(true);
	}

	public void stopTest () {
		results.sampleEnd();
	}

	public abstract SampleResult runTest(JavaSamplerContext context);
	public abstract Arguments getDefaultParameters();

	@SuppressWarnings("deprecation")
	public void setResult(ClientResponse obtainedResult) {
		int responseCode = obtainedResult.getResponseStatus().getStatusCode();
		if (responseCode == 200) {
			this.setSuccessful();
		} else {
			this.getResults().setResponseCode(Integer.toString(responseCode));
		}
        results.setResponseData(obtainedResult.getEntity(String.class));
	}
}
