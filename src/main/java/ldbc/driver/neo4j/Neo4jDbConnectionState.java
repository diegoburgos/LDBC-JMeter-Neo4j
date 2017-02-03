package ldbc.driver.neo4j;

import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class Neo4jDbConnectionState {
	private final String uri;
	private final Client client;

	public Neo4jDbConnectionState(String host, String port) {
		this.uri = "http://" + host + ":" + port + "/db/data/";
		this.client = Client.create();
	}
	
	public String toString () {
		return "REST Client [" + uri + "]";
	}

	public ClientResponse execAndCommit(String message) {
		return execAndCommit(message, "cypher/");
	}

	public ClientResponse execAndCommit(String message, String endUri) {
        ClientResponse response;
		response = client.resource(uri + endUri)
				.accept(MediaType.APPLICATION_JSON)
				.type(MediaType.APPLICATION_JSON)
				.entity(message)
				.post(ClientResponse.class);
		return response;
	}
}
