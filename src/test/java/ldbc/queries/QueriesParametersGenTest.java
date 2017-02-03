package ldbc.queries;

import ldbc.driver.neo4j.Neo4jDbQueries;
import ldbc.queries.updateQueries.UpdateQueryParametersProvider;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;

/**
 * Created by dburgos on 31/01/17.
 */
public class QueriesParametersGenTest {

    @Test
    public void testingUpdateParameters () {
        HashMap<String, UpdateQueryParametersProvider> updates;
        String filePersons = "/local/dburgos/benchmarks/benchmark-ldbc/dataset/factor1/"
                + "social_network_3/social_network/updateStream_0_0_person.csv",
                fileForum = "/local/dburgos/benchmarks/benchmark-ldbc/dataset/factor1/"
                        + "social_network_3/social_network/updateStream_0_0_forum.csv";
        QueriesParametersGen gen = new QueriesParametersGen();
        updates = gen.getUpdateParameters (filePersons, fileForum);
        for (int i=0; i<10; i++) {
            Object[] ob = updates.get("update_query1").getNewArguments();
            String[] properties = updates.get("update_query1").getProperties();
            this.printProperties(properties, ob);
            Neo4jDbQueries.LdbcUpdate1AddPersonHandler qHandler = new Neo4jDbQueries.LdbcUpdate1AddPersonHandler();
            System.out.println(qHandler.getMessage(ob));
        }
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