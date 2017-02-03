package ldbc.queries.updateQueries;

import java.util.Arrays;
import java.util.LinkedList;

public class UpdateQueryParametersProvider {
	private final String queryName;
	private final LinkedList<Object[]> arguments;
    private final String[] properties;

	public UpdateQueryParametersProvider(String queryName, String[] properties, LinkedList<Object[]> arguments) {
		this.queryName = queryName;
		this.arguments = arguments;
        this.properties = properties;
	}

	public LinkedList<Object[]> getArguments () {
		return arguments;
	}

	public synchronized Object[] getNewArguments () {
        return arguments.remove();
    }

	public String getQueryName() {
		return queryName;
	}

	@Override
	public String toString() {
		return queryName + "[" + arguments.size() +
				" arguments " + "]";
        /*return queryName + "[" + arguments.size()
                + " arguments\n\t{ " + Arrays.toString(properties) + " }, "
                + " \n\tvalues : {" + argumentsToText() + " } ]";*/
	}
	private String argumentsToText() {
        String ret = " [ ";
        for (Object[] ob : arguments) {
            ret += "\n\t\t" + Arrays.toString(ob) + ", ";
        }
        return ret + " ]";
    }

    public String[] getProperties() {
        return properties;
    }
}
