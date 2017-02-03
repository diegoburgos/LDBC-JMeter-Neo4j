package ldbc.queries.largeQueries;

import java.util.Arrays;
import java.util.Random;

public class LargeQueryParametersProvider {
	private final String queryName;
	private final String[] header = null;
	private final String[][] arguments;
	private final int argsLength;
	private final Random random = new Random();

	public LargeQueryParametersProvider(String queryName, String[][] arguments, String[] header) {
		this.queryName = queryName;
		this.arguments = arguments;
		this.argsLength = arguments.length;
	}

	public String[][] getArguments () {
		return arguments;
	}

	public String[] getNewArguments () {
		return arguments[random.nextInt(argsLength)];
	}

	public String[] getHeader() {
		return header;
	}

	public String getQueryName() {
		return queryName;
	}

	@Override
	public String toString() {
		return queryName + "[" + argsLength +
				" arguments, header " + Arrays.toString(header) + "]";
	}
}
