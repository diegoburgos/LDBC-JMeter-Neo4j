package ldbc.queries.shortQueries;

import java.util.Random;

public class ShortQueryParameterProvider {
	private String[] arguments;
	private final int argsLength;
	private Random random = new Random();

	public ShortQueryParameterProvider(String[] arguments) {
		this.arguments = arguments;
		this.argsLength = arguments.length;
	}

	public synchronized String getNewArguments () {
		return arguments[random.nextInt(argsLength)];
	}

	@Override
	public String toString() {
		return "[ " + argsLength +" arguments ]";
	}
}
