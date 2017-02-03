package ldbc.driver;

import ldbc.queries.shortQueries.ShortQueryParameterProvider;

public interface ShortOperation extends Operation {
	public String[] getNewArguments(ShortQueryParameterProvider personIds,
			ShortQueryParameterProvider commentIds,
			ShortQueryParameterProvider postIds);
}