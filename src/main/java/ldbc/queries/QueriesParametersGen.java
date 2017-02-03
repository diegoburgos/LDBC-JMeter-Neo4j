package ldbc.queries;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Serializable;
import java.util.*;

import ldbc.driver.neo4j.Neo4jDbQueries;
import ldbc.driver.neo4j.Organization;
import ldbc.queries.largeQueries.LargeQueryParametersProvider;
import ldbc.queries.shortQueries.ShortQueryParameterProvider;
import ldbc.queries.updateQueries.UpdateQueriesFactory;
import ldbc.queries.updateQueries.UpdateQuery;
import ldbc.queries.updateQueries.UpdateQueryParametersProvider;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

public class QueriesParametersGen extends AbstractJavaSamplerClient implements Serializable {
	private final static String MAX_PARAMETERS = "max_parameters";
    private final static String SUBSTITUTION_PARAMETERS = "substitution_parameters";
	private final static String COMMENT_CSV = "comment_csv";
	private final static String POST_CSV = "post_csv";
	private final static String PERSON_CSV = "person_csv";
	private final static String UPDATE_PERSON_CSV = "update_person_csv";
	private final static String UPDATE_FORUM_CSV = "update_forum_csv";

	private static final HashMap<String, LargeQueryParametersProvider> params = new HashMap<String, LargeQueryParametersProvider>();
	private static ShortQueryParameterProvider personIds, commentIds, postIds;
	private static HashMap<String, UpdateQueryParametersProvider> updates;

    private static final Map<String, String> paramDataTypes;
    static {
        Map<String, String> dataTypeMap = new HashMap<>();
        dataTypeMap.put("authorPersonId", "Long");
        dataTypeMap.put("birthday", "Date");
        dataTypeMap.put("browserUsed", "String");
        dataTypeMap.put("cityId", "Long");
        dataTypeMap.put("commentId", "Long");
        dataTypeMap.put("content", "String");
        dataTypeMap.put("countryId", "Long");
        dataTypeMap.put("creationDate", "Date");
        dataTypeMap.put("emails", "List<String>");
        dataTypeMap.put("firstName", "String");
        dataTypeMap.put("forumId", "Long");
        dataTypeMap.put("forumTitle", "String");
        dataTypeMap.put("gender", "String");
        dataTypeMap.put("imageFile", "String");
        dataTypeMap.put("joinDate", "Date");
        dataTypeMap.put("language", "String");
        dataTypeMap.put("lastName", "String");
        dataTypeMap.put("length", "Integer");
        dataTypeMap.put("locationIP", "String");
        dataTypeMap.put("moderatorPersonId", "Long");
        dataTypeMap.put("person1Id", "Long");
        dataTypeMap.put("person2Id", "Long");
        dataTypeMap.put("personId", "Long");
        dataTypeMap.put("postId", "Long");
        dataTypeMap.put("replyToCommentId", "Long");
        dataTypeMap.put("replyToPostId", "Long");
        dataTypeMap.put("speaks", "List<String>");
        dataTypeMap.put("studyAt", "List<Organization>");
        dataTypeMap.put("tagIds", "List<Long>");
        dataTypeMap.put("workAt", "List<Organization>");

        paramDataTypes = Collections.unmodifiableMap(dataTypeMap);
    }

	@Override
	public Arguments getDefaultParameters() {
		Arguments defaultParameters = new Arguments();
		defaultParameters.addArgument(SUBSTITUTION_PARAMETERS, 
				"/local/dburgos/benchmarks/benchmark-ldbc/dataset/factor1/"
				+ "social_network_3/substitution_parameters/");
		defaultParameters.addArgument(COMMENT_CSV, 
				"/local/dburgos/benchmarks/benchmark-ldbc/dataset/factor1/"
				+ "social_network_3/social_network/comment_0_0.csv");
		defaultParameters.addArgument(POST_CSV, 
				"/local/dburgos/benchmarks/benchmark-ldbc/dataset/factor1/"
				+ "social_network_3/social_network/post_0_0.csv");
		defaultParameters.addArgument(PERSON_CSV, 
				"/local/dburgos/benchmarks/benchmark-ldbc/dataset/factor1/"
				+ "social_network_3/social_network/person_0_0.csv");
		defaultParameters.addArgument(UPDATE_PERSON_CSV, 
				"/local/dburgos/benchmarks/benchmark-ldbc/dataset/factor1/"
				+ "social_network_3/social_network/updateStream_0_0_person.csv");
		defaultParameters.addArgument(UPDATE_FORUM_CSV, 
				"/local/dburgos/benchmarks/benchmark-ldbc/dataset/factor1/"
				+ "social_network_3/social_network/updateStream_0_0_forum.csv");
        defaultParameters.addArgument(MAX_PARAMETERS, "1000");
		return defaultParameters;
	}

	public SampleResult runTest(JavaSamplerContext context) {
		String substitutionParametersFolder = context.getParameter(SUBSTITUTION_PARAMETERS);
		final File folder = new File(substitutionParametersFolder);
		for (final File fileEntry : folder.listFiles()) {
			String queryName = fileEntry.getName().split("_param.txt")[0];
			String path = fileEntry.getAbsolutePath();
			params.put(queryName, getQueryParamProv(queryName, path, true));
	    }
		personIds = new ShortQueryParameterProvider(getIds(context.getParameter(PERSON_CSV)));
		commentIds = new ShortQueryParameterProvider(getIds(context.getParameter(COMMENT_CSV)));
		postIds = new ShortQueryParameterProvider(getIds(context.getParameter(POST_CSV)));
		this.updates = getUpdateParameters (context.getParameter(UPDATE_PERSON_CSV), context.getParameter(UPDATE_FORUM_CSV));

        System.out.println(params);
        System.out.println(updates);
        System.out.println(personIds);
        System.out.println(commentIds);
        System.out.println(postIds);

        System.out.println("All parameters are loaded");
        return null;
	}

	public HashMap<String, UpdateQueryParametersProvider> getUpdateParameters(String filePersons, String fileForum) {
		HashMap<String, UpdateQueryParametersProvider> updates = new HashMap<>();
		LinkedList<Object[]> updatePersons = new LinkedList<>();
        String[] propertiesStr = Neo4jDbQueries.LdbcUpdate1AddPersonHandler.properties;
		BufferedReader br = null;
		int i = 0;
		try {
			br = new BufferedReader(new FileReader(filePersons));
			String line = br.readLine();
			while (line != null) {
				updatePersons.add(
				        convertStringsToObjects(propertiesStr, line.split("\\|")));
				i++;
				line = br.readLine();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		String[] queryNames = new String[8];
        for (int p=0; p<queryNames.length;) {
            queryNames[p] = "update_query" + ++p;
        }
		updates.put(queryNames[0], new UpdateQueryParametersProvider(queryNames[0],
                UpdateQueriesFactory.getPropertiesUpdateQuert(queryNames[0]), updatePersons));

        LinkedList<Object[]>[] allArgs = new LinkedList[queryNames.length-1];
        String[][] properties = new String[queryNames.length-1][];

        properties[0] = Neo4jDbQueries.LdbcUpdate2AddPostLikeHandler.properties;
        properties[1] = Neo4jDbQueries.LdbcUpdate3AddCommentLikeHandler.properties;
        properties[2] = Neo4jDbQueries.LdbcUpdate4AddForumHandler.properties;
        properties[3] = Neo4jDbQueries.LdbcUpdate5AddForumMembershipHandler.properties;
        properties[4] = Neo4jDbQueries.LdbcUpdate6AddPostHandler.properties;
        properties[5] = Neo4jDbQueries.LdbcUpdate7AddCommentHandler.properties;
        properties[6] = Neo4jDbQueries.LdbcUpdate8AddFriendshipHandler.properties;

		for (i=0; i<allArgs.length; i++) {
            allArgs[i] = new LinkedList<>();
			updates.put(queryNames[i+1], new UpdateQueryParametersProvider(queryNames[i+1], properties[i], allArgs[i]));
        }

        i=0;
		try {
			br = new BufferedReader(new FileReader(fileForum));
			String line = br.readLine();
			while (line != null) {
                String[] splittedLine = line.split("\\|");
                int opId = Integer.parseInt(splittedLine[2])-2;
                allArgs[opId].add(convertStringsToObjects(properties[opId], splittedLine));
				line = br.readLine();
                i++;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return updates;
	}

	private Object[] convertStringsToObjects (String[] parameters, String[] args1) {
        Object[] args2 = new Object [args1.length-3];
        for (int i=0; i< args1.length-3; i++) {
            switch (paramDataTypes.get(parameters[i])) {
                case "Date":
                    args2[i] = new Date(Long.decode(args1[i+3]));
                    break;
                case "Integer":
                    args2[i] = Integer.decode(args1[i+3]);
                    break;
                case "List<Long>":
                    String[] strs = args1[i+3].split(";");
                    Integer[] nums = new Integer[strs.length];
                    for (int k=0; k<strs.length; k++) {
                        nums[k] = Integer.parseInt(strs[k]);
                    }
                    args2[i] = nums;
                    break;
                case "List<Organization>":
                    Organization[] orgList = null;
                    if (args1[i+3].length() > 0) {
                        String orgs [] = args1[i+3].split(";");
                        orgList = new Organization[orgs.length];
                        for (int k=0; k<orgs.length; k++) {
                            String[] placeAndYear = orgs[k].split(",");
                            long orgId = Long.decode(placeAndYear[0]);
                            int year = Integer.decode(placeAndYear[1]);
                            orgList[k] = new Organization(orgId, year);
                        }
                    }
                    args2[i] = orgList;
                    break;
                case "List<String>":
                    args2[i] = args1[i+3].split(";");
                    break;
                case "Long":
                    args2[i] = Long.decode(args1[i+3]);
                    break;
                case "String":
                    args2[i] = args1[i+3];
                    break;
                default:
                    throw new RuntimeException(String.format("Don't know how to parse "
                                    + "field of type %s for update type %s",
                            paramDataTypes.get(paramDataTypes.get(args1[i+3])), "TODO"));
            }
        }
        return args2;
    }

	private String[] getIds (String sourceFile) {
		String[] toFill = new String[countLines(sourceFile)];
		BufferedReader br = null;
		int i = 0;
		try {
			br = new BufferedReader(new FileReader(sourceFile));
            br.readLine(); // header is discarded
			String line = br.readLine();
			while (line != null) {
				toFill[i] = line.split("\\|")[0];
				i++;
				line = br.readLine();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return toFill;
	}

	public LargeQueryParametersProvider getQueryParamProv (String queryName, String file, boolean hasHeader) {
		int samples = this.countLines(file);
		if (!hasHeader) {
			samples++;
		}
		String[][] arguments = new String[samples][];
		String[] header = null;
		int i=0;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			if (hasHeader) {
				header = line.split("\\|");
				line = br.readLine();
			}
			while (line != null) {
				arguments[i++] = line.split("\\|");
				line = br.readLine();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return new LargeQueryParametersProvider(queryName, arguments, header);
	}

	private int countLines (String file) {
		LineNumberReader lnr = null;
		int totalLines = -1;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));
			String line;
			while ((line = br.readLine()) != null) {
				totalLines++;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return totalLines;
	}

	public static UpdateQueryParametersProvider getUpdateParameters (String queryName) {
        return updates.get(queryName);
    }

	public static ShortQueryParameterProvider getPersonIds() {
		return personIds;
	}

	public static ShortQueryParameterProvider getCommentIds() {
		return commentIds;
	}

	public static ShortQueryParameterProvider getPostIds() {
		return postIds;
	}

	public static HashMap<String, LargeQueryParametersProvider> getParams() {
		return params;
	}
}
