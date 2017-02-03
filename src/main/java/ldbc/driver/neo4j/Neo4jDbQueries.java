/* 
 * Copyright (C) 2015-2016 Stanford University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package ldbc.driver.neo4j;

import java.util.Date;

import ldbc.driver.Operation;
import ldbc.driver.ShortOperation;
import ldbc.driver.UpdateOperation;
import ldbc.driver.util.DbHelper;
import ldbc.queries.shortQueries.ShortQueryParameterProvider;
import ldbc.queries.updateQueries.UpdateQuery;

/**
 * An implementation of the LDBC SNB interactive workload[1] for Neo4j. Queries
 * are executed against a running Neo4j server. Configuration parameters for
 * this implementation (that are supplied via the LDBC driver) are listed
 * below.
 * <p>
 * Configuration Parameters:
 * <ul>
 * <li>host - IP address of the Neo4j web server (default: 127.0.0.1).</li>
 * <li>port - port of the Neo4j web server (default: 7474).</li>
 * </ul>
 * <p>
 * References:<br>
 * [1]: Prat, Arnau (UPC) and Boncz, Peter (VUA) and Larriba, Josep Lluís (UPC)
 * and Angles, Renzo (TALCA) and Averbuch, Alex (NEO) and Erling, Orri (OGL)
 * and Gubichev, Andrey (TUM) and Spasić, Mirko (OGL) and Pham, Minh-Duc (VUA)
 * and Martínez, Norbert (SPARSITY). "LDBC Social Network Benchmark (SNB) -
 * v0.2.2 First Public Draft Release". http://www.ldbcouncil.org/.
 * <p>
 * TODO:<br>
 * <ul>8
 * </ul>
 * <p>
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class Neo4jDbQueries {
	private static final String operationLimit = "10";

	public static final String getString (String statement, String parameters) {
		return "{\"query\": \"" + statement + "\"" +
				", \"params\": " + parameters + "}";
	}


	public static final String getMultipleStatements (String statement1, String parameter1,
			String statement2, String parameter2) {
	    return getMultipleStatements(new String[]{statement1, statement2}, new String[]{parameter1, parameter2});
	}

    public static final String getMultipleStatements (String[] statements, String[] parameters) {
	    StringBuffer sb = new StringBuffer("{\"statements\" : [ ");
	    for (int i=0; i<statements.length; i++) {
	        if (parameters[i] != null && statements[i] != null) {
	            if (i>0) {
	                sb.append(", ");
                }
                sb.append("{\"statement\": \"").append(statements[i]).append("\"").append(", \"parameters\": ")
                        .append(parameters[i]).append("}");
            }
        }
        return sb.append("] }").toString();
    }

	/**
	 * ------------------------------------------------------------------------
	 * Complex Queries
	 * ------------------------------------------------------------------------
	 */
	/**
	 * Given a start Person, find up to 20 Persons with a given first name that
	 * the start Person is connected to (excluding start Person) by at most 3
	 * steps via Knows relationships. Return Persons, including summaries of the
	 * Persons workplaces and places of study. Sort results ascending by their
	 * distance from the start Person, for Persons within the same distance sort
	 * ascending by their last name, and for Persons with same last name
	 * ascending by their identifier.[1]
	 */
	public static class LdbcQuery1Handler implements Operation {
		public static final String[] arguments = "Person|Name".split("\\|");
		private static final String statement =
				"   MATCH (:Person {id:{1}})-[path:KNOWS*1..3]-(friend:Person)"
						+ " WHERE friend.firstName = {2}"
						+ " WITH friend, min(length(path)) AS distance"
						+ " ORDER BY distance ASC, friend.lastName ASC, toInt(friend.id) ASC"
						+ " LIMIT {3}"
						+ " MATCH (friend)-[:IS_LOCATED_IN]->(friendCity:Place)"
						+ " OPTIONAL MATCH (friend)-[studyAt:STUDY_AT]->(uni:Organisation)-[:IS_LOCATED_IN]->(uniCity:Place)"
						+ " WITH"
						+ "   friend,"
						+ "   collect("
						+ "     CASE uni.name"
						+ "       WHEN null THEN null"
						+ "       ELSE [uni.name, studyAt.classYear, uniCity.name]"
						+ "     END"
						+ "   ) AS unis,"
						+ "   friendCity,"
						+ "   distance"
						+ " OPTIONAL MATCH (friend)-[worksAt:WORK_AT]->(company:Organisation)-[:IS_LOCATED_IN]->(companyCountry:Place)"
						+ " WITH"
						+ "   friend,"
						+ "   collect("
						+ "     CASE company.name"
						+ "       WHEN null THEN null"
						+ "       ELSE [company.name, worksAt.workFrom, companyCountry.name]"
						+ "     END"
						+ "   ) AS companies,"
						+ "   unis,"
						+ "   friendCity,"
						+ "   distance"
						+ " RETURN"
						+ "   friend.id AS id,"
						+ "   friend.lastName AS lastName,"
						+ "   distance,"
						+ "   friend.birthday AS birthday,"
						+ "   friend.creationDate AS creationDate,"
						+ "   friend.gender AS gender,"
						+ "   friend.browserUsed AS browser,"
						+ "   friend.locationIP AS locationIp,"
						+ "   friend.email AS emails,"
						+ "   friend.speaks AS languages,"
						+ "   friendCity.name AS cityName,"
						+ "   unis,"
						+ "   companies"
						+ " ORDER BY distance ASC, friend.lastName ASC, toInt(friend.id) ASC"
						+ " LIMIT {3}";
		public String getMessage(String[] args) {
			String parameters = "{ "
					+ "\"1\" : \"" + args[0] + "\", "
					+ "\"2\" : \"" + args[1] + "\", "
					+ "\"3\" : " + operationLimit
					+ " }";
			return getString (statement, parameters);
		}
	}

	/**
	 * Given a start Person, find (most recent) Posts and Comments from all of
	 * that Person’s friends, that were created before (and including) a given
	 * date. Return the top 20 Posts/Comments, and the Person that created each
	 * of them. Sort results descending by creation date, and then ascending by
	 * Post identifier.[1]
	 */
	public static class LdbcQuery2Handler implements Operation {
		public static final String[] arguments = "Person|Date0".split("\\|");
		private static final String statement =
				"   MATCH (:Person {id:{1}})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(message)"
						+ " WHERE message.creationDate <= {2} AND (message:Post OR message:Comment)"
						+ " RETURN"
						+ "   friend.id AS personId,"
						+ "   friend.firstName AS personFirstName,"
						+ "   friend.lastName AS personLastName,"
						+ "   message.id AS messageId,"
						+ "   CASE has(message.content)"
						+ "     WHEN true THEN message.content"
						+ "     ELSE message.imageFile"
						+ "   END AS messageContent,"
						+ "   message.creationDate AS messageDate"
						+ " ORDER BY messageDate DESC, toInt(messageId) ASC"
						+ " LIMIT {3}";
		public String getMessage(String[] args) {
			String parameters = "{ "
					+ "\"1\" : \"" + args[0] + "\", "
					+ "\"2\" : " + args[1] + ", "
					+ "\"3\" : " + operationLimit
					+ " }";
			return getString (statement, parameters);
		}
	}

	/**
	 * Given a start Person, find Persons that are their friends and friends of
	 * friends (excluding start Person) that have made Posts/Comments in both of
	 * the given Countries, X and Y, within a given period. Only Persons that are
	 * foreign to Countries X and Y are considered, that is Persons whose
	 * Location is not Country X or Country Y. Return top 20 Persons, and their
	 * Post/Comment counts, in the given countries and period. Sort results
	 * descending by total number of Posts/Comments, and then ascending by Person
	 * identifier.[1]
	 */
	public static class LdbcQuery3Handler implements Operation {
		public static final String[] arguments = "Person|Date0|Duration|Country1|Country2".split("\\|");
		private static final String statement =
				"   MATCH (person:Person {id:{1}})-[:KNOWS*1..2]-(friend:Person)<-[:HAS_CREATOR]-(messageX),"
						+ " (messageX)-[:IS_LOCATED_IN]->(countryX:Place)"
						+ " WHERE"
						+ "   not(person=friend)"
						+ "   AND not((friend)-[:IS_LOCATED_IN]->()-[:IS_PART_OF]->(countryX))"
						+ "   AND countryX.name={2} AND messageX.creationDate>={4}"
						+ "   AND messageX.creationDate<{5}"
						+ " WITH friend, count(DISTINCT messageX) AS xCount"
						+ " MATCH (friend)<-[:HAS_CREATOR]-(messageY)-[:IS_LOCATED_IN]->(countryY:Place)"
						+ " WHERE"
						+ "   countryY.name={3}"
						+ "   AND not((friend)-[:IS_LOCATED_IN]->()-[:IS_PART_OF]->(countryY))"
						+ "   AND messageY.creationDate>={4}"
						+ "   AND messageY.creationDate<{5}"
						+ " WITH"
						+ "   friend.id AS friendId,"
						+ "   friend.firstName AS friendFirstName,"
						+ "   friend.lastName AS friendLastName,"
						+ "   xCount,"
						+ "   count(DISTINCT messageY) AS yCount"
						+ " RETURN"
						+ "   friendId,"
						+ "   friendFirstName,"
						+ "   friendLastName,"
						+ "   xCount,"
						+ "   yCount,"
						+ "   xCount + yCount AS xyCount"
						+ " ORDER BY xyCount DESC, toInt(friendId) ASC"
						+ " LIMIT {6}";
		public String getMessage(String[] args) {
			long periodStart = Long.parseLong(args[1]);
			long periodEnd = periodStart
					+ Long.parseLong(args[2]) * 24l * 60l * 60l * 1000l;
			String parameters = "{ "
					+ "\"1\" : \"" + args[0] + "\", "
					+ "\"2\" : \"" + args[3] + "\", "
					+ "\"3\" : \"" + args[4] + "\", "
					+ "\"4\" : " + periodStart + ", "
					+ "\"5\" : " + periodEnd + ", "
					+ "\"6\" : " + operationLimit
					+ " }";
			return getString (statement, parameters);
		}
	}

	/**
	 * Given a start Person, find Tags that are attached to Posts that were
	 * created by that Person’s friends. Only include Tags that were attached to
	 * friends’ Posts created within a given time interval, and that were never
	 * attached to friends’ Posts created before this interval. Return top 10
	 * Tags, and the count of Posts, which were created within the given time
	 * interval, that this Tag was attached to. Sort results descending by Post
	 * count, and then ascending by Tag name.[1]
	 */
	public static class LdbcQuery4Handler implements Operation {
		public static final String[] arguments = "Person|Date0|Duration".split("\\|");
		private static final String statement =
				"   MATCH (person:Person {id:{1}})-[:KNOWS]-(:Person)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag:Tag)"
						+ " WHERE post.creationDate >= {2} AND post.creationDate < {3}"
						+ " OPTIONAL MATCH (tag)<-[:HAS_TAG]-(oldPost:Post)-[:HAS_CREATOR]->(:Person)-[:KNOWS]-(person)"
						+ " WHERE oldPost.creationDate < {2}"
						+ " WITH tag, post, length(collect(oldPost)) AS oldPostCount"
						+ " WHERE oldPostCount=0"
						+ " RETURN"
						+ "   tag.name AS tagName,"
						+ "   length(collect(post)) AS postCount"
						+ " ORDER BY postCount DESC, tagName ASC"
						+ " LIMIT {4}";

		public String getMessage(String[] args) {
			long periodStart = Long.parseLong(args[1]);
			long periodEnd = periodStart
					+ Long.parseLong(args[2]) * 24l * 60l * 60l * 1000l;
			String parameters = "{ "
					+ "\"1\" : \"" + args[0] + "\", "
					+ "\"2\" : " + periodStart + ", "
					+ "\"3\" : " + periodEnd + ", "
					+ "\"4\" : " + operationLimit
					+ " }";
			return getString (statement, parameters);
		}
	}

	/**
	 * Given a start Person, find the Forums which that Person’s friends and
	 * friends of friends (excluding start Person) became Members of after a
	 * given date. Return top 20 Forums, and the number of Posts in each Forum
	 * that was Created by any of these Persons. For each Forum consider only
	 * those Persons which joined that particular Forum after the given date.
	 * Sort results descending by the count of Posts, and then ascending by Forum
	 * identifier.[1]
	 */

	public static class LdbcQuery5Handler implements Operation {
		public static final String[] arguments = "Person|Date0".split("\\|");
		private static final String statement =
				"   MATCH (person:Person {id:{1}})-[:KNOWS*1..2]-(friend:Person)<-[membership:HAS_MEMBER]-(forum:Forum)"
						+ " WHERE membership.joinDate>{2} AND not(person=friend)"
						+ " WITH DISTINCT friend, forum"
						+ " OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)<-[:CONTAINER_OF]-(forum)"
						+ " WITH forum, count(post) AS postCount"
						+ " RETURN"
						+ "   forum.title AS forumName,"
						+ "   postCount"
						+ " ORDER BY postCount DESC, toInt(forum.id) ASC"
						+ " LIMIT {3}";
		public String getMessage(String[] args) {
			String parameters = "{ "
					+ "\"1\" : \"" + args[0] + "\", "
					+ "\"2\" : " + args[1] + ", "
					+ "\"3\" : " + operationLimit
					+ " }";
			return getString (statement, parameters);
		}
	}

	/**
	 * Given a start Person and some Tag, find the other Tags that occur together
	 * with this Tag on Posts that were created by start Person’s friends and
	 * friends of friends (excluding start Person). Return top 10 Tags, and the
	 * count of Posts that were created by these Persons, which contain both this
	 * Tag and the given Tag. Sort results descending by count, and then
	 * ascending by Tag name.[1]
	 */
	public static class LdbcQuery6Handler implements Operation {
		public static final String[] arguments = "Person|Tag".split("\\|");
		private static final String statement =
				"   MATCH"
						+ "   (person:Person {id:{1}})-[:KNOWS*1..2]-(friend:Person),"
						+ "   (friend)<-[:HAS_CREATOR]-(friendPost:Post)-[:HAS_TAG]->(knownTag:Tag {name:{2}})"
						+ " WHERE not(person=friend)"
						+ " MATCH (friendPost)-[:HAS_TAG]->(commonTag:Tag)"
						+ " WHERE not(commonTag=knownTag)"
						+ " WITH DISTINCT commonTag, knownTag, friend"
						+ " MATCH (commonTag)<-[:HAS_TAG]-(commonPost:Post)-[:HAS_TAG]->(knownTag)"
						+ " WHERE (commonPost)-[:HAS_CREATOR]->(friend)"
						+ " RETURN"
						+ "   commonTag.name AS tagName,"
						+ "   count(commonPost) AS postCount"
						+ " ORDER BY postCount DESC, tagName ASC"
						+ " LIMIT {3}";
		public String getMessage(String[] args) {
			String parameters = "{ "
					+ "\"1\" : \"" + args[0] + "\", "
					+ "\"2\" : \"" + args[1] + "\", "
					+ "\"3\" : " + operationLimit
					+ " }";
			return getString (statement, parameters);
		}
	}

	/**
	 * Given a start Person, find (most recent) Likes on any of start Person’s
	 * Posts/Comments. Return top 20 Persons that Liked any of start Person’s
	 * Posts/Comments, the Post/Comment they liked most recently, creation date
	 * of that Like, and the latency (in minutes) between creation of
	 * Post/Comment and Like. Additionally, return a flag indicating whether the
	 * liker is a friend of start Person. In the case that a Person Liked
	 * multiple Posts/Comments at the same time, return the Post/Comment with
	 * lowest identifier. Sort results descending by creation time of Like, then
	 * ascending by Person identifier of liker.[1]
	 */
	public static class LdbcQuery7Handler implements Operation {
		public static final String[] arguments = new String[] {"Person"};
		private static final String statement =
				"   MATCH (person:Person {id:{1}})<-[:HAS_CREATOR]-(message)<-[like:LIKES]-(liker:Person)"
						+ " WITH liker, message, like.creationDate AS likeTime, person"
						+ " ORDER BY likeTime DESC, toInt(message.id) ASC"
						+ " WITH"
						+ "   liker,"
						+ "   head(collect({msg: message, likeTime: likeTime})) AS latestLike,"
						+ "   person"
						+ " RETURN"
						+ "   liker.id AS personId,"
						+ "   liker.firstName AS personFirstName,"
						+ "   liker.lastName AS personLastName,"
						+ "   latestLike.likeTime AS likeTime,"
						+ "   latestLike.msg.id AS messageId,"
						+ "   CASE has(latestLike.msg.content)"
						+ "     WHEN true THEN latestLike.msg.content"
						+ "     ELSE latestLike.msg.imageFile"
						+ "   END AS messageContent,"
						+ "   latestLike.likeTime - latestLike.msg.creationDate AS latencyAsMilli,"
						+ "   not((liker)-[:KNOWS]-(person)) AS isNew"
						+ " ORDER BY likeTime DESC, toInt(personId) ASC"
						+ " LIMIT {2}";
		public String getMessage(String[] args) {
			String parameters = "{ "
					+ "\"1\" : \"" + args[0] + "\", "
					+ "\"2\" : " + operationLimit
					+ " }";
			return getString (statement, parameters);
		}
	}

	/**
	 * Given a start Person, find (most recent) Comments that are replies to
	 * Posts/Comments of the start Person. Only consider immediate (1-hop)
	 * replies, not the transitive (multi-hop) case. Return the top 20 reply
	 * Comments, and the Person that created each reply Comment. Sort results
	 * descending by creation date of reply Comment, and then ascending by
	 * identifier of reply Comment.[1]
	 */
	public static class LdbcQuery8Handler implements Operation {
		public static final String[] arguments = new String[] {"Person"};
		private static final String statement =
				"   MATCH"
						+ "   (start:Person {id:{1}})<-[:HAS_CREATOR]-()<-[:REPLY_OF]-(comment:Comment)-[:HAS_CREATOR]->(person:Person)"
						+ " RETURN"
						+ "   person.id AS personId,"
						+ "   person.firstName AS personFirstName,"
						+ "   person.lastName AS personLastName,"
						+ "   comment.creationDate AS commentCreationDate,"
						+ "   comment.id AS commentId,"
						+ "   comment.content AS commentContent"
						+ " ORDER BY commentCreationDate DESC, toInt(commentId) ASC"
						+ " LIMIT {2}";
		public String getMessage(String[] args) {
			String parameters = "{ "
					+ "\"1\" : \"" + args[0] + "\", "
					+ "\"2\" : " + operationLimit
					+ " }";
			return getString (statement, parameters);
		}
	}

	/**
	 * Given a start Person, find the (most recent) Posts/Comments created by
	 * that Person’s friends or friends of friends (excluding start Person). Only
	 * consider the Posts/Comments created before a given date (excluding that
	 * date). Return the top 20 Posts/Comments, and the Person that created each
	 * of those Posts/Comments. Sort results descending by creation date of
	 * Post/Comment, and then ascending by Post/Comment identifier.[1]
	 */
	public static class LdbcQuery9Handler implements Operation {
		public static final String[] arguments = "Person|Date0".split("\\|");
		private static final String statement =
				"   MATCH (:Person {id:{1}})-[:KNOWS*1..2]-(friend:Person)<-[:HAS_CREATOR]-(message)"
						+ " WHERE message.creationDate < {2}"
						+ " RETURN DISTINCT"
						+ "   friend.id AS personId,"
						+ "   friend.firstName AS personFirstName,"
						+ "   friend.lastName AS personLastName,"
						+ "   message.id AS messageId,"
						+ "   CASE has(message.content)"
						+ "     WHEN true THEN message.content"
						+ "     ELSE message.imageFile"
						+ "   END AS messageContent,"
						+ "   message.creationDate AS messageCreationDate"
						+ " ORDER BY message.creationDate DESC, toInt(message.id) ASC"
						+ " LIMIT {3}";

		public String getMessage(String[] args) {
			// TODO args[1] must be a date 
			String parameters = "{ "
					+ "\"1\" : \"" + args[0] + "\", "
					+ "\"2\" : " + args[1] + ", "
					+ "\"3\" : " + operationLimit
					+ " }";
			return getString (statement, parameters);
		}
	}

	/**
	 * Given a start Person, find that Person’s friends of friends (excluding
	 * start Person, and immediate friends), who were born on or after the 21st
	 * of a given month (in any year) and before the 22nd of the following month.
	 * Calculate the similarity between each of these Persons and start Person,
	 * where similarity for any Person is defined as follows:
	 * <ul>
	 * <li>common = number of Posts created by that Person, such that the Post
	 * has a Tag that start Person is Interested in</li>
	 * <li>uncommon = number of Posts created by that Person, such that the Post
	 * has no Tag that start Person is Interested in</li>
	 * <li>similarity = common - uncommon</li>
	 * </ul>
	 * Return top 10 Persons, their Place, and their similarity score. Sort
	 * results descending by similarity score, and then ascending by Person
	 * identifier.[1]
	 */
	public static class LdbcQuery10Handler implements Operation {
		public static final String[] arguments = "Person|HS0".split("\\|");
		private static final String statement =
				"   MATCH (person:Person {id:{1}})-[:KNOWS*2..2]-(friend:Person)-[:IS_LOCATED_IN]->(city:Place)"
						+ " WHERE "
						+ "   ((friend.birthday_month = {2} AND friend.birthday_day >= 21) OR"
						+ "   (friend.birthday_month = ({2}%12)+1 AND friend.birthday_day < 22))"
						+ "   AND not(friend=person)"
						+ "   AND not((friend)-[:KNOWS]-(person))"
						+ " WITH DISTINCT friend, city, person"
						+ " OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)"
						+ " WITH friend, city, collect(post) AS posts, person"
						+ " WITH "
						+ "   friend,"
						+ "   city,"
						+ "   length(posts) AS postCount,"
						+ "   length([p IN posts WHERE (p)-[:HAS_TAG]->(:Tag)<-[:HAS_INTEREST]-(person)]) AS commonPostCount"
						+ " RETURN"
						+ "   friend.id AS personId,"
						+ "   friend.firstName AS personFirstName,"
						+ "   friend.lastName AS personLastName,"
						+ "   friend.gender AS personGender,"
						+ "   city.name AS personCityName,"
						+ "   commonPostCount - (postCount - commonPostCount) AS commonInterestScore"
						+ " ORDER BY commonInterestScore DESC, toInt(personId) ASC"
						+ " LIMIT {3}";
		public String getMessage(String[] args) {
			String parameters = "{ "
					+ "\"1\" : \"" + args[0] + "\", "
					+ "\"2\" : " + args[1] + ", "
					+ "\"3\" : " + operationLimit
					+ " }";
			return getString (statement, parameters);
		}
	}

	/**
	 * Given a start Person, find that Person’s friends and friends of friends
	 * (excluding start Person) who started Working in some Company in a given
	 * Country, before a given date (year). Return top 10 Persons, the Company
	 * they worked at, and the year they started working at that Company. Sort
	 * results ascending by the start date, then ascending by Person identifier,
	 * and lastly by Organization name descending.[1]
	 */
	public static class LdbcQuery11Handler implements Operation {
		public static final String[] arguments = "Person|Country|Year".split("\\|");
		private static final String statement =
				"   MATCH (person:Person {id:{1}})-[:KNOWS*1..2]-(friend:Person)"
						+ " WHERE not(person=friend)"
						+ " WITH DISTINCT friend"
						+ " MATCH (friend)-[worksAt:WORK_AT]->(company:Organisation)-[:IS_LOCATED_IN]->(:Place {name:{3}})"
						+ " WHERE worksAt.workFrom < {2}"
						+ " RETURN"
						+ "   friend.id AS friendId,"
						+ "   friend.firstName AS friendFirstName,"
						+ "   friend.lastName AS friendLastName,"
						+ "   company.name AS companyName,"
						+ "   worksAt.workFrom AS workFromYear"
						+ " ORDER BY workFromYear ASC, toInt(friendId) ASC, companyName DESC"
						+ " LIMIT {4}";
		public String getMessage(String[] args) {
			String parameters = "{ "
					+ "\"1\" : \"" + args[0] + "\", "
					+ "\"2\" : " + args[2] + ", "
					+ "\"3\" : \"" + args[1] + "\", "
					+ "\"4\" : " + operationLimit
					+ " }";
			return getString (statement, parameters);
		}
	}

	/**
	 * Given a start Person, find the Comments that this Person’s friends made in
	 * reply to Posts, considering only those Comments that are immediate (1-hop)
	 * replies to Posts, not the transitive (multi-hop) case. Only consider Posts
	 * with a Tag in a given TagClass or in a descendent of that TagClass. Count
	 * the number of these reply Comments, and collect the Tags (with valid tag
	 * class) that were attached to the Posts they replied to. Return top 20
	 * Persons with at least one reply, the reply count, and the collection of
	 * Tags. Sort results descending by Comment count, and then ascending by
	 * Person identifier.[1]
	 */
	public static class LdbcQuery12Handler implements Operation {
		public static final String[] arguments = "Person|TagType".split("\\|");
		private static final String statement =
				"   MATCH (:Person {id:{1}})-[:KNOWS]-(friend:Person)"
						+ " OPTIONAL MATCH"
						+ "   (friend)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(:Post)-[:HAS_TAG]->(tag:Tag),"
						+ "   (tag)-[:HAS_TYPE]->(tagClass:TagClass)-[:IS_SUBCLASS_OF*0..]->(baseTagClass:TagClass)"
						+ " WHERE tagClass.name = {2} OR baseTagClass.name = {2}"
						+ " RETURN"
						+ "   friend.id AS friendId,"
						+ "   friend.firstName AS friendFirstName,"
						+ "   friend.lastName AS friendLastName,"
						+ "   collect(DISTINCT tag.name) AS tagNames,"
						+ "   count(DISTINCT comment) AS count"
						+ " ORDER BY count DESC, toInt(friendId) ASC"
						+ " LIMIT {3}";
		public String getMessage(String[] args) {
			String parameters = "{ "
					+ "\"1\" : \"" + args[0] + "\", "
					+ "\"2\" : \"" + args[1] + "\", "
					+ "\"3\" : " + operationLimit
					+ " }";
			return getString (statement, parameters);
		}
	}

	/**
	 * Given two Persons, find the shortest path between these two Persons in the
	 * subgraph induced by the Knows relationships. Return the length of this
	 * path. -1 should be returned if no path is found, and 0 should be returned
	 * if the start person is the same as the end person.[1]
	 */
	public static class LdbcQuery13Handler implements Operation {
		public static final String[] arguments = "Person1|Person2".split("\\|");
		private static final String statement =
				"   MATCH (person1:Person {id:{1}}), (person2:Person {id:{2}})"
						+ " OPTIONAL MATCH path = shortestPath((person1)-[:KNOWS*..15]-(person2))"
						+ " RETURN"
						+ " CASE path IS NULL"
						+ "   WHEN true THEN -1"
						+ "   ELSE length(path)"
						+ " END AS pathLength";
		public String getMessage(String[] args) {
			String parameters = "{ "
					+ "\"1\" : \"" + args[0] + "\", "
					+ "\"2\" : \"" + args[1] + "\""
					+ " }";
			return getString (statement, parameters);
		}
	}

	/**
	 * Given two Persons, find all (unweighted) shortest paths between these two
	 * Persons, in the subgraph induced by the Knows relationship. Then, for each
	 * path calculate a weight. The nodes in the path are Persons, and the weight
	 * of a path is the sum of weights between every pair of consecutive Person
	 * nodes in the path. The weight for a pair of Persons is calculated such
	 * that every reply (by one of the Persons) to a Post (by the other Person)
	 * contributes 1.0, and every reply (by ones of the Persons) to a Comment (by
	 * the other Person) contributes 0.5. Return all the paths with shortest
	 * length, and their weights. Sort results descending by path weight. The
	 * order of paths with the same weight is unspecified.[1]
	 */
	public static class LdbcQuery14Handler implements Operation {
		public static final String[] arguments = "Person1|Person2".split("\\|");
		private static final String statement =
				"   MATCH path = allShortestPaths((person1:Person {id:{1}})-[:KNOWS*..15]-(person2:Person {id:{2}}))"
						+ " WITH nodes(path) AS pathNodes"
						+ " RETURN"
						+ "   extract(n IN pathNodes | n.id) AS pathNodeIds,"
						+ "   reduce(weight=0.0, idx IN range(1,size(pathNodes)-1) | extract(prev IN [pathNodes[idx-1]] | extract(curr IN [pathNodes[idx]] | weight + length((curr)<-[:HAS_CREATOR]-(:Comment)-[:REPLY_OF]->(:Post)-[:HAS_CREATOR]->(prev))*1.0 + length((prev)<-[:HAS_CREATOR]-(:Comment)-[:REPLY_OF]->(:Post)-[:HAS_CREATOR]->(curr))*1.0 + length((prev)-[:HAS_CREATOR]-(:Comment)-[:REPLY_OF]-(:Comment)-[:HAS_CREATOR]-(curr))*0.5) )[0][0]) AS weight"
						+ " ORDER BY weight DESC";
		public String getMessage(String[] args) {
			String parameters = "{ "
					+ "\"1\" : \"" + args[0] + "\", "
					+ "\"2\" : \"" + args[1] + "\""
					+ " }";
			return getString (statement, parameters);
		}
	}

	/**
	 * ------------------------------------------------------------------------
	 * Short Queries
	 * ------------------------------------------------------------------------
	 */

	/**
	 * Given a start Person, retrieve their first name, last name, birthday, IP
	 * address, browser, and city of residence.[1]
	 */
	public static class LdbcShortQuery1PersonProfileHandler implements ShortOperation {
		private static final String statement =
				"   MATCH (n:Person {id:{id}})-[:IS_LOCATED_IN]-(p:Place)"
						+ " RETURN"
						+ "   n.firstName AS firstName,"
						+ "   n.lastName AS lastName,"
						+ "   n.birthday AS birthday,"
						+ "   n.locationIP AS locationIp,"
						+ "   n.browserUsed AS browserUsed,"
						+ "   n.gender AS gender,"
						+ "   n.creationDate AS creationDate,"
						+ "   p.id AS cityId";
		@Override
		public String getMessage(String args[]) {
			String parameters = "{ \"id\" : \"" + args[0] + "\" }";
			return getString (statement, parameters);
		}

		@Override
		public String[] getNewArguments(ShortQueryParameterProvider personIds,
				ShortQueryParameterProvider commentIds,
				ShortQueryParameterProvider postIds) {
			return new String[]{personIds.getNewArguments()};
		}
	}

	/**
	 * Given a start Person, retrieve the last 10 Messages (Posts or Comments)
	 * created by that user. For each message, return that message, the original
	 * post in its conversation, and the author of that post. If any of the
	 * Messages is a Post, then the original Post will be the same Message, i.e.,
	 * that Message will appear twice in that result. Order results descending by
	 * message creation date, then descending by message identifier.[1]
	 */
	public static class LdbcShortQuery2PersonPostsHandler implements ShortOperation {
		private static final String statement =
				"   MATCH (:Person {id:{id}})<-[:HAS_CREATOR]-(m)-[:REPLY_OF*0..]->(p:Post)"
						+ " MATCH (p)-[:HAS_CREATOR]->(c)"
						+ " RETURN"
						+ "   m.id as messageId,"
						+ "   CASE has(m.content)"
						+ "     WHEN true THEN m.content"
						+ "     ELSE m.imageFile"
						+ "   END AS messageContent,"
						+ "   m.creationDate AS messageCreationDate,"
						+ "   p.id AS originalPostId,"
						+ "   c.id AS originalPostAuthorId,"
						+ "   c.firstName as originalPostAuthorFirstName,"
						+ "   c.lastName as originalPostAuthorLastName"
						+ " ORDER BY messageCreationDate DESC"
						+ " LIMIT {limit}";
		@Override
		public String getMessage(String args[]) {
			String parameters = "{ \"id\" : \"" + args[0] + "\", "
					+ "\"limit\" : " + operationLimit + " }";
			return getString (statement, parameters);
		}

		@Override
		public String[] getNewArguments(ShortQueryParameterProvider personIds,
				ShortQueryParameterProvider commentIds,
				ShortQueryParameterProvider postIds) {
			return new String[]{personIds.getNewArguments()};
		}
	}

	/**
	 * Given a start Person, retrieve all of their friends, and the date at which
	 * they became friends. Order results descending by friendship creation date,
	 * then ascending by friend identifier.[1]
	 */
	public static class LdbcShortQuery3PersonFriendsHandler implements ShortOperation {
		private static final String statement =
				"   MATCH (n:Person {id:{id}})-[r:KNOWS]-(friend)"
						+ " RETURN"
						+ "   friend.id AS personId,"
						+ "   friend.firstName AS firstName,"
						+ "   friend.lastName AS lastName,"
						+ "   r.creationDate AS friendshipCreationDate"
						+ "   ORDER BY friendshipCreationDate DESC, toInt(personId) ASC";
		@Override
		public String getMessage(String args[]) {
			String parameters = "{ \"id\" : \"" + args[0] + "\" }";
			return getString (statement, parameters);
		}

		@Override
		public String[] getNewArguments(ShortQueryParameterProvider personIds,
				ShortQueryParameterProvider commentIds,
				ShortQueryParameterProvider postIds) {
			return new String[]{personIds.getNewArguments()};
		}
	}

	/**
	 * Given a Message (Comment), retrieve its content and creation
	 * date.[1]
	 */
	public static class LdbcShortQuery4MessageContentHandlerComment implements ShortOperation {
		private static final String statement =
				"   MATCH (m:Message {id:{id}})"
						+ " RETURN"
						+ "   CASE has(m.content)"
						+ "     WHEN true THEN m.content"
						+ "     ELSE m.imageFile"
						+ "   END AS messageContent,"
						+ "   m.creationDate as messageCreationDate";
		@Override
		public String getMessage (String args[]) {
			String parameters = "{ \"id\" : \"" + args[0] + "\" }";
			return getString(statement, parameters);
		}

		@Override
		public String[] getNewArguments(ShortQueryParameterProvider personIds,
				ShortQueryParameterProvider commentIds,
				ShortQueryParameterProvider postIds) {
			return new String[]{commentIds.getNewArguments()};
		}
	}

	/**
	 * Given a Message (Post), retrieve its content and creation
	 * date.[1]
	 */
	public static class LdbcShortQuery4MessageContentHandlerPost implements ShortOperation {
		private static final String statement =
				"   MATCH (m:Message {id:{id}})"
						+ " RETURN"
						+ "   CASE has(m.content)"
						+ "     WHEN true THEN m.content"
						+ "     ELSE m.imageFile"
						+ "   END AS messageContent,"
						+ "   m.creationDate as messageCreationDate";
		@Override
		public String getMessage (String args[]) {
			String parameters = "{ \"id\" : \"" + args[0] + "\" }";
			return getString(statement, parameters);
		}

		@Override
		public String[] getNewArguments(ShortQueryParameterProvider personIds,
				ShortQueryParameterProvider commentIds,
				ShortQueryParameterProvider postIds) {
			return new String[]{postIds.getNewArguments()};
		}
	}


	/**
	 * Given a Message (Comment), retrieve its author.[1]
	 */
	public static class LdbcShortQuery5MessageCreatorHandlerComment implements ShortOperation {
		private static final String statement =
				"   MATCH (m:Message {id:{id}})-[:HAS_CREATOR]->(p:Person)"
						+ " RETURN"
						+ "   p.id AS personId,"
						+ "   p.firstName AS firstName,"
						+ "   p.lastName AS lastName";
		@Override
		public String getMessage (String args[]) {
			String parameters = "{ \"id\" : \"" + args[0] + "\" }";
			return getString(statement, parameters);
		}

		@Override
		public String[] getNewArguments(ShortQueryParameterProvider personIds,
				ShortQueryParameterProvider commentIds,
				ShortQueryParameterProvider postIds) {
			return new String[]{commentIds.getNewArguments()};
		}
	}

	/**
	 * Given a Message (Post), retrieve its author.[1]
	 */
	public static class LdbcShortQuery5MessageCreatorHandlerPost implements ShortOperation {
		private static final String statement =
				"   MATCH (m:Message {id:{id}})-[:HAS_CREATOR]->(p:Person)"
						+ " RETURN"
						+ "   p.id AS personId,"
						+ "   p.firstName AS firstName,"
						+ "   p.lastName AS lastName";
		@Override
		public String getMessage (String args[]) {
			String parameters = "{ \"id\" : \"" + args[0] + "\" }";
			return getString(statement, parameters);
		}

		@Override
		public String[] getNewArguments(ShortQueryParameterProvider personIds,
				ShortQueryParameterProvider commentIds,
				ShortQueryParameterProvider postIds) {
			return new String[]{postIds.getNewArguments()};
		}
	}

	/**
	 * Given a Message (Comment), retrieve the Forum that contains it and
	 * the Person that moderates that forum. Since comments are not directly
	 * contained in forums, for comments, return the forum containing the
	 * original post in the thread which the comment is replying to.[1]
	 */
	public static class LdbcShortQuery6MessageForumHandlerComment implements ShortOperation {
		private static final String statement =
				"   MATCH (m:Message {id:{id}})-[:REPLY_OF*0..]->(p:Post)<-[:CONTAINER_OF]-(f:Forum)-[:HAS_MODERATOR]->(mod:Person)"
						+ " RETURN"
						+ "   f.id AS forumId,"
						+ "   f.title AS forumTitle,"
						+ "   mod.id AS moderatorId,"
						+ "   mod.firstName AS moderatorFirstName,"
						+ "   mod.lastName AS moderatorLastName";
		@Override
		public String getMessage (String args[]) {
			String parameters = "{ \"id\" : \"" + args[0] + "\" }";
			return getString(statement, parameters);
		}

		@Override
		public String[] getNewArguments(ShortQueryParameterProvider personIds,
				ShortQueryParameterProvider commentIds,
				ShortQueryParameterProvider postIds) {
			return new String[]{commentIds.getNewArguments()};
		}
	}

	/**
	 * Given a Message (Post), retrieve the Forum that contains it and
	 * the Person that moderates that forum. Since comments are not directly
	 * contained in forums, for comments, return the forum containing the
	 * original post in the thread which the comment is replying to.[1]
	 */
	public static class LdbcShortQuery6MessageForumHandlerPost implements ShortOperation {
		private static final String statement =
				"   MATCH (m:Message {id:{id}})-[:REPLY_OF*0..]->(p:Post)<-[:CONTAINER_OF]-(f:Forum)-[:HAS_MODERATOR]->(mod:Person)"
						+ " RETURN"
						+ "   f.id AS forumId,"
						+ "   f.title AS forumTitle,"
						+ "   mod.id AS moderatorId,"
						+ "   mod.firstName AS moderatorFirstName,"
						+ "   mod.lastName AS moderatorLastName";
		@Override
		public String getMessage (String args[]) {
			String parameters = "{ \"id\" : \"" + args[0] + "\" }";
			return getString(statement, parameters);
		}

		@Override
		public String[] getNewArguments(ShortQueryParameterProvider personIds,
				ShortQueryParameterProvider commentIds,
				ShortQueryParameterProvider postIds) {
			return new String[]{postIds.getNewArguments()};
		}
	}

	/**
	 * Given a Message (Comment), retrieve the (1-hop) Comments that
	 * reply to it. In addition, return a boolean flag indicating if the author
	 * of the reply knows the author of the original message. If author is same
	 * as original author, return false for "knows" flag. Order results
	 * descending by creation date, then ascending by author identifier.[1]
	 */
	public static class LdbcShortQuery7MessageRepliesHandlerComment implements ShortOperation {
		private static final String statement =
				"   MATCH (m:Message {id:{id}})<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(p:Person)"
						+ " OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p)"
						+ " RETURN"
						+ "   c.id AS commentId,"
						+ "   c.content AS commentContent,"
						+ "   c.creationDate AS commentCreationDate,"
						+ "   p.id AS replyAuthorId,"
						+ "   p.firstName AS replyAuthorFirstName,"
						+ "   p.lastName AS replyAuthorLastName,"
						+ "   CASE r"
						+ "     WHEN null THEN false"
						+ "     ELSE true"
						+ "   END AS replyAuthorKnowsOriginalMessageAuthor"
						+ " ORDER BY commentCreationDate DESC, toInt(replyAuthorId) ASC";
		@Override
		public String getMessage (String args[]) {
			String parameters = "{ \"id\" : \"" + args[0] + "\" }";
			return getString(statement, parameters);
		}

		@Override
		public String[] getNewArguments(ShortQueryParameterProvider personIds,
				ShortQueryParameterProvider commentIds,
				ShortQueryParameterProvider postIds) {
			return new String[]{commentIds.getNewArguments()};
		}
	}

	/**
	 * Given a Message (Post), retrieve the (1-hop) Comments that
	 * reply to it. In addition, return a boolean flag indicating if the author
	 * of the reply knows the author of the original message. If author is same
	 * as original author, return false for "knows" flag. Order results
	 * descending by creation date, then ascending by author identifier.[1]
	 */
	public static class LdbcShortQuery7MessageRepliesHandlerPost implements ShortOperation {
		private static final String statement =
				"   MATCH (m:Message {id:{id}})<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(p:Person)"
						+ " OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p)"
						+ " RETURN"
						+ "   c.id AS commentId,"
						+ "   c.content AS commentContent,"
						+ "   c.creationDate AS commentCreationDate,"
						+ "   p.id AS replyAuthorId,"
						+ "   p.firstName AS replyAuthorFirstName,"
						+ "   p.lastName AS replyAuthorLastName,"
						+ "   CASE r"
						+ "     WHEN null THEN false"
						+ "     ELSE true"
						+ "   END AS replyAuthorKnowsOriginalMessageAuthor"
						+ " ORDER BY commentCreationDate DESC, toInt(replyAuthorId) ASC";
		@Override
		public String getMessage (String args[]) {
			String parameters = "{ \"id\" : \"" + args[0] + "\" }";
			return getString(statement, parameters);
		}

		@Override
		public String[] getNewArguments(ShortQueryParameterProvider personIds,
				ShortQueryParameterProvider commentIds,
				ShortQueryParameterProvider postIds) {
			return new String[]{postIds.getNewArguments()};
		}
	}

	/**
	 * ------------------------------------------------------------------------
	 * Update Queries
	 * ------------------------------------------------------------------------
	 */

	/**
	 * Add a Person to the social network. [1]
	 * <p>
	 * TODO:
	 * <ul>
	 * <li>This query involves creating many relationships of different types.
	 * This is currently done using multiple cypher queries, but it may be
	 * possible to combine them in some way to amortize per query overhead and
	 * thus increase performance.</li>
	 * </ul>
	 */
	public static class LdbcUpdate1AddPersonHandler implements UpdateOperation {
		private final static String statement1 =
				"   CREATE (p:Person {props})";
        private final static String statement2 =
                "   MATCH (p:Person {id:{personId}}),"
                + "       (c:Place {id:{cityId}})"
                + " OPTIONAL MATCH (t:Tag)"
                + " WHERE t.id IN {tagIds}"
                + " WITH p, c, collect(t) AS tagSet"
                + " CREATE (p)-[:IS_LOCATED_IN]->(c)"
                + " FOREACH(t IN tagSet| CREATE (p)-[:HAS_INTEREST]->(t))";
		public final static String[] properties = new String[]{
				"personId", "firstName", "lastName", "gender",
				"birthday", "creationDate", "locationIP", "browserUsed", "cityId",
				"speaks", "emails", "tagIds", "studyAt", "workAt"};

		@Override
		public String getMessage (Object[] args) {
            String[] statements = new String[4];
            String[] parameters = new String[4];
			String parameters1 = "{ \"props\" : {"
					+ " \"id\" : \"" + args[0] + "\","
					+ " \"firstName\" : \"" + args[1] + "\","
					+ " \"lastName\" : \"" + args[2] + "\","
					+ " \"gender\" : \"" + args[3] + "\","
					+ " \"birthday\" : " + ((Date)args[4]).getTime() + ","
					+ " \"creationDate\" : " + ((Date)args[5]).getTime() + ","
					+ " \"locationIP\" : \"" + args[6] + "\","
					+ " \"browserUsed\" : \"" + args[7] + "\","
					+ " \"speaks\" : "
					+ DbHelper.listToJsonArray((Object[])args[9]) + ","
					+ " \"emails\" : "
					+ DbHelper.listToJsonArray((Object[])args[10])
					+ " } }";
            statements[0] = statement1;
            parameters[0] = parameters1;
            String parameters2 = "{ "
                    + " \"personId\" : \"" + args[0] + "\","
                    + " \"cityId\" : \"" + args[8] + "\","
                    + " \"tagIds\" : " + DbHelper.listToJsonArray((Object[])args[11])
                    + " }";
            statements[1] = statement2;
            parameters[1] = parameters2;
            Object[] studiesAt = ((Object[])getIfExists(args, 12));
            if (studiesAt!=null && studiesAt.length > 0) { // Studies at
                StringBuilder matchBldr = new StringBuilder();
                StringBuilder createBldr = new StringBuilder();
                StringBuilder paramBldr = new StringBuilder();
                matchBldr.append("MATCH (p:Person {id:{personId}}), ");
                createBldr.append("CREATE ");
                paramBldr.append("{\"personId\" : \"" + args[0] + "\", ");
                for (int i = 0; i < studiesAt.length; i++) {
                    Organization org = (Organization)studiesAt[i];
                    if (i > 0) {
                        matchBldr.append(", ");
                        createBldr.append(", ");
                        paramBldr.append(", ");
                    }
                    matchBldr.append(
                            String.format("(u%d:Organisation {id:{uId%d}})", i, i));
                    createBldr.append(
                            String.format("(p)-[:STUDY_AT {classYear:{cY%d}}]->(u%d)", i, i));
                    paramBldr.append(
                            String.format("\"uId%d\" : \"%d\"", i, org.organizationId()));
                    paramBldr.append(", ");
                    paramBldr.append(
                            String.format("\"cY%d\" : %d", i, org.year()));
                }
                paramBldr.append("}");
                statements[2] = matchBldr.toString() + " " + createBldr.toString();
                parameters[2] = paramBldr.toString();
            }
            Object[] worksAt = ((Object[])getIfExists(args, 13));
            if (worksAt!=null && worksAt.length > 0) { // Works at
                StringBuilder matchBldr = new StringBuilder();
                StringBuilder createBldr = new StringBuilder();
                StringBuilder paramBldr = new StringBuilder();

                matchBldr.append("MATCH (p:Person {id:{personId}}), ");
                createBldr.append("CREATE ");
                paramBldr.append("{\"personId\" : \"" + args[0] + "\", ");

                for (int i = 0; i < worksAt.length; i++) {
                    Organization org = (Organization)worksAt[i];
                    if (i > 0) {
                        matchBldr.append(", ");
                        createBldr.append(", ");
                        paramBldr.append(", ");
                    }
                    matchBldr.append(
                            String.format("(c%d:Organisation {id:{cId%d}})", i, i));
                    createBldr.append(
                            String.format("(p)-[:WORK_AT {workFrom:{wF%d}}]->(c%d)", i, i));
                    paramBldr.append(
                            String.format("\"cId%d\" : \"%d\"", i, org.organizationId()));
                    paramBldr.append(", ");
                    paramBldr.append(
                            String.format("\"wF%d\" : %d", i, org.year()));
                }

                paramBldr.append("}");
                statements[3] = matchBldr.toString() + " " + createBldr.toString();
                parameters[3] = paramBldr.toString();
            }
			return getMultipleStatements(statements, parameters);
		}

	}

	/**
	 * Add a Like to a Post of the social network.[1]
	 */
	public static class LdbcUpdate2AddPostLikeHandler implements UpdateOperation {
		public final static String[] properties = new String[]{
				"personId", "postId", "creationDate"};
		private final static String statement =
				"   MATCH (p:Person {id:{personId}}),"
						+ "       (m:Post {id:{postId}})"
						+ " CREATE (p)-[:LIKES {creationDate:{creationDate}}]->(m)";
		@Override
		public String getMessage (Object[] args) {
			String parameters = "{ "
					+ " \"personId\" : \"" + args[0] + "\","
					+ " \"postId\" : \"" + args[1] + "\","
					+ " \"creationDate\" : " + ((Date)args[2]).getTime()
							+ " }";
			return getString(statement, parameters);
		}
	}

	/**
	 * Add a Like to a Comment of the social network.[1]
	 */
	public static class LdbcUpdate3AddCommentLikeHandler implements UpdateOperation {
		public final static String[] properties = new String[]{
                "personId", "commentId", "creationDate"};
		private final static String statement =
				"   MATCH (p:Person {id:{personId}}),"
						+ "       (m:Comment {id:{commentId}})"
						+ " CREATE (p)-[:LIKES {creationDate:{creationDate}}]->(m)";
		@Override
		public String getMessage (Object[] args) {
			String parameters = "{ "
					+ " \"personId\" : \"" + args[0] + "\","
					+ " \"commentId\" : \"" + args[1] + "\","
					+ " \"creationDate\" : " + ((Date)args[2]).getTime()
							+ " }";
			return getString(statement, parameters);
		}
	}

	/**
	 * Add a Forum to the social network.[1]
	 */
	public static class LdbcUpdate4AddForumHandler implements UpdateOperation {
		public final static String[] properties = new String[]{
                "forumId", "forumTitle", "creationDate",
                "moderatorPersonId", "tagIds"};
		private final static String statement =
				"   CREATE (f:Forum {props})";

		@Override
		public String getMessage(Object[] args) {
			String parameters = "{ \"props\" : {"
					+ " \"id\" : \"" + args[0] + "\","
					+ " \"title\" : \"" + args[1] + "\","
					+ " \"creationDate\" : " + ((Date)args[2]).getTime()
							+ " } }";
			return getString(statement, parameters);
		}
	}

	/**
	 * Add a Forum membership to the social network.[1]
	 */
	public static class LdbcUpdate5AddForumMembershipHandler implements UpdateOperation {
		public final static String[] properties = new String[]{
				"forumId", "personId", "joinDate"};
		private final static String statement =
				"   MATCH (f:Forum {id:{forumId}}),"
						+ "       (p:Person {id:{personId}})"
						+ " CREATE (f)-[:HAS_MEMBER {joinDate:{joinDate}}]->(p)";

		@Override
		public String getMessage(Object[] args) {
			String parameters = "{ "
					+ " \"forumId\" : \"" + args[0] + "\","
					+ " \"personId\" : \"" + args[1] + "\","
					+ " \"joinDate\" : " + ((Date)args[2]).getTime()
							+ " }";
			return getString(statement, parameters);
		}
	}

	/**
	 * Add a Post to the social network.[1]
	 */
	public static class LdbcUpdate6AddPostHandler implements UpdateOperation {
		public final static String[] properties = new String[]{
				"postId", "imageFile", "creationDate",
				"locationIP", "browserUsed", "language", "content", "length",
				"authorPersonId", "forumId", "countryId", "tagIds"};
		private static final String statement1 =
				"   CREATE (m:Post:Message {props})",
				statement2 = 
				"   MATCH (m:Post {id:{postId}}),"
						+ "       (p:Person {id:{authorId}}),"
						+ "       (f:Forum {id:{forumId}}),"
						+ "       (c:Place {id:{countryId}})"
						+ " OPTIONAL MATCH (t:Tag)"
						+ " WHERE t.id IN {tagIds}"
						+ " WITH m, p, f, c, collect(t) as tagSet"
						+ " CREATE (m)-[:HAS_CREATOR]->(p),"
						+ "        (m)<-[:CONTAINER_OF]-(f),"
						+ "        (m)-[:IS_LOCATED_IN]->(c)"
						+ " FOREACH (t IN tagSet| CREATE (m)-[:HAS_TAG]->(t))";

		@Override
		public String getMessage(Object[] args) {
			String parameters1;
			if (((String)args[1]).length() > 0) {
				parameters1 = "{ \"props\" : {"
						+ " \"id\" : \"" + args[0] + "\","
						+ " \"imageFile\" : \"" + args[1] + "\","
						+ " \"creationDate\" : " + ((Date)args[2]).getTime() + ","
						+ " \"locationIP\" : \"" + args[3] + "\","
						+ " \"browserUsed\" : \"" + args[4] + "\","
						+ " \"language\" : \"" + args[5] + "\","
						+ " \"length\" : " + args[7]
								+ " } }";
			} else {
				parameters1 = "{ \"props\" : {"
						+ " \"id\" : \"" + args[0] + "\","
						+ " \"creationDate\" : " + ((Date)args[2]).getTime() + ","
						+ " \"locationIP\" : \"" + args[3] + "\","
						+ " \"browserUsed\" : \"" + args[4] + "\","
						+ " \"language\" : \"" + args[5] + "\","
						+ " \"content\" : \"" + args[6] + "\","
						+ " \"length\" : " + args[7]
								+ " } }";
			}
			String parameters2 = "{ "
					+ " \"postId\" : \"" + args[0] + "\","
					+ " \"authorId\" : \"" + args[8] + "\","
					+ " \"forumId\" : \"" + args[9] + "\","
					+ " \"countryId\" : \"" + args[10] + "\","
					+ " \"tagIds\" : " + DbHelper.listToJsonArray((Object[])getIfExists(args, 11))
					+ " }";
			// TODO get the correct arguments
			return getMultipleStatements(statement1, parameters1, statement2, parameters2);
		}
	}

	/**
	 * Add a Comment replying to a Post/Comment to the social network.[1]
	 */
	public static class LdbcUpdate7AddCommentHandler implements UpdateOperation {
		public final static String[] properties = new String[]{
				"commentId", "creationDate", "locationIP",
				"browserUsed", "content", "length", "authorPersonId", "countryId",
				"replyToPostId", "replyToCommentId", "tagIds"};
		private static final String statement1 =
				"   CREATE (c:Comment:Message {props})", 
				statement2 =
				"   MATCH (m:Comment {id:{commentId}}),"
						+ "       (p:Person {id:{authorId}}),"
						+ "       (r:Message {id:{replyOfId}}),"
						+ "       (c:Place {id:{countryId}})"
						+ " OPTIONAL MATCH (t:Tag)"
						+ " WHERE t.id IN {tagIds}"
						+ " WITH m, p, r, c, collect(t) as tagSet"
						+ " CREATE (m)-[:HAS_CREATOR]->(p),"
						+ "        (m)-[:REPLY_OF]->(r),"
						+ "        (m)-[:IS_LOCATED_IN]->(c)"
						+ " FOREACH (t IN tagSet| CREATE (m)-[:HAS_TAG]->(t))";

		// Add hasCreator, containerOf, isLocatedIn, and hasTag relationships.
		@Override
		public String getMessage(Object[] args) {
			String parameters1 = "{ \"props\" : {"
					+ " \"id\" : \"" + args[0] + "\","
					+ " \"creationDate\" : " + ((Date)args[1]).getTime() + ","
					+ " \"locationIP\" : \"" + args[2] + "\","
					+ " \"browserUsed\" : \"" + args[3] + "\","
					+ " \"content\" : \"" + args[4] + "\","
					+ " \"length\" : " + args[5]
							+ " } }";

            Long replyOfId = (Long)args[8];
			if (replyOfId != -1) {
			    replyOfId = (Long)args[9];
			}
			String parameters2 = "{ "
					+ " \"commentId\" : \"" + args[0] + "\","
					+ " \"authorId\" : \"" + args[6] + "\","
					+ " \"replyOfId\" : \"" + replyOfId + "\","
					+ " \"countryId\" : \"" + args[7] + "\","
					+ " \"tagIds\" : " + DbHelper.listToJsonArray((Object[])getIfExists(args, 10))
					+ " }";
			return getMultipleStatements(statement1, parameters1, statement2, parameters2);
		}
	}

	/**
	 * Add a friendship relation to the social network.[1]
	 */
	public static class LdbcUpdate8AddFriendshipHandler implements UpdateOperation {
		public final static String[] properties = new String[]{
				"person1Id", "person2Id", "creationDate"};
		private static final String statement =
				"   MATCH (p1:Person {id:{person1Id}}),"
						+ "       (p2:Person {id:{person2Id}})"
						+ " CREATE (p1)-[:KNOWS {creationDate:{creationDate}}]->(p2)";

		@Override
		public String getMessage(Object[] args) {
			String parameters = "{ "
					+ " \"person1Id\" : \"" + args[0] + "\","
					+ " \"person2Id\" : \"" + args[1] + "\","
					+ " \"creationDate\" : " + ((Date)args[2]).getTime()
							+ " }";
			return getString(statement, parameters);
		}
	}

    public static Object getIfExists (Object[] ob, int pos) {
        if (ob.length > pos) {
            return ob[pos];
        } else {
            return new Object[]{};
        }
    }
}
