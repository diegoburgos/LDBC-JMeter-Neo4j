#!/bin/bash

URL="blade67"
PORT="8080"
HOME_FOLDER="/ssd/leandata/neo4j/"
ROOT_RESULT_FOLDER="${HOME_FOLDER}/results/${1}"
#NEO4J_HOME="${HOME_FOLDER}/dataset/factor1/social_network_3/neo4j-enterprise-2.2.0/"
NEO4J_HOME="${HOME_FOLDER}/dataset/factor3/social_network_1/neo4j-enterprise-2.2.0/"
#NEO4J_HOME="${HOME_FOLDER}/dataset/factor10/social_network_1/neo4j-enterprise-2.2.0/"

SOCIAL_NETWORK="${HOME_FOLDER}/jmeter/dataset/factor3/social_network_1/"

SUBSTITUTION_PARAMETERS="${SOCIAL_NETWORK}/substitution_parameters/"
COMMENT_CSV="${SOCIAL_NETWORK}/social_network/comment_0_0.csv"
POST_CSV="${SOCIAL_NETWORK}/social_network/post_0_0.csv"
PERSON_CSV="${SOCIAL_NETWORK}/social_network/person_0_0.csv"
UPDATE_PERSON_CSV="${SOCIAL_NETWORK}/social_network/updateStream_0_0_person.csv"
UPDATE_FORUM_CSV="${SOCIAL_NETWORK}/social_network/updateStream_0_0_forum.csv"

PARAMETER_FILES="$SUBSTITUTION_PARAMETERS $COMMENT_CSV $POST_CSV $PERSON_CSV $UPDATE_PERSON_CSV $UPDATE_FORUM_CSV"

EX_TIME="1920" # 30 + 2 mins

#for wl in 25,75 50,50 75,25 90,10
for wl in 90,10
do
    reads=$(echo $wl | cut -f1 -d,)
    writes=$(echo $wl | cut -f2 -d,)
    echo ${workload[0]}
    for th in 50 100 150 80 120 200 30 10
    do
        for cl in 24 12 8 4 2 1 36 48
        do
            echo "Workload <$reads, $writes> -- $cl clients -- $th throughput"
            RESULTS_FOLDER="$ROOT_RESULT_FOLDER/${reads}reads${writes}writes/${cl}clients/${th}target/"
            mkdir -p $RESULTS_FOLDER
            ${HOME_FOLDER}/jmeter/LDBC-JMeter-Neo4j/runJMeter.sh $NEO4J_HOME $cl $EX_TIME $URL $PORT $RESULTS_FOLDER $PARAMETER_FILES $reads $writes $th
        done
    done
done

# neo4j_home nusers exTime url port result
# substitution_parameters comment_csv post_csv person_csv update_person_csv update_forum_csv
# pReads pUpdates
