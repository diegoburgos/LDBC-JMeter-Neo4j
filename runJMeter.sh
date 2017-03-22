#!/bin/bash

UI="-n"

if [ "$#" -ne 15 ]; then
    echo "Usage: $0 neo4j_home nusers exTime server port result substitution_parameters
            comment_csv post_csv person_csv update_person_csv update_forum_csv pReads pUpdates target"
    echo "Arguments used: $@"
    exit 1
fi

HOME_FOLDER="/ssd/leandata/neo4j/"
JMETER_HOME="${HOME_FOLDER}/jmeter/apache-jmeter-3.1"
JMETER=$JMETER_HOME/bin/jmeter
JMETER_TEST_PLAN="${HOME_FOLDER}/jmeter/LDBC-JMeter-Neo4j/Testing.jmx"


NUSERS_ARG="nusers"
EX_TIME_ARG="exTime"
URL_ARG="url"
PORT_ARG="port"
RESULT_ARGS="result"
SUBSTITUTION_PARAMETERS_ARG="substitution_parameters"
COMMENT_CSV_ARG="comment_csv"
POST_CSV_ARG="post_csv"
PERSON_CSV_ARG="person_csv"
UPDATE_PERSON_CSV_ARG="update_person_csv"
UPDATE_FORUM_CSV_ARG="update_forum_csv"
P_READS_ARG="preads"
P_UPDATES_ARG="pupdates"
TARGET_ARG="target"

NEO4J_HOME=$1
NUSERS=$2
EX_TIME=$3
SERVER=$4
PORT=$5
RESULTS=$6
SUBSTITUTION_PARAMETERS=$7
COMMENT_CSV=$8
POST_CSV=$9
PERSON_CSV=${10}
UPDATE_PERSON_CSV=${11}
UPDATE_FORUM_CSV=${12}
P_READS=${13}
P_UPDATES=${14}
TARGET=${15}

ARGS="-J${EX_TIME_ARG}=$EX_TIME"
ARGS="$ARGS -J${NUSERS_ARG}=$NUSERS"
ARGS="$ARGS -J${URL_ARG}=$SERVER"
ARGS="$ARGS -J${PORT_ARG}=$PORT"
ARGS="$ARGS -J${RESULT_ARGS}=$RESULTS"
ARGS="$ARGS -J${SUBSTITUTION_PARAMETERS_ARG}=$SUBSTITUTION_PARAMETERS"
ARGS="$ARGS -J${COMMENT_CSV_ARG}=$COMMENT_CSV"
ARGS="$ARGS -J${POST_CSV_ARG}=$POST_CSV"
ARGS="$ARGS -J${PERSON_CSV_ARG}=$PERSON_CSV"
ARGS="$ARGS -J${UPDATE_PERSON_CSV_ARG}=$UPDATE_PERSON_CSV"
ARGS="$ARGS -J${UPDATE_FORUM_CSV_ARG}=$UPDATE_FORUM_CSV"
ARGS="$ARGS -J${P_READS_ARG}=$P_READS"
ARGS="$ARGS -J${P_UPDATES_ARG}=$P_UPDATES"
ARGS="$ARGS -J${TARGET_ARG}=$TARGET"

function runDatabase {
    ssh $SERVER "$NEO4J_HOME/bin/neo4j start"
}

function stopDatabase {
    ssh $SERVER "$NEO4J_HOME/bin/neo4j stop"
}

function restoreDBStorage {
    echo "Restoring OR Neo4j DB ${NEO4J_HOME}/data/graph.db using ${NEO4J_HOME}/data/graph.db_index.tar.gz"
    ssh $SERVER "rm -rf ${NEO4J_HOME}/data/graph.db && tar -zxf ${NEO4J_HOME}/data/graph.db_index.tar.gz -C ${NEO4J_HOME}/data/"
    #echo "Restoring -SI- Neo4j DB ${NEO4J_HOME}/data/graph.db using ${NEO4J_HOME}/data/graph.db_v_index.tar.gz"
    #ssh $SERVER "rm -rf ${NEO4J_HOME}/data/graph.db_v && tar -zxf ${NEO4J_HOME}/data/graph.db_v_index.tar.gz -C ${NEO4J_HOME}/data/"
}

monitor () {
    MONITORING_INTERVAL=5
    echo "Monitoring $1"
    if [ $1 == "localhost" ]; then
        vmstat $MONITORING_INTERVAL > ${2}/${1}-vmstat.txt &
        iostat -x $MONITORING_INTERVAL > ${2}/${1}-iostat.txt &
    else
       ssh $1 "mkdir -p $2"
       ssh $1 "vmstat $MONITORING_INTERVAL > ${2}/${1}-vmstat.txt &"
       ssh $1 "iostat -x $MONITORING_INTERVAL > ${2}/${1}-iostat.txt &"
    fi
}

stop_monitoring () {
    if [ $1 == "localhost" ]; then
        killall vmstat iostat
    else
        ssh $1 killall vmstat iostat
    fi
    echo "Stopped monitoring $1"
}

runDatabase
echo "sleeping 10"
sleep 10
monitor localhost $RESULTS
monitor $SERVER $RESULTS

echo "$JMETER $UI -t $JMETER_TEST_PLAN $ARGS"
$JMETER $UI -t $JMETER_TEST_PLAN $ARGS

stop_monitoring localhost
stop_monitoring $SERVER
stopDatabase
scp $SERVER:$RESULTS/*txt $RESULTS/
mv jmeter.log $RESULTS/
echo "Sleeping 10"
sleep 10
restoreDBStorage
