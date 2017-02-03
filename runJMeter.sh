#!/usr/bin/env bash

UI="-n"

if [ "$#" -ne 15 ]; then
    echo "Usage: $0 neo4j_home nusers exTime url port result substitution_parameters
            comment_csv post_csv person_csv update_person_csv update_forum_csv pReads pUpdates target"
    echo "Arguments used: $@"
    exit 1
fi

#JMETER_HOME="/home/dburgos/neo4j-versioning-2y/benchmarks/jmeter/software/apache-jmeter-3.1/"
JMETER_HOME="/ssd/dburgos/jmeter/apache-jmeter-3.1"
JMETER=$JMETER_HOME/bin/jmeter
#JMETER_TEST_PLAN="/home/dburgos/neo4j-versioning-2y/benchmarks/jmeter/Testing.jmx"
JMETER_TEST_PLAN="/ssd/dburgos/jmeter/LDBC-JMeter-Neo4j/Testing.jmx"
SERVER=blade131


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
URL=$4
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
ARGS="$ARGS -J${URL_ARG}=$URL"
ARGS="$ARGS -J${PORT_ARG}=$PORT"
ARGS="$ARGS -J${RESULT_ARGS}=$RESULTS/results.csv"
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


monitor () {
    MONITORING_INTERVAL=5
    echo "Monitoring $1"
    if [ $1 == "localhost" ]; then
        vmstat $MONITORING_INTERVAL > ${2}/${1}-vmstat.txt &
        # iostat -x $MONITORING_INTERVAL > ${2}/${1}-iostat.txt &
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
sleep 10
monitor localhost $RESULTS
monitor $SERVER $RESULTS

$JMETER $UI -t $JMETER_TEST_PLAN $ARGS

stop_monitoring localhost
stop_monitoring $SERVER
stopDatabase
sleep 10

scp $SERVER:$RESULTS/*txt $RESULTS/
