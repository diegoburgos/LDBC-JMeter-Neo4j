#!/usr/bin/env bash

EX_TIME="1920" # 30 + 2 mins
URL="blade131"
PORT="8080"
ROOT_RESULT_FOLDER="results/${1}"
NEO4J_HOME="neo4jHome"

SUBSTITUTION_PARAMETERS="/local/dburgos/benchmarks/benchmark-ldbc/dataset/factor1/social_network_3/substitution_parameters/"
COMMENT_CSV="/local/dburgos/benchmarks/benchmark-ldbc/dataset/factor1/social_network_3/social_network/comment_0_0.csv"
POST_CSV="/local/dburgos/benchmarks/benchmark-ldbc/dataset/factor1/social_network_3/social_network/post_0_0.csv"
PERSON_CSV="/local/dburgos/benchmarks/benchmark-ldbc/dataset/factor1/cd social_network_3/social_network/person_0_0.csv"
UPDATE_PERSON_CSV="/local/dburgos/benchmarks/benchmark-ldbc/dataset/factor1/social_network_3/social_network/updateStream_0_0_person.csv"
UPDATE_FORUM_CSV="/local/dburgos/benchmarks/benchmark-ldbc/dataset/factor1/social_network_3/social_network/updateStream_0_0_forum.csv"
PARAMETER_FILES="$SUBSTITUTION_PARAMETERS $COMMENT_CSV $POST_CSV $PERSON_CSV $UPDATE_PERSON_CSV $UPDATE_FORUM_CSV"

for wl in 25,75 50,50 75,25 90,10
do
    reads=$(echo $wl | cut -f1 -d,)
    writes=$(echo $wl | cut -f2 -d,)
    echo ${workload[0]}
    for cl in 1 2 4 6 8 10 12 24 36 48
    do
        for th in 100 200 300 400 500 600
        do
            echo "Workload <$reads, $writes> -- $cl clients -- $th throughput"
            RESULTS_FOLDER="$ROOT_RESULT_FOLDER/${reads}reads${writes}writes/${cl}clients/"
            mkdir -p $RESULTS_FOLDER
            ./runJMeter.sh $NEO4J_HOME $cl $EX_TIME $URL $PORT $RESULTS_FOLDER $PARAMETER_FILES $reads $writes $th
        done
    done
done

# neo4j_home nusers exTime url port result
# substitution_parameters comment_csv post_csv person_csv update_person_csv update_forum_csv
# pReads pUpdates