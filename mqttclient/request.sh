#!/bin/bash
TEST_ID_PREFIX=$1
REQ_TOPIC=$2
RESP_TOPIC=$3
DATA=$4
NUM_REQ=$5
# exec 6>&1
# exec > /dev/null

for i in `seq 1 1 $NUM_REQ`
do
	echo "Staring $i"
./target/debug/mqtt-client \
--client-id ${TEST_ID_PREFIX}-$i \
--host 127.0.0.1:1883 \
request  \
--pub-topic $REQ_TOPIC \
--resp-topic $RESP_TOPIC \
--corr-data ${TEST_ID_PREFIX}-$i \
--msg $DATA &
done
wait
