#!/bin/bash -x

# Start PostgreSQL process for tests
PGSQL_DATA=`mktemp -d /tmp/gnocchi-psql-XXXXX`
PGSQL_PATH=`pg_config --bindir`
PGSQL_PORT=9823
${PGSQL_PATH}/initdb ${PGSQL_DATA}
LANGUAGE=C ${PGSQL_PATH}/pg_ctl -w -D ${PGSQL_DATA} -o "-k ${PGSQL_DATA} -p ${PGSQL_PORT}" start > /dev/null
export GNOCCHI_TEST_INDEXER_URL="postgresql:///template1?host=${PGSQL_DATA}&port=${PGSQL_PORT}"

mkdir $PGSQL_DATA/tooz
export GNOCCHI_COORDINATION_URL="file:///$PGSQL_DATA/tooz"

$*

ret=$?
${PGSQL_PATH}/pg_ctl -w -D ${PGSQL_DATA} -o "-p $PGSQL_PORT" stop
rm -rf ${PGSQL_DATA}
exit $ret
