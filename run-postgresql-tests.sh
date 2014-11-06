#!/bin/bash -x

# Start PostgreSQL process for tests
PGSQL_DATA=`mktemp -d /tmp/gnocchi-psql-XXXXX`
PGSQL_PATH=`pg_config --bindir`
PGSQL_PORT=9823
${PGSQL_PATH}/initdb ${PGSQL_DATA}
LANGUAGE=C ${PGSQL_PATH}/pg_ctl -w -D ${PGSQL_DATA} -o "-k ${PGSQL_DATA} -p ${PGSQL_PORT}" start
export GNOCCHI_TEST_PGSQL_URL="postgresql:///?host=${PGSQL_DATA}&port=${PGSQL_PORT}&dbname=template1"

python setup.py testr --slowest --testr-args="$*"

ret=$?
${PGSQL_PATH}/pg_ctl -w -D ${PGSQL_DATA} -o "-p $PGSQL_PORT" stop
rm -rf ${PGSQL_DATA}
exit $ret
