#!/bin/bash -x
wait_for_line () {
    while read line
    do
        echo "$line" | grep -q "$1" && break
    done < "$2"
    # Read the fifo for ever otherwise process would block
    cat "$2" >/dev/null &
}

# Start PostgreSQL process for tests
PGSQL_DATA=`mktemp -d /tmp/gnocchi-psql-XXXXX`
PGSQL_PATH=`pg_config --bindir`
${PGSQL_PATH}/initdb ${PGSQL_DATA}
mkfifo ${PGSQL_DATA}/out
${PGSQL_PATH}/postgres -N 100 -F -k ${PGSQL_DATA} -D ${PGSQL_DATA} -p 9823 &> ${PGSQL_DATA}/out &
# Wait for PostgreSQL to start listening to connections
wait_for_line "database system is ready to accept connections" ${PGSQL_DATA}/out
export GNOCCHI_TEST_PGSQL_URL="postgresql:///?host=${PGSQL_DATA}&port=9823&dbname=template1"

python setup.py testr --slowest --testr-args="$*"

ret=$?
kill $(jobs -p)
rm -rf "${PGSQL_DATA}"
exit $ret
