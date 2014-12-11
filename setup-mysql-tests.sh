#!/bin/bash -x
wait_for_line () {
    while read line
    do
        echo "$line" | grep -q "$1" && break
    done < "$2"
    # Read the fifo for ever otherwise process would block
    cat "$2" >/dev/null &
}

# Start MySQL process for tests
MYSQL_DATA=`mktemp -d /tmp/gnocchi-mysql-XXXXX`
mkfifo ${MYSQL_DATA}/out
PATH=$PATH:/usr/libexec
mysqld --datadir=${MYSQL_DATA} --pid-file=${MYSQL_DATA}/mysql.pid --socket=${MYSQL_DATA}/mysql.socket --skip-networking --skip-grant-tables &> ${MYSQL_DATA}/out &
# Wait for MySQL to start listening to connections
wait_for_line "mysqld: ready for connections." ${MYSQL_DATA}/out
export GNOCCHI_TEST_MYSQL_URL="mysql://root@localhost/test?unix_socket=${MYSQL_DATA}/mysql.socket&charset=utf8"
mysql -S ${MYSQL_DATA}/mysql.socket -e 'CREATE DATABASE test;'

$*

ret=$?
kill $(jobs -p)
rm -rf "${MYSQL_DATA}"
exit $ret
