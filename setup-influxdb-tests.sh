#!/bin/bash -x

wait_for_line () {
    while read line
    do
        echo "$line" | grep -q "$1" && break
    done < "$2"
    # Read the fifo for ever otherwise process would block
    cat "$2" >/dev/null &
}

INFLUXDB_DATA=`mktemp -d /tmp/gnocchi-influxdb-XXXXX`
export GNOCCHI_TEST_INFLUXDB_PORT=51234

mkdir ${INFLUXDB_DATA}/{broker,data,meta,hh,wal}
mkfifo ${INFLUXDB_DATA}/out

cat > $INFLUXDB_DATA/config <<EOF
[meta]
   dir = "${INFLUXDB_DATA}/meta"
   bind-address = ":51233"
[admin]
  enabled = false
[data]
  dir = "${INFLUXDB_DATA}/data"
  wal-dir = "${INFLUXDB_DATA}/wal"
[http]
  bind-address  = ":$GNOCCHI_TEST_INFLUXDB_PORT"
[hinted-handoff]
  dir = "${INFLUXDB_DATA}/hh"
[retention]
  enabled = true
EOF

# Influxdb distributed rpms and debs install to opt by default
PATH=$PATH:/opt/influxdb influxd -config $INFLUXDB_DATA/config > ${INFLUXDB_DATA}/out 2>&1 &
# Wait for InfluxDB to start listening to connections
wait_for_line "Listening on HTTP" ${INFLUXDB_DATA}/out

$*

ret=$?
kill $(jobs -p)
rm -rf "${INFLUXDB_DATA}"
exit $ret
