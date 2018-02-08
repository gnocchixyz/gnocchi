#!/bin/bash

cleanup(){
    type -t indexer_stop >/dev/null && indexer_stop || true
    type -t incoming_stop >/dev/null && incoming_stop || true
}
trap cleanup EXIT

check_empty_var() {
    local x=$(eval echo `echo \\$${1}`)
    if [ -z "$x" ]; then
        echo "Variable \$${1} is unset"
        exit 15
    fi
}


incoming_driver=${1:-redis}
storage_driver=${2:-redis}
indexer="postgresql"
export MICRO_METRICD_NODES=${3:-1}

LOG="perf/${incoming_driver}-${storage_driver}.log"

src=$(dirname $(dirname $(readlink -f $0)))
cd $src

venv=".tox/py27-$indexer-$storage_driver"
if [ ! -d "$venv" ]; then
    tox -epy27-$indexer-$storage_driver --notest
fi
source $venv/bin/activate

data="perf/data-$driver"
rm -rf ${data}
mkdir -p $data

if [ "$incoming_driver" == "redis" ]; then
    echo "Start redis..."
    eval $(pifpaf -e INCOMING run redis)
    check_empty_var INCOMING_URL
fi

echo "Start $indexer..."
eval $(pifpaf -e INDEXER run $indexer)
check_empty_var INDEXER_URL

echo "Generate configuration..."
cat > $data/gnocchi.conf <<EOF
[DEFAULT]
# debug = True
verbose = True
[incoming]
driver = $incoming_driver
redis_url = $INCOMING_URL
file_basepath = ${data}/incoming
[storage]
driver = $storage_driver
file_basepath = ${data}/storage
rocksdb_path = ${data}/rocksdb
rocksdb_readonly = False
rocksdb_writer_socket = ${data}/writer.sock
# [metricd]
# metric_processing_delay = 1
# metric_cleanup_delay = 1
[indexer]
url = $INDEXER_URL
EOF

echo "Run gnocchi-upgrade..."
gnocchi-upgrade --config-file $data/gnocchi.conf

echo "Run micro-metricd..."

cat >> $LOG <<EOF
##############################################
$(date --rfc-3339=seconds)
##############################################
EOF


python perf/micro-metricd.py --config-file $data/gnocchi.conf 2>&1| tee -a $LOG
# python perf/micro-checks.py --config-file $data/gnocchi.conf 2>&1| tee -a $LOG
