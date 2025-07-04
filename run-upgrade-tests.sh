#!/bin/bash
set -e

if [ "$1" == "postgresql-file" ]; then
  eval $(pifpaf --env-prefix INDEXER run postgresql)
elif [ "$1" == "mysql-ceph" ]; then
  eval $(pifpaf --env-prefix INDEXER run mysql)
  eval $(pifpaf --env-prefix STORAGE run ceph)
else
  echo "error: unsupported upgrade type"
  exit 1
fi

export GNOCCHI_DATA=$(mktemp -d -t gnocchi.XXXX)

echo "* Installing Gnocchi from ${GNOCCHI_VERSION_FROM}"
python -m pip install -q --force-reinstall "gnocchi[${GNOCCHI_VARIANT}] @ git+https://github.com/gnocchixyz/gnocchi.git@${GNOCCHI_VERSION_FROM}"

RESOURCE_IDS=(
    "5a301761-aaaa-46e2-8900-8b4f6fe6675a"
    "5a301761-bbbb-46e2-8900-8b4f6fe6675a"
    "5a301761-cccc-46e2-8900-8b4f6fe6675a"
    "non-uuid"
)

dump_data(){
    dir="$1"
    mkdir -p $dir
    echo "* Dumping measures aggregations to $dir"
    gnocchi resource list -c id -c type -c project_id -c user_id -c original_resource_id -c started_at -c ended_at -c revision_start -c revision_end | tee $dir/resources.list
    for resource_id in ${RESOURCE_IDS[@]} $RESOURCE_ID_EXT; do
        for agg in min max mean sum ; do
            gnocchi measures show --aggregation $agg --resource-id $resource_id metric -f json > $dir/${agg}.json
        done
    done
}

inject_data() {
    echo "* Injecting measures in Gnocchi"
    # TODO(sileht): Generate better data that ensure we have enought split that cover all
    # situation

    for resource_id in ${RESOURCE_IDS[@]}; do
        gnocchi resource create generic --attribute id:$resource_id -n metric:high > /dev/null
    done

    # Create a resource with an history
    gnocchi resource-type create ext --attribute someattr:string:false:max_length=32 > /dev/null
    gnocchi resource create --type ext --attribute someattr:foobar -n metric:high historized_resource > /dev/null
    gnocchi resource update --type ext --attribute someattr:foobaz historized_resource > /dev/null

    {
        measures_sep=""
        MEASURES=$(python -c 'import datetime, random, json; now = datetime.datetime.utcnow(); print(json.dumps([{"timestamp": (now - datetime.timedelta(seconds=i)).isoformat(), "value": random.uniform(-100000, 100000)} for i in range(0, 288000, 10)]))')
        echo -n '{'
        resource_sep=""
        for resource_id in ${RESOURCE_IDS[@]} $RESOURCE_ID_EXT; do
            echo -n "$resource_sep \"$resource_id\": { \"metric\": $MEASURES }"
            resource_sep=","
        done
        echo -n '}'
    } | gnocchi measures batch-resources-metrics -

    timeout=300
    echo "* Waiting for measures computation (for up to ${timeout} seconds)"
    for i in $(seq 0 "${timeout}"); do
        if [ "${i}" -ge "${timeout}" ]; then
            echo "ERROR: Timed out while waiting for measures computation: ${num_measures} not processed" > /dev/stderr
            exit 1
        fi
        num_measures=$(gnocchi status -f value -c "storage/total number of measures to process")
        if [ "${num_measures}" -eq 0 ]; then
            break
        fi
        if [ "${i}" -gt 0 ] && [ $(( "${i}" % 60 )) -eq 0 ]; then
            echo "* Still waiting..."
        fi
        sleep 1
    done
}

pifpaf_stop(){
    :
}

cleanup(){
    pifpaf_stop
    rm -rf $GNOCCHI_DATA
    indexer_stop || true
    [ "$STORAGE_DAEMON" == "ceph" ] && storage_stop || true
}
trap cleanup EXIT


if [ "$STORAGE_DAEMON" == "ceph" ]; then
    echo "* Creating 'gnocchi' pool"
    ceph -c $STORAGE_CEPH_CONF osd pool create gnocchi 16 16 replicated
    STORAGE_URL=ceph://$STORAGE_CEPH_CONF
else
    STORAGE_URL=file://$GNOCCHI_DATA
fi

echo "* Starting Gnocchi ${GNOCCHI_VERSION_FROM}"
eval $(pifpaf run gnocchi --indexer-url $INDEXER_URL --storage-url $STORAGE_URL)
export OS_AUTH_TYPE=gnocchi-basic
export GNOCCHI_USER=$GNOCCHI_USER_ID
original_statsd_resource_id=$GNOCCHI_STATSD_RESOURCE_ID
inject_data $GNOCCHI_DATA
dump_data $GNOCCHI_DATA/old
pifpaf_stop

new_version=$(python setup.py --version)
echo "* Upgrading Gnocchi from $GNOCCHI_VERSION_FROM to $new_version"
python -m pip install -v -U .[${GNOCCHI_VARIANT}]

eval $(pifpaf --verbose --debug run gnocchi --indexer-url $INDEXER_URL --storage-url $STORAGE_URL)
# Gnocchi 3.1 uses basic auth by default
export OS_AUTH_TYPE=gnocchi-basic
export GNOCCHI_USER=$GNOCCHI_USER_ID

# Pifpaf configures the logs for the standard output. Therefore, depending
# on the operating system, the standard output has some buffer size, which
# needs to be released. Otherwise, the logs stop to be writen, and the
# execution of the code is "frozen", due to the lack of buffer in the
# process output. To work around that, we can read the buffer, and dump it
# into a log file. Then, we can cat the log file content at the end of the
# process.
UWSGI_LOG_FILE=/tmp/uwsgi-new-version.log
METRICD_LOG_FILE=/tmp/gnocchi-metricd-new-version.log
for PID in $(pidof uwsgi); do
  echo "Configuring dump of uWSGI process [PID=${PID}] outputs to avoid freeze while processing because the logs are not read from buffer"
  cat /proc/${PID}/fd/1 >> ${UWSGI_LOG_FILE} &
  cat /proc/${PID}/fd/2 >> ${UWSGI_LOG_FILE} &
done

for PID in $(pidof gnocchi-metricd); do
  echo "Configuring dump of MetricD process [PID=${PID}] outputs to avoid freeze while processing because the logs are not read from buffer"
  cat /proc/${PID}/fd/1 >> ${METRICD_LOG_FILE} &
  cat /proc/${PID}/fd/2 >> ${METRICD_LOG_FILE} &
done

# pifpaf creates a new statsd resource on each start
echo "Executing the deletion of statsd resource [${GNOCCHI_STATSD_RESOURCE_ID}]."
gnocchi resource delete $GNOCCHI_STATSD_RESOURCE_ID

dump_data $GNOCCHI_DATA/new

echo "* Checking output difference between Gnocchi $GNOCCHI_VERSION_FROM and $new_version"
# This asserts we find the new measures in the old ones. Gnocchi > 4.1 will
# store less points because it uses the timespan and not the points of the
# archive policy
for old in $GNOCCHI_DATA/old/*.json; do
    new=$GNOCCHI_DATA/new/$(basename $old)
    python -c "import json; old = json.load(open('$old')); new = json.load(open('$new')); assert all(i in old for i in new)"
done

echo "Dump uWSGI log file"
cat ${UWSGI_LOG_FILE}

echo "Dump MetricD log file"
cat ${METRICD_LOG_FILE}
