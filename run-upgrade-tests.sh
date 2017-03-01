#!/bin/bash
set -e

export GNOCCHI_DATA=$(mktemp -d -t gnocchi.XXXX)

GDATE=$((which gdate >/dev/null && echo gdate) || echo date)

old_version=$(pip freeze | sed -n '/gnocchi==/s/.*==\(.*\)/\1/p')

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
            gnocchi measures show --aggregation $agg --resource-id $resource_id metric > $dir/${agg}.txt
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

    {
        measures_sep=""
        MEASURES=$(for i in $(seq 0 10 288000); do
                       now=$($GDATE --iso-8601=s -d "-${i}minute") ; value=$((RANDOM % 13 + 52))
                       echo -n "$measures_sep {\"timestamp\": \"$now\", \"value\": $value }"
                       measures_sep=","
                   done)
        echo -n '{'
        resource_sep=""
        for resource_id in ${RESOURCE_IDS[@]} $RESOURCE_ID_EXT; do
            echo -n "$resource_sep \"$resource_id\": { \"metric\": [ $MEASURES ] }"
            resource_sep=","
        done
        echo -n '}'
    } | gnocchi measures batch-resources-metrics -

    echo "* Waiting for measures computation"
    while [ $(gnocchi status -f value -c "storage/total number of measures to process") -gt 0 ]; do sleep 1 ; done
}

pifpaf_stop(){
    :
}

cleanup(){
    pifpaf_stop
    rm -rf $GNOCCHI_DATA
}
trap cleanup EXIT


if [ "$STORAGE_DAEMON" == "ceph" ]; then
    rados -c $STORAGE_CEPH_CONF mkpool gnocchi
    STORAGE_URL=ceph://$STORAGE_CEPH_CONF
else
    STORAGE_URL=file://$GNOCCHI_DATA
fi

eval $(pifpaf run gnocchi --indexer-url $INDEXER_URL --storage-url $STORAGE_URL)
export OS_AUTH_TYPE=gnocchi-basic
export GNOCCHI_USER=$GNOCCHI_USER_ID
original_statsd_resource_id=$GNOCCHI_STATSD_RESOURCE_ID
inject_data $GNOCCHI_DATA
dump_data $GNOCCHI_DATA/old
pifpaf_stop

new_version=$(python setup.py --version)
echo "* Upgrading Gnocchi from $old_version to $new_version"
pip install -q -U .[${GNOCCHI_VARIANT}]

eval $(pifpaf --debug run gnocchi --indexer-url $INDEXER_URL --storage-url $STORAGE_URL)
# Gnocchi 3.1 uses basic auth by default
export OS_AUTH_TYPE=gnocchi-basic
export GNOCCHI_USER=$GNOCCHI_USER_ID

# pifpaf creates a new statsd resource on each start
gnocchi resource delete $GNOCCHI_STATSD_RESOURCE_ID

dump_data $GNOCCHI_DATA/new

echo "* Checking output difference between Gnocchi $old_version and $new_version"
diff -uNr $GNOCCHI_DATA/old $GNOCCHI_DATA/new
