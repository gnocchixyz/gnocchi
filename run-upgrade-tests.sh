#!/bin/bash
set -e

export GNOCCHI_DATA=$(mktemp -d -t gnocchi.XXXX)

GDATE=$((which gdate >/dev/null && echo gdate) || echo date)
GSED=$((which gsed >/dev/null && echo gsed) || echo sed)

old_version=$(pip freeze | sed -n '/gnocchi==/s/.*==\(.*\)/\1/p')
[ "${old_version:0:1}" == "3" ] && have_resource_type_post=1

RESOURCE_IDS=(
    "5a301761-aaaa-46e2-8900-8b4f6fe6675a"
    "5a301761-bbbb-46e2-8900-8b4f6fe6675a"
    "5a301761-cccc-46e2-8900-8b4f6fe6675a"
    "non-uuid"
)

[ "$have_resource_type_post" ] && RESOURCE_ID_EXT="5a301761/dddd/46e2/8900/8b4f6fe6675a"

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

    if [ "$have_resource_type_post" ]
    then
        gnocchi resource-type create ext > /dev/null
        gnocchi resource create ext --attribute id:$RESOURCE_ID_EXT -n metric:high > /dev/null
    fi

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
# Override default to be sure to use noauth
export OS_AUTH_TYPE=gnocchi-noauth
export GNOCCHI_USER_ID=admin
export GNOCCHI_PROJECT_ID=admin
original_statsd_resource_id=$GNOCCHI_STATSD_RESOURCE_ID
inject_data $GNOCCHI_DATA
# Encode resource id as it contains slashes and gnocchiclient does not encode it
[ "$have_resource_type_post" ] && RESOURCE_ID_EXT="19235bb9-35ca-5f55-b7db-165cfb033c86"
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

RESOURCE_IDS=(
    "5a301761-aaaa-46e2-8900-8b4f6fe6675a"
    "5a301761-bbbb-46e2-8900-8b4f6fe6675a"
    "5a301761-cccc-46e2-8900-8b4f6fe6675a"
    "24d2e3ed-c7c1-550f-8232-56c48809a6d4"
)
# NOTE(sileht): / are now _
# NOTE(jdanjou): and we reencode for admin:admin, but we cannot authenticate as
# admin:admin in basic since ":" is forbidden in any username, so let's use the direct
# computed ID
[ "$have_resource_type_post" ] && RESOURCE_ID_EXT="517920a9-2e50-58b8-88e8-25fd7aae1d8f"

dump_data $GNOCCHI_DATA/new

# NOTE(sileht): change the output of the old gnocchi to compare with the new without '/'
$GSED -i -e "s,5a301761/dddd/46e2/8900/8b4f6fe6675a,5a301761_dddd_46e2_8900_8b4f6fe6675a,g" \
      -e "s,19235bb9-35ca-5f55-b7db-165cfb033c86,517920a9-2e50-58b8-88e8-25fd7aae1d8f,g" \
      -e "s,None                                ,${original_statsd_resource_id},g" \
      -e "s,37d1416a-381a-5b6c-99ef-37d89d95f1e1,24d2e3ed-c7c1-550f-8232-56c48809a6d4,g" $GNOCCHI_DATA/old/resources.list

echo "* Checking output difference between Gnocchi $old_version and $new_version"
diff -uNr $GNOCCHI_DATA/old $GNOCCHI_DATA/new
