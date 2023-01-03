#!/bin/bash
set -e

if [ "$1" == "postgresql-file" ]; then
  echo "Deploying Postgresql with PifPaf."
  eval $(pifpaf --debug --env-prefix INDEXER run postgresql)
elif [ "$1" == "mysql-ceph" ]; then
  # Installing PifPaf from source due to the lack of a new version to handle
  # Ceph global insecure claims. The patch was introduced in PifPaf via commit
  # https://github.com/jd/pifpaf/commit/fb376a83a47d678952672a7f5d36a02101135fb2,
  # but it has never been released. Therefore, we need to install it here from
  # master/main branch in the upstream repository
  pip install install git+https://github.com/jd/pifpaf.git@51f74a3d8743a7ac33259413df7efc30df993460

  echo "Deploying MySQL with PifPaf."
  eval $(pifpaf --debug --env-prefix INDEXER run mysql)
  echo "Deploying Ceph with PifPaf."
  eval $(pifpaf --debug --env-prefix STORAGE run ceph)
else
  echo "error: unsupported upgrade type"
  exit 1
fi
echo "Finished deploying backend components with PifPaf."

export GNOCCHI_DATA=$(mktemp -d -t gnocchi.XXXX)

echo "* Installing Gnocchi from ${GNOCCHI_VERSION_FROM}"
pip install -q --force-reinstall git+https://github.com/gnocchixyz/gnocchi.git@${GNOCCHI_VERSION_FROM}#egg=gnocchi[${GNOCCHI_VARIANT}]

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
    } | gnocchi measures batch-resources-metrics --debug -

    echo "* Waiting for measures computation"
    while [ $(gnocchi status -f value -c "storage/total number of measures to process") -gt 0 ]; do sleep 1 ; done
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
    ceph -c $STORAGE_CEPH_CONF osd pool create gnocchi 16 16 replicated
    STORAGE_URL=ceph://$STORAGE_CEPH_CONF
else
    STORAGE_URL=file://$GNOCCHI_DATA
fi

# This downgrade of `numpy` is needed to enable the merge of PR
# https://github.com/gnocchixyz/gnocchi/pull/1279, which is the PR that
# introduces the support of numpy >= 1.24. After we merge it, we can remove
# this downgrade here.
pip install "numpy<1.24"
eval $(pifpaf run gnocchi --indexer-url $INDEXER_URL --storage-url $STORAGE_URL)

export OS_AUTH_TYPE=gnocchi-basic
export GNOCCHI_USER=$GNOCCHI_USER_ID
original_statsd_resource_id=$GNOCCHI_STATSD_RESOURCE_ID
inject_data $GNOCCHI_DATA
dump_data $GNOCCHI_DATA/old
pifpaf_stop

new_version=$(python setup.py --version)
echo "* Upgrading Gnocchi from $GNOCCHI_VERSION_FROM to $new_version"
pip install -v -U .[${GNOCCHI_VARIANT}]

eval $(pifpaf run gnocchi --indexer-url $INDEXER_URL --storage-url $STORAGE_URL)
# Gnocchi 3.1 uses basic auth by default
export OS_AUTH_TYPE=gnocchi-basic
export GNOCCHI_USER=$GNOCCHI_USER_ID

# pifpaf creates a new statsd resource on each start
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
