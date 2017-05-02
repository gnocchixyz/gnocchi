#!/bin/bash -x
set -e

cleanup(){
    type -t indexer_stop >/dev/null && indexer_stop || true
    type -t storage_stop >/dev/null && storage_stop || true
}
trap cleanup EXIT

GNOCCHI_TEST_STORAGE_DRIVERS=${GNOCCHI_TEST_STORAGE_DRIVERS:-file}
GNOCCHI_TEST_INDEXER_DRIVERS=${GNOCCHI_TEST_INDEXER_DRIVERS:-postgresql}
for storage in ${GNOCCHI_TEST_STORAGE_DRIVERS}; do
    for indexer in ${GNOCCHI_TEST_INDEXER_DRIVERS}; do
        case $storage in
            ceph)
                eval $(pifpaf -e STORAGE run ceph)
                rados -c $STORAGE_CEPH_CONF mkpool gnocchi
                STORAGE_URL=ceph://$STORAGE_CEPH_CONF
                ;;
            s3)
                if ! which s3rver >/dev/null 2>&1
                then
                    mkdir -p npm-s3rver
                    export NPM_CONFIG_PREFIX=npm-s3rver
                    npm install s3rver --global
                    export PATH=$PWD/npm-s3rver/bin:$PATH
                fi
                eval $(pifpaf -e STORAGE run s3rver)
                ;;
            file)
                STORAGE_URL=file://
                ;;

            swift|redis)
                eval $(pifpaf -e STORAGE run $storage)
                ;;
            *)
                echo "Unsupported storage backend by functional tests: $storage"
                exit 1
                ;;
        esac

        eval $(pifpaf -e INDEXER run $indexer)

        export GNOCCHI_SERVICE_TOKEN="" # Just make gabbi happy
        export GNOCCHI_AUTHORIZATION="basic YWRtaW46" # admin in base64
        export OS_TEST_PATH=gnocchi/tests/functional_live
        pifpaf -e GNOCCHI run gnocchi --indexer-url $INDEXER_URL --storage-url $STORAGE_URL --coordination-driver redis -- ./tools/pretty_tox.sh $*

        cleanup
    done
done
