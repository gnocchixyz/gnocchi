#!/bin/bash -x
set -e

cleanup(){
    type -t indexer_stop >/dev/null && indexer_stop || true
    type -t storage_stop >/dev/null && storage_stop || true
}
trap cleanup EXIT

check_empty_var() {
    local x=$(eval echo `echo \\$${1}`)
    if [ -z "$x" ]; then
        echo "Variable \$${1} is unset"
        exit 15
    fi
}

PYTHON_VERSION_MAJOR=$(python -c 'import sys; print(sys.version_info.major)')

GNOCCHI_TEST_STORAGE_DRIVERS=${GNOCCHI_TEST_STORAGE_DRIVERS:-file}
GNOCCHI_TEST_INDEXER_DRIVERS=${GNOCCHI_TEST_INDEXER_DRIVERS:-postgresql}
for storage in ${GNOCCHI_TEST_STORAGE_DRIVERS}; do
    if [ "$storage" == "swift" ] && [ "$PYTHON_VERSION_MAJOR" == "3" ]; then
        echo "WARNING: swift does not support python 3 skipping"
        continue
    fi
    for indexer in ${GNOCCHI_TEST_INDEXER_DRIVERS}; do
        unset STORAGE_URL
        unset INDEXER_URL
        case $storage in
            ceph)
                eval $(pifpaf -e STORAGE run ceph)
                check_empty_var STORAGE_URL
                ceph -c $STORAGE_CEPH_CONF osd pool create gnocchi 16 16 replicated
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

        check_empty_var STORAGE_URL

        eval $(pifpaf -e INDEXER run $indexer)
        check_empty_var INDEXER_URL

        export GNOCCHI_SERVICE_TOKEN="" # Just make gabbi happy
        export GNOCCHI_AUTHORIZATION="basic YWRtaW46" # admin in base64
        export GNOCCHI_TEST_PATH=gnocchi/tests/functional_live
        pifpaf -e GNOCCHI run gnocchi --indexer-url $INDEXER_URL --storage-url $STORAGE_URL --coordination-driver redis -- ./tools/pretty_tox.sh $*

        cleanup
    done
done
