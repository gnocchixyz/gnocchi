#!/bin/bash -x
set -e
GNOCCHI_TEST_STORAGE_DRIVERS=${GNOCCHI_TEST_STORAGE_DRIVERS:-file}
GNOCCHI_TEST_INDEXER_DRIVERS=${GNOCCHI_TEST_INDEXER_DRIVERS:-postgresql}
for storage in ${GNOCCHI_TEST_STORAGE_DRIVERS}
do
    export GNOCCHI_TEST_STORAGE_DRIVER=$storage
    for indexer in ${GNOCCHI_TEST_INDEXER_DRIVERS}
    do
        case $GNOCCHI_TEST_STORAGE_DRIVER in
            ceph)
                pifpaf run ceph -- pifpaf -g GNOCCHI_INDEXER_URL run $indexer -- ./tools/pretty_tox.sh $*
                ;;
            s3)
                if ! which s3rver >/dev/null 2>&1
                then
                    mkdir npm-s3rver
                    export NPM_CONFIG_PREFIX=npm-s3rver
                    npm install s3rver --global
                    export PATH=$PWD/npm-s3rver/bin:$PATH
                fi
                pifpaf -e GNOCCHI_STORAGE run s3rver -- \
                       pifpaf -e GNOCCHI_INDEXER run $indexer -- \
                       ./tools/pretty_tox.sh $*
                ;;
            *)
                pifpaf -g GNOCCHI_INDEXER_URL run $indexer -- ./tools/pretty_tox.sh $*
                ;;
        esac
    done
done
