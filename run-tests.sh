#!/bin/bash -x
set -e
GNOCCHI_TEST_STORAGE_DRIVERS=${GNOCCHI_TEST_STORAGE_DRIVERS:-file}
GNOCCHI_TEST_INDEXER_DRIVERS=${GNOCCHI_TEST_INDEXER_DRIVERS:-postgresql}
for storage in ${GNOCCHI_TEST_STORAGE_DRIVERS}
do
    export GNOCCHI_TEST_STORAGE_DRIVER=$storage
    for indexer in ${GNOCCHI_TEST_INDEXER_DRIVERS}
    do
        if [ "$GNOCCHI_TEST_STORAGE_DRIVER" == "ceph" ]; then
            pifpaf run ceph -- pifpaf -g GNOCCHI_INDEXER_URL run $indexer -- ./tools/pretty_tox.sh $*
        elif [ "$GNOCCHI_TEST_STORAGE_DRIVER" == "s3" ]
        then
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

        else
            pifpaf -g GNOCCHI_INDEXER_URL run $indexer -- ./tools/pretty_tox.sh $*
        fi
    done
done
