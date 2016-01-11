#!/bin/bash -x
set -e
GNOCCHI_TEST_STORAGE_DRIVERS=${GNOCCHI_TEST_STORAGE_DRIVERS:-file}
GNOCCHI_TEST_INDEXER_DRIVERS=${GNOCCHI_TEST_INDEXER_DRIVERS:-postgresql}
for storage in ${GNOCCHI_TEST_STORAGE_DRIVERS}
do
    for indexer in ${GNOCCHI_TEST_INDEXER_DRIVERS}
    do
        export GNOCCHI_TEST_INDEXER_DRIVER=$indexer
        export GNOCCHI_TEST_STORAGE_DRIVER=$storage
        ./setup-test-env.sh ./tools/pretty_tox.sh $*
    done
done
