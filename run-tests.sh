#!/bin/bash -x
set -e
GNOCCHI_TEST_STORAGE_DRIVERS=${GNOCCHI_TEST_STORAGE_DRIVERS:-file}
GNOCCHI_TEST_INDEXER_DRIVERS=${GNOCCHI_TEST_INDEXER_DRIVERS:-postgresql}
for storage in ${GNOCCHI_TEST_STORAGE_DRIVERS}
do
    for indexer in ${GNOCCHI_TEST_INDEXER_DRIVERS}
    do
        storage_setup_script=./setup-${storage}-tests.sh
        if [ ! -x "$storage_setup_script" ]
        then
            unset storage_setup_script
        fi
        GNOCCHI_TEST_STORAGE_DRIVER=$storage ./setup-${indexer}-tests.sh "${storage_setup_script}" ./tools/pretty_tox.sh $*
    done
done
