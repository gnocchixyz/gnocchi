#!/bin/bash -x
set -e
GNOCCHI_TEST_STORAGE_DRIVERS=${GNOCCHI_TEST_STORAGE_DRIVERS:-file}
GNOCCHI_TEST_INDEXER_DRIVERS=${GNOCCHI_TEST_INDEXER_DRIVERS:-postgresql}
for storage in ${GNOCCHI_TEST_STORAGE_DRIVERS}
do
    export GNOCCHI_TEST_STORAGE_DRIVER=$storage
    for indexer in ${GNOCCHI_TEST_INDEXER_DRIVERS}
    do
        pifpaf -g GNOCCHI_INDEXER_URL run $indexer -- ./tools/pretty_tox.sh $*
    done
    # NOTE(sileht): Wait all storage tests
    wait
    # TODO(sileht): the output can be a mess with this
    # Create a less verbose testrun output (with dot like nose ?)
    # merge all subunit output and print it in after_script in travis
done
