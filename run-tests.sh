#!/bin/bash -x
set -e

# NOTE(sileht): Enable bash process tracking and send sigterm to the whole
# process group

cleanup(){
    for PID in $PIDS; do
        PGID=$(ps -o pgid "$PID" | grep [0-9] | tr -d ' ')
        kill -- -$PGID
    done
}
trap cleanup EXIT

sudo apt update && sudo apt upgrade

echo "MySQL version: $(mysql --version)"

echo "Pip path: $(whereis pip)"

echo "Listing all pip libraries."
pip freeze

echo "Gnocchi-api version: $(gnocchi-api --version)"
echo "Gnocchi version: $(gnocchi --version)"


PIDS=""
GNOCCHI_TEST_STORAGE_DRIVERS=${GNOCCHI_TEST_STORAGE_DRIVERS:-file}
GNOCCHI_TEST_INDEXER_DRIVERS=${GNOCCHI_TEST_INDEXER_DRIVERS:-postgresql}
for storage in ${GNOCCHI_TEST_STORAGE_DRIVERS}
do
    export GNOCCHI_TEST_STORAGE_DRIVER=$storage
    for indexer in ${GNOCCHI_TEST_INDEXER_DRIVERS}
    do
        {
        case $GNOCCHI_TEST_STORAGE_DRIVER in
            ceph|redis)
                pifpaf --debug run $GNOCCHI_TEST_STORAGE_DRIVER -- pifpaf --debug -g GNOCCHI_INDEXER_URL run $indexer -- stestr run $*
                ;;
            s3)
                if ! which s3rver >/dev/null 2>&1
                then
                    mkdir -p npm-s3rver
                    export NPM_CONFIG_PREFIX=npm-s3rver
                    npm install s3rver --global
                    export PATH=$PWD/npm-s3rver/bin:$PATH
                fi
                pifpaf --debug -e GNOCCHI_STORAGE run s3rver -- \
                       pifpaf -e GNOCCHI_INDEXER --debug run $indexer -- \
                       stestr run $*
                ;;
            *)
                pifpaf --debug -g GNOCCHI_INDEXER_URL run $indexer -- stestr run $*
                ;;
        esac
        # NOTE(sileht): Start all storage tests at once
        } &
        PIDS="$PIDS $!"
    done
    # NOTE(sileht): Wait all storage tests, we tracks pid
    # because wait without pid always return 0
    for pid in $PIDS; do
        wait $pid
    done
    PIDS=""
    # TODO(sileht): the output can be a mess with this
    # Create a less verbose testrun output (with dot like nose ?)
    # merge all subunit output and print it after.
done
