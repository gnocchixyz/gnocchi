#!/bin/bash
set -e
set -x
# Activate pifpaf for indexer
GNOCCHI_TEST_INDEXER_DRIVER=${GNOCCHI_TEST_INDEXER_DRIVER:-postgresql}
eval `pifpaf run $GNOCCHI_TEST_INDEXER_DRIVER`
kill_pifpaf ()
{
    test -n "$PIFPAF_PID" && kill "$PIFPAF_PID"
}
trap kill_pifpaf EXIT
export GNOCCHI_INDEXER_URL=${PIFPAF_URL/#mysql:/mysql+pymysql:}
$*
