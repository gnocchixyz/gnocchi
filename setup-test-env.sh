#!/bin/bash
set -e
set -x
# Activate overtest for indexer
GNOCCHI_TEST_INDEXER_DRIVER=${GNOCCHI_TEST_INDEXER_DRIVER:-postgresql}
source $(which overtest) $GNOCCHI_TEST_INDEXER_DRIVER
export GNOCCHI_INDEXER_URL=${OVERTEST_URL/#mysql:/mysql+pymysql:}
$*
