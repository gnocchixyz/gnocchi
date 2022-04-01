#!/bin/bash -e

function print_header() {
    echo "==============================="
    echo "$1"
    echo "==============================="
}

if ! command -v tox &> /dev/null; then
    print_header "ERROR: tox not found"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    print_header "ERROR: docker-compose not found"
    exit 1
fi

print_header "Composing indexer service: $GNOCCHI_TEST_INDEXER_DRIVER"
GNOCCHI_INDEXER_COMPOSE_FILE=composes/${GNOCCHI_TEST_INDEXER_DRIVER}-indexer.yml
if [ ! -f $GNOCCHI_INDEXER_COMPOSE_FILE ]; then
    print_header "ERROR: $GNOCCHI_INDEXER_COMPOSE_FILE not found"
    exit 1
fi
docker-compose -f $GNOCCHI_INDEXER_COMPOSE_FILE up -d

if [ "$GNOCCHI_TEST_INDEXER_DRIVER" == "mysql" ]; then
    export GNOCCHI_INDEXER_URL=mysql+pymysql://root:secret@localhost/gnocchi
elif [ "$GNOCCHI_TEST_INDEXER_DRIVER" == "postgresql" ]; then
    export GNOCCHI_INDEXER_URL=postgresql://postgres:postgres@localhost/postgres
fi

print_header "Composing storage service: $GNOCCHI_TEST_STORAGE_DRIVER"
# TODO

print_header "Running unit tests"
tox -e stestr

print_header "Recreate indexer service: $GNOCCHI_TEST_INDEXER_DRIVER"
docker-compose -f $GNOCCHI_INDEXER_COMPOSE_FILE up -d --force-recreate

print_header "Recreate storage service: $GNOCCHI_TEST_STORAGE_DRIVER"
# TODO

print_header "Composing coordinator service: $GNOCCHI_TEST_COORDINATOR_DRIVER"
GNOCCHI_COORDINATOR_COMPOSE_FILE=composes/${GNOCCHI_TEST_COORDINATOR_DRIVER}-coordinator.yml
if [ ! -f $GNOCCHI_COORDINATOR_COMPOSE_FILE ]; then
    print_header "ERROR: $GNOCCHI_COORDINATOR_COMPOSE_FILE not found"
    exit 1
fi
docker-compose -f $GNOCCHI_COORDINATOR_COMPOSE_FILE up -d

print_header "Composing Gnocchi service"
export GNOCCHI_SERVICE_TOKEN=
export GNOCCHI_AUTHORIZATION=basic YWRtaW46
docker-compose -f composes/gnocchi.yml up -d

print_header "Running functional live tests"
export GNOCCHI_ENDPOINT=http://localhost:8041
export GNOCCHI_TEST_PATH=gnocchi/tests/functional_live
tox -e stestr
