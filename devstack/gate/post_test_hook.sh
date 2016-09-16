#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

# This script is executed inside post_test_hook function in devstack gate.

source $BASE/new/devstack/openrc admin admin

set -e

function generate_testr_results {
    if [ -f .testrepository/0 ]; then
        sudo /usr/os-testr-env/bin/testr last --subunit > $WORKSPACE/testrepository.subunit
        sudo mv $WORKSPACE/testrepository.subunit $BASE/logs/testrepository.subunit
        sudo /usr/os-testr-env/bin/subunit2html $BASE/logs/testrepository.subunit $BASE/logs/testr_results.html
        sudo gzip -9 $BASE/logs/testrepository.subunit
        sudo gzip -9 $BASE/logs/testr_results.html
        sudo chown jenkins:jenkins $BASE/logs/testrepository.subunit.gz $BASE/logs/testr_results.html.gz
        sudo chmod a+r $BASE/logs/testrepository.subunit.gz $BASE/logs/testr_results.html.gz
    fi
}

set -x

export GNOCCHI_DIR="$BASE/new/gnocchi"
sudo chown -R stack:stack $GNOCCHI_DIR
cd $GNOCCHI_DIR

openstack catalog list

export GNOCCHI_SERVICE_TOKEN=$(openstack token issue -c id -f value)
export GNOCCHI_SERVICE_URL=$(openstack catalog show metric -c endpoints -f value | awk '/public/{print $2}')

curl -X GET ${GNOCCHI_SERVICE_URL}/v1/archive_policy -H "Content-Type: application/json"

sudo gnocchi-upgrade

# Just ensure tools still works
gnocchi metric create
sudo -E -H -u stack $GNOCCHI_DIR/tools/measures_injector.py --metrics 1 --batch-of-measures 2 --measures-per-batch 2

# NOTE(sileht): on swift job permissions are wrong, I don't known why
sudo chown -R tempest:stack $BASE/new/tempest
sudo chown -R tempest:stack $BASE/data/tempest

# Run tests with tempst
cd $BASE/new/tempest
set +e
sudo -H -u tempest OS_TEST_TIMEOUT=$TEMPEST_OS_TEST_TIMEOUT tox -eall-plugin -- gnocchi --concurrency=$TEMPEST_CONCURRENCY
TEMPEST_EXIT_CODE=$?
set -e
if [[ $TEMPEST_EXIT_CODE != 0 ]]; then
    # Collect and parse result
    generate_testr_results
    exit $TEMPEST_EXIT_CODE
fi

# Run tests with tox
cd $GNOCCHI_DIR
echo "Running gnocchi functional test suite"
set +e
sudo -E -H -u stack tox -epy27-gate
EXIT_CODE=$?
set -e

# Collect and parse result
generate_testr_results
exit $EXIT_CODE
