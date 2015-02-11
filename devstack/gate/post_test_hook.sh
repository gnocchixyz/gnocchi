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

source $BASE/new/devstack/functions-common
source $BASE/new/devstack/openrc admin admin

set -x

cd $BASE/new/gnocchi

keystone endpoint-list
keystone service-list
keystone endpoint-get --service metric

gnocchi_endpoint=$(keystone endpoint-get --service metric | grep ' metric.publicURL ' | get_field 2)
die_if_not_set $LINENO gnocchi_endpoint "Keystone fail to get gnocchi endpoint"
token=$(keystone token-get | grep ' id ' | get_field 2)
die_if_not_set $LINENO token "Keystone fail to get token"

# NOTE(sileht): Just list policies for now
curl -X GET $gnocchi_endpoint/v1/archive_policy -H "Content-Type: application/json" -H "X-Auth-Token: $token"
