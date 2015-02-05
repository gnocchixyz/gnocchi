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

# This script is executed inside gate_hook function in devstack gate.


ENABLED_SERVICES="key,gnocchi-api,s-proxy,s-account,s-container,s-object,"
ENABLED_SERVICES+="ceilometer-acentral,ceilometer-collector,ceilometer-api,"
ENABLED_SERVICES+="ceilometer-alarm-notifier,ceilometer-alarm-evaluator,ceilometer-anotification"

export ENABLED_SERVICES
export DEVSTACK_LOCAL_CONFIG='enable_plugin gnocchi https://github.com/stackforge/gnocchi master'
export DEVSTACK_GATE_INSTALL_TESTONLY=1
export DEVSTACK_GATE_NO_SERVICES=1
export DEVSTACK_GATE_TEMPEST=0
export DEVSTACK_GATE_EXERCISES=0
export KEEP_LOCALRC=1


