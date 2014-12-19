# -*- encoding: utf-8 -*-
#
# Copyright © 2014 eNovance
#
# Authors: Julien Danjou <julien@danjou.info>
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
import itertools

import gnocchi.indexer
import gnocchi.openstack.common.log
import gnocchi.openstack.common.policy
import gnocchi.rest.app
import gnocchi.storage
import gnocchi.storage.file
import gnocchi.storage.swift


def list_opts():
    return [
        ("DEFAULT", itertools.chain(
            gnocchi.openstack.common.log.common_cli_opts,
            gnocchi.openstack.common.log.logging_cli_opts,
            gnocchi.openstack.common.log.generic_log_opts,
            gnocchi.openstack.common.log.log_opts,
            gnocchi.openstack.common.policy.policy_opts)),
        ("indexer", gnocchi.indexer.OPTS),
        ("api", gnocchi.rest.app.OPTS),
        ("storage", itertools.chain(gnocchi.storage.OPTS,
                                    gnocchi.storage.file.OPTS,
                                    gnocchi.storage.swift.OPTS)),
    ]
