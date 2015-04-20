# -*- encoding: utf-8 -*-
#
# Copyright © 2014-2015 eNovance
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

from oslo_config import cfg
from oslo_config import types

import gnocchi.archive_policy
import gnocchi.indexer
import gnocchi.storage
import gnocchi.storage.ceph
import gnocchi.storage.file
import gnocchi.storage.swift


def list_opts():
    return [
        ("indexer", gnocchi.indexer.OPTS),
        ("api", (
            cfg.IntOpt('port',
                       default=8041,
                       help='The port for the Gnocchi API server.'),
            cfg.StrOpt('host',
                       default='0.0.0.0',
                       help='The listen IP for the Gnocchi API server.'),
            cfg.BoolOpt('pecan_debug',
                        default=False,
                        help='Toggle Pecan Debug Middleware.'),
            cfg.MultiStrOpt(
                'middlewares',
                default=['keystonemiddleware.auth_token.AuthProtocol'],
                help='Middlewares to use',),
            cfg.Opt('workers', type=types.Integer(min=1),
                    help='Number of workers for Gnocchi API server. '
                    'By default the available number of CPU is used.'),
        )),
        ("storage", itertools.chain(gnocchi.storage._carbonara.OPTS,
                                    gnocchi.storage.OPTS,
                                    gnocchi.storage.ceph.OPTS,
                                    gnocchi.storage.file.OPTS,
                                    gnocchi.storage.swift.OPTS)),
        ("statsd", (
            cfg.StrOpt(
                'resource_id',
                help='Resource UUID to use to identify statsd in Gnocchi'),
            cfg.StrOpt(
                'user_id',
                help='User UUID to use to identify statsd in Gnocchi'),
            cfg.StrOpt(
                'project_id',
                help='Project UUID to use to identify statsd in Gnocchi'),
            cfg.StrOpt(
                'archive_policy_name',
                help='Archive policy name to use when creating metrics'),
            cfg.FloatOpt(
                'flush_delay',
                help='Delay between flushes'),
        )),
        ("archive_policy", gnocchi.archive_policy.OPTS),
    ]
