# -*- encoding: utf-8 -*-
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
import copy
import itertools
import os
import uuid

from oslo_config import cfg
from oslo_middleware import cors
from stevedore import extension

import gnocchi.archive_policy
import gnocchi.indexer
import gnocchi.storage
import gnocchi.storage.ceph
import gnocchi.storage.file
import gnocchi.storage.s3
import gnocchi.storage.swift

_STORAGE_OPTS = list(itertools.chain(gnocchi.storage.ceph.OPTS,
                                     gnocchi.storage.file.OPTS,
                                     gnocchi.storage.swift.OPTS,
                                     gnocchi.storage.s3.OPTS))


_INCOMING_OPTS = copy.deepcopy(_STORAGE_OPTS)
for opt in _INCOMING_OPTS:
    opt.default = '${storage.%s}' % opt.name


def list_opts():
    return [
        ("indexer", gnocchi.indexer.OPTS),
        ("metricd", (
            cfg.IntOpt('workers', min=1,
                       help='Number of workers for Gnocchi metric daemons. '
                       'By default the available number of CPU is used.'),
            cfg.IntOpt('metric_processing_delay',
                       default=60,
                       deprecated_group='storage',
                       help="How many seconds to wait between "
                       "scheduling new metrics to process"),
            cfg.IntOpt('metric_reporting_delay',
                       deprecated_group='storage',
                       default=120,
                       help="How many seconds to wait between "
                       "metric ingestion reporting"),
            cfg.IntOpt('metric_cleanup_delay',
                       deprecated_group='storage',
                       default=300,
                       help="How many seconds to wait between "
                       "cleaning of expired data"),
        )),
        ("api", (
            cfg.StrOpt('paste_config',
                       default=os.path.abspath(
                           os.path.join(
                               os.path.dirname(__file__),
                               "rest", "api-paste.ini")),
                       help='Path to API Paste configuration.'),
            cfg.StrOpt('auth_mode',
                       default="noauth",
                       choices=extension.ExtensionManager(
                           "gnocchi.rest.auth_helper").names(),
                       help='Authentication mode to use.'),
            cfg.IntOpt('max_limit',
                       default=1000,
                       help=('The maximum number of items returned in a '
                             'single response from a collection resource')),
        )),
        ("storage", (_STORAGE_OPTS + gnocchi.storage._carbonara.OPTS +
                     gnocchi.storage.OPTS)),
        ("incoming", _INCOMING_OPTS),
        ("statsd", (
            cfg.StrOpt('host',
                       default='0.0.0.0',
                       help='The listen IP for statsd'),
            cfg.PortOpt('port',
                        default=8125,
                        help='The port for statsd'),
            cfg.Opt(
                'resource_id',
                type=uuid.UUID,
                help='Resource UUID to use to identify statsd in Gnocchi'),
            cfg.StrOpt(
                'user_id',
                help='User ID to use to identify statsd in Gnocchi'),
            cfg.StrOpt(
                'project_id',
                help='Project ID to use to identify statsd in Gnocchi'),
            cfg.StrOpt(
                'archive_policy_name',
                help='Archive policy name to use when creating metrics'),
            cfg.FloatOpt(
                'flush_delay',
                default=10,
                help='Delay between flushes'),
        )),
        ("archive_policy", gnocchi.archive_policy.OPTS),
    ]


def set_defaults():
    cfg.set_defaults(cors.CORS_OPTS,
                     allow_headers=[
                         'X-Auth-Token',
                         'X-Subject-Token',
                         'X-User-Id',
                         'X-Domain-Id',
                         'X-Project-Id',
                         'X-Roles'])
