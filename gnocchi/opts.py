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
import operator
import os
import pkg_resources
import uuid

from oslo_config import cfg
from oslo_middleware import cors

import gnocchi.archive_policy
import gnocchi.indexer
import gnocchi.storage
import gnocchi.storage.ceph
import gnocchi.storage.file
import gnocchi.storage.s3
import gnocchi.storage.swift


# NOTE(sileht): The oslo.config interpolation is buggy when the value
# is None, this replaces it by the expected empty string.
# Fix will perhaps be fixed by https://review.openstack.org/#/c/417496/
# But it seems some projects are relaying on the bug...
class CustomStrSubWrapper(cfg.ConfigOpts.StrSubWrapper):
    def __getitem__(self, key):
        value = super(CustomStrSubWrapper, self).__getitem__(key)
        if value is None:
            return ''
        return value

cfg.ConfigOpts.StrSubWrapper = CustomStrSubWrapper


_STORAGE_OPTS = list(itertools.chain(gnocchi.storage.OPTS,
                                     gnocchi.storage.ceph.OPTS,
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
                       required=True,
                       help='Number of workers for Gnocchi metric daemons. '
                       'By default the available number of CPU is used.'),
            cfg.IntOpt('metric_processing_delay',
                       default=30,
                       required=True,
                       deprecated_group='storage',
                       help="How many seconds to wait between "
                       "scheduling new metrics to process"),
            cfg.IntOpt('metric_reporting_delay',
                       deprecated_group='storage',
                       default=120,
                       min=-1,
                       required=True,
                       help="How many seconds to wait between "
                       "metric ingestion reporting. Set value to -1 to "
                       "disable reporting"),
            cfg.IntOpt('metric_cleanup_delay',
                       deprecated_group='storage',
                       default=300,
                       required=True,
                       help="How many seconds to wait between "
                       "cleaning of expired data"),
            cfg.IntOpt('tasks_per_worker',
                       default=256,
                       min=1,
                       help="How many tasks to assign each metricd worker "
                       "when scheduling measures processing jobs"),
        )),
        ("api", (
            cfg.StrOpt('paste_config',
                       default=os.path.abspath(
                           os.path.join(
                               os.path.dirname(__file__),
                               "rest", "api-paste.ini")),
                       help='Path to API Paste configuration.'),
            cfg.StrOpt('auth_mode',
                       default="basic",
                       choices=list(map(operator.attrgetter("name"),
                                    pkg_resources.iter_entry_points(
                                        "gnocchi.rest.auth_helper"))),
                       help='Authentication mode to use.'),
            cfg.IntOpt('max_limit',
                       default=1000,
                       required=True,
                       help=('The maximum number of items returned in a '
                             'single response from a collection resource')),
        )),
        ("storage", (_STORAGE_OPTS + gnocchi.storage._carbonara.OPTS)),
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
                deprecated_for_removal=True,
                help='User ID to use to identify statsd in Gnocchi'),
            cfg.StrOpt(
                'project_id',
                deprecated_for_removal=True,
                help='Project ID to use to identify statsd in Gnocchi'),
            cfg.StrOpt(
                'creator',
                default="${statsd.user_id}:${statsd.project_id}",
                help='Creator value to use to identify statsd in Gnocchi'),
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
