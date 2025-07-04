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
import sys
import uuid

from oslo_config import cfg

import gnocchi.archive_policy
import gnocchi.common.redis
import gnocchi.indexer
import gnocchi.rest.http_proxy_to_wsgi
import gnocchi.storage
import gnocchi.storage.ceph
import gnocchi.storage.file
import gnocchi.storage.s3
import gnocchi.storage.swift

if sys.version_info < (3, 10, 0):
    import importlib_metadata
else:
    from importlib import metadata as importlib_metadata


_STORAGE_OPTS = list(itertools.chain(gnocchi.storage.OPTS,
                                     gnocchi.storage.ceph.OPTS,
                                     gnocchi.storage.file.OPTS,
                                     gnocchi.storage.swift.OPTS,
                                     gnocchi.common.redis.OPTS,
                                     gnocchi.storage.s3.OPTS))


_INCOMING_OPTS = copy.deepcopy(_STORAGE_OPTS)
for opt in _INCOMING_OPTS:
    opt.default = '${storage.%s}' % opt.name

API_OPTS = (
    cfg.HostAddressOpt('host',
                       default="0.0.0.0",
                       help="Host to listen on"),
    cfg.PortOpt('port',
                default=8041,
                help="Port to listen on"),
    cfg.StrOpt('uwsgi-mode',
               default='http',
               choices=["http", "http-socket", "socket"],
               help="""Socket type to use for uWSGI:
* http: support HTTP/1.1 and keepalive,
  but not chunked encoding (InfluxDB)
* http-socket/socket: support chunked encoding, but require a upstream HTTP
  Server for HTTP/1.1, keepalive and HTTP protocol correctness.
""")
)


_cli_options = (
    cfg.BoolOpt(
        'debug',
        short='d',
        default=False,
        help='If set to true, the logging level will be set to DEBUG.'),
    cfg.BoolOpt(
        'verbose',
        short='v',
        default=True,
        help='If set to true, the logging level will be set to INFO.'),
    cfg.StrOpt(
        "log-dir",
        help="Base directory for log files. "
        "If not set, logging will go to stderr."),
    cfg.StrOpt(
        'log-file',
        metavar='PATH',
        help='(Optional) Name of log file to send logging output to. '
        'If no default is set, logging will go to stderr as '
        'defined by use_stderr.'),
)


def list_opts():
    return [
        ("DEFAULT", _cli_options + (
            cfg.StrOpt(
                'coordination_url',
                secret=True,
                deprecated_group="storage",
                help='Coordination driver URL'),
            cfg.IntOpt(
                'parallel_operations',
                min=1,
                deprecated_name='aggregation_workers_number',
                deprecated_group='storage',
                help='Number of threads to use to parallelize '
                'some operations. '
                'Default is set to the number of CPU available.'),
            cfg.BoolOpt(
                'use-syslog',
                default=False,
                help='Use syslog for logging.'),
            cfg.BoolOpt(
                'use-journal',
                default=False,
                help='Enable journald for logging. '
                'If running in a systemd environment you may wish '
                'to enable journal support. Doing so will use the '
                'journal native protocol which includes structured '
                'metadata in addition to log messages.'),
            cfg.StrOpt(
                'syslog-log-facility',
                default='user',
                help='Syslog facility to receive log lines.')
        )),
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
            cfg.BoolOpt(
                'greedy', default=True,
                help="Allow to bypass `metric_processing_delay` if metricd "
                "is notified that measures are ready to be processed."
            ),
            cfg.IntOpt('metric_reporting_delay',
                       deprecated_group='storage',
                       default=120,
                       min=-1,
                       help="How many seconds to wait between "
                       "metric ingestion reporting. Set value to -1 to "
                       "disable reporting"),
            cfg.IntOpt('metric_cleanup_delay',
                       deprecated_group='storage',
                       default=300,
                       help="How many seconds to wait between "
                       "cleaning of expired data"),
            cfg.IntOpt('processing_replicas',
                       default=3,
                       min=1,
                       help="Number of workers that share a task. A higher "
                       "value may improve worker utilization but may also "
                       "increase load on coordination backend. Value is "
                       "capped by number of workers globally."),
            cfg.IntOpt('cleanup_batch_size',
                       default=10000,
                       min=1,
                       help="Number of metrics that should be deleted "
                            "simultaneously by one janitor."),
            cfg.IntOpt('metric_inactive_after',
                       default=0,
                       help="Number of seconds to wait before we consider a "
                            "metric inactive. An inactive metric is a metric "
                            "that has not received new measurements for a "
                            "given period. If all metrics of a resource are "
                            "inactive, we mark the resource with the "
                            "'ended_at' timestamp. The default is 0 (zero), "
                            "which means that we never execute process.")
        )),
        ("api", (
            cfg.StrOpt('paste_config',
                       default="api-paste.ini",
                       help='Path to API Paste configuration.'),
            cfg.StrOpt(
                'auth_mode',
                default="basic",
                choices=list(map(
                    operator.attrgetter("name"),
                    importlib_metadata.entry_points(
                        group='gnocchi.rest.auth_helper'))),
                help='Authentication mode to use.'),
            cfg.IntOpt('max_limit',
                       default=1000,
                       help=('The maximum number of items returned in a '
                             'single response from a collection resource')),
            cfg.IntOpt('operation_timeout',
                       deprecated_name="refresh_timeout",
                       default=10, min=0,
                       help='Number of seconds before timeout when attempting '
                            'to do some operations.'),
            cfg.StrOpt('uwsgi_path',
                       default=None,
                       help="Custom UWSGI path to avoid auto discovery of packages.")
        ) + API_OPTS + gnocchi.rest.http_proxy_to_wsgi.OPTS,
        ),
        ("storage", _STORAGE_OPTS),
        ("incoming", _INCOMING_OPTS),
        ("statsd", (
            cfg.HostAddressOpt('host',
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
                'creator',
                help='Creator value to use to identify statsd in Gnocchi'),
            cfg.StrOpt(
                'archive_policy_name',
                help='Archive policy name to use when creating metrics'),
            cfg.FloatOpt(
                'flush_delay',
                default=10,
                help='Delay between flushes'),
        )),
        ("amqp1d", (
            cfg.StrOpt('url',
                       default='localhost:5672/u/collectd/telemetry',
                       help='AMQP 1.0 URL to listen to'),
            cfg.StrOpt('data_source',
                       default='collectd',
                       choices=['collectd'],
                       help='Data source for amqp1d'),
            cfg.StrOpt('resource_type',
                       default='collectd_amqp1d',
                       help='Resource type name to use to identify metrics'),
            cfg.StrOpt('creator', help='Creator value to use to amqpd1'),
            cfg.FloatOpt('flush_delay',
                         default=10,
                         help='Delay between flushes in seconds'),
        )),
        ("archive_policy", gnocchi.archive_policy.OPTS),
    ]


def set_defaults():
    from oslo_middleware import cors
    cfg.set_defaults(cors.CORS_OPTS,
                     allow_headers=[
                         'Authorization',
                         'X-Auth-Token',
                         'X-Subject-Token',
                         'X-User-Id',
                         'X-Domain-Id',
                         'X-Project-Id',
                         'X-Roles'])
