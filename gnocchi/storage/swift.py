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
from collections import defaultdict
import contextlib
import datetime
import itertools
import uuid

from oslo_config import cfg
from oslo_log import log
import six
from six.moves.urllib.parse import quote
try:
    from swiftclient import client as swclient
    from swiftclient import utils as swift_utils
except ImportError:
    swclient = None

from gnocchi import storage
from gnocchi.storage import _carbonara
from gnocchi import utils

LOG = log.getLogger(__name__)

OPTS = [
    cfg.StrOpt('swift_auth_version',
               default='1',
               help='Swift authentication version to user.'),
    cfg.StrOpt('swift_preauthurl',
               help='Swift pre-auth URL.'),
    cfg.StrOpt('swift_authurl',
               default="http://localhost:8080/auth/v1.0",
               help='Swift auth URL.'),
    cfg.StrOpt('swift_preauthtoken',
               secret=True,
               help='Swift token to user to authenticate.'),
    cfg.StrOpt('swift_user',
               default="admin:admin",
               help='Swift user.'),
    cfg.StrOpt('swift_user_domain_name',
               default='Default',
               help='Swift user domain name.'),
    cfg.StrOpt('swift_key',
               secret=True,
               default="admin",
               help='Swift key/password.'),
    cfg.StrOpt('swift_project_name',
               help='Swift tenant name, only used in v2/v3 auth.',
               deprecated_name="swift_tenant_name"),
    cfg.StrOpt('swift_project_domain_name',
               default='Default',
               help='Swift project domain name.'),
    cfg.StrOpt('swift_container_prefix',
               default='gnocchi',
               help='Prefix to namespace metric containers.'),
    cfg.StrOpt('swift_endpoint_type',
               default='publicURL',
               help='Endpoint type to connect to Swift',),
    cfg.IntOpt('swift_timeout',
               min=0,
               default=300,
               help='Connection timeout in seconds.'),
]


class SwiftStorage(_carbonara.CarbonaraBasedStorage):

    WRITE_FULL = True
    POST_HEADERS = {'Accept': 'application/json', 'Content-Type': 'text/plain'}

    def __init__(self, conf):
        super(SwiftStorage, self).__init__(conf)
        if swclient is None:
            raise RuntimeError("python-swiftclient unavailable")
        self.swift = self._get_connection(conf)
        self._container_prefix = conf.swift_container_prefix
        self.swift.put_container(self.MEASURE_PREFIX)

    @utils.retry
    def _get_connection(self, conf):
        return swclient.Connection(
            auth_version=conf.swift_auth_version,
            authurl=conf.swift_authurl,
            preauthtoken=conf.swift_preauthtoken,
            user=conf.swift_user,
            key=conf.swift_key,
            tenant_name=conf.swift_project_name,
            timeout=conf.swift_timeout,
            os_options={'endpoint_type': conf.swift_endpoint_type,
                        'user_domain_name': conf.swift_user_domain_name},
            retries=0)

    def _container_name(self, metric):
        return '%s.%s' % (self._container_prefix, str(metric.id))

    @staticmethod
    def _object_name(split_key, aggregation, granularity, version=3):
        name = '%s_%s_%s' % (split_key, aggregation, granularity)
        return name + '_v%s' % version if version else name

    def _create_metric(self, metric):
        # TODO(jd) A container per user in their account?
        resp = {}
        self.swift.put_container(self._container_name(metric),
                                 response_dict=resp)
        # put_container() should return 201 Created; if it returns 204, that
        # means the metric was already created!
        if resp['status'] == 204:
            raise storage.MetricAlreadyExists(metric)

    def _store_new_measures(self, metric, data):
        now = datetime.datetime.utcnow().strftime("_%Y%m%d_%H:%M:%S")
        self.swift.put_object(
            self.MEASURE_PREFIX,
            six.text_type(metric.id) + "/" + six.text_type(uuid.uuid4()) + now,
            data)

    def _build_report(self, details):
        metric_details = defaultdict(int)
        if details:
            headers, files = self.swift.get_container(self.MEASURE_PREFIX,
                                                      full_listing=True)
            metrics = set()
            for f in files:
                metric, metric_files = f['name'].split("/", 1)
                metric_details[metric] += 1
                metrics.add(metric)
            nb_metrics = len(metrics)
        else:
            headers, files = self.swift.get_container(self.MEASURE_PREFIX,
                                                      delimiter='/',
                                                      full_listing=True)
            nb_metrics = len(files)
        measures = int(headers.get('x-container-object-count'))
        return nb_metrics, measures, metric_details if details else None

    def _list_metric_with_measures_to_process(self):
        headers, files = self.swift.get_container(self.MEASURE_PREFIX,
                                                  full_listing=True)
        names = sorted(f['name'].split("/", 1)[0] for f in files)
        return ((metric, len(list(measures)))
                for metric, measures in itertools.groupby(names))

    def _list_measure_files_for_metric_id(self, metric_id):
        headers, files = self.swift.get_container(
            self.MEASURE_PREFIX, path=six.text_type(metric_id),
            full_listing=True)
        return files

    def _pending_measures_to_process_count(self, metric_id):
        return len(self._list_measure_files_for_metric_id(metric_id))

    def _bulk_delete(self, container, objects):
        objects = [quote(('/%s/%s' % (container, obj['name'])).encode('utf-8'))
                   for obj in objects]
        resp = {}
        headers, body = self.swift.post_account(
            headers=self.POST_HEADERS, query_string='bulk-delete',
            data=b''.join(obj.encode('utf-8') + b'\n' for obj in objects),
            response_dict=resp)
        if resp['status'] != 200:
            raise storage.StorageError(
                "Unable to bulk-delete, is bulk-delete enabled in Swift?")
        resp = swift_utils.parse_api_response(headers, body)
        LOG.debug('# of objects deleted: %s, # of objects skipped: %s',
                  resp['Number Deleted'], resp['Number Not Found'])

    def _delete_unprocessed_measures_for_metric_id(self, metric_id):
        files = self._list_measure_files_for_metric_id(metric_id)
        self._bulk_delete(self.MEASURE_PREFIX, files)

    @contextlib.contextmanager
    def _process_measure_for_metric(self, metric):
        files = self._list_measure_files_for_metric_id(metric.id)

        measures = []
        for f in files:
            headers, data = self.swift.get_object(
                self.MEASURE_PREFIX, f['name'])
            measures.extend(self._unserialize_measures(f['name'], data))

        yield measures

        # Now clean objects
        self._bulk_delete(self.MEASURE_PREFIX, files)

    def _store_metric_measures(self, metric, timestamp_key, aggregation,
                               granularity, data, offset=None, version=3):
        self.swift.put_object(
            self._container_name(metric),
            self._object_name(timestamp_key, aggregation, granularity,
                              version),
            data)

    def _delete_metric_measures(self, metric, timestamp_key, aggregation,
                                granularity, version=3):
        self.swift.delete_object(
            self._container_name(metric),
            self._object_name(timestamp_key, aggregation, granularity,
                              version))

    def _delete_metric(self, metric):
        self._delete_unaggregated_timeserie(metric)
        container = self._container_name(metric)
        try:
            headers, files = self.swift.get_container(
                container, full_listing=True)
        except swclient.ClientException as e:
            if e.http_status != 404:
                # Maybe it never has been created (no measure)
                raise
        else:
            self._bulk_delete(container, files)
            try:
                self.swift.delete_container(container)
            except swclient.ClientException as e:
                if e.http_status != 404:
                    # Deleted in the meantime? Whatever.
                    raise

    def _get_measures(self, metric, timestamp_key, aggregation, granularity,
                      version=3):
        try:
            headers, contents = self.swift.get_object(
                self._container_name(metric), self._object_name(
                    timestamp_key, aggregation, granularity, version))
        except swclient.ClientException as e:
            if e.http_status == 404:
                try:
                    self.swift.head_container(self._container_name(metric))
                except swclient.ClientException as e:
                    if e.http_status == 404:
                        raise storage.MetricDoesNotExist(metric)
                    raise
                raise storage.AggregationDoesNotExist(metric, aggregation)
            raise
        return contents

    def _list_split_keys_for_metric(self, metric, aggregation, granularity,
                                    version=None):
        container = self._container_name(metric)
        try:
            headers, files = self.swift.get_container(
                container, full_listing=True)
        except swclient.ClientException as e:
            if e.http_status == 404:
                raise storage.MetricDoesNotExist(metric)
            raise
        keys = set()
        for f in files:
            try:
                meta = f['name'].split('_')
                if (aggregation == meta[1] and granularity == float(meta[2])
                        and self._version_check(f['name'], version)):
                    keys.add(meta[0])
            except (ValueError, IndexError):
                # Might be "none", or any other file. Be resilient.
                continue
        return keys

    @staticmethod
    def _build_unaggregated_timeserie_path(version):
        return 'none' + ("_v%s" % version if version else "")

    def _get_unaggregated_timeserie(self, metric, version=3):
        try:
            headers, contents = self.swift.get_object(
                self._container_name(metric),
                self._build_unaggregated_timeserie_path(version))
        except swclient.ClientException as e:
            if e.http_status == 404:
                raise storage.MetricDoesNotExist(metric)
            raise
        return contents

    def _store_unaggregated_timeserie(self, metric, data, version=3):
        self.swift.put_object(self._container_name(metric),
                              self._build_unaggregated_timeserie_path(version),
                              data)

    def _delete_unaggregated_timeserie(self, metric, version=3):
        try:
            self.swift.delete_object(
                self._container_name(metric),
                self._build_unaggregated_timeserie_path(version))
        except swclient.ClientException as e:
            if e.http_status != 404:
                raise
