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
import logging
import uuid

from oslo_config import cfg
import retrying
import six
from six.moves.urllib.parse import quote
try:
    from swiftclient import client as swclient
    from swiftclient import utils as swift_utils
except ImportError:
    swclient = None

from gnocchi import storage
from gnocchi.storage import _carbonara

LOG = logging.getLogger(__name__)

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
    cfg.StrOpt('swift_key',
               secret=True,
               default="admin",
               help='Swift key/password.'),
    cfg.StrOpt('swift_tenant_name',
               help='Swift tenant name, only used in v2 auth.'),
    cfg.StrOpt('swift_container_prefix',
               default='gnocchi',
               help='Prefix to namespace metric containers.'),
    cfg.IntOpt('swift_timeout',
               min=0,
               default=300,
               help='Connection timeout in seconds.'),
]


def retry_if_result_empty(result):
    return len(result) == 0


class SwiftStorage(_carbonara.CarbonaraBasedStorage):

    POST_HEADERS = {'Accept': 'application/json', 'Content-Type': 'text/plain'}

    def __init__(self, conf):
        super(SwiftStorage, self).__init__(conf)
        if swclient is None:
            raise RuntimeError("python-swiftclient unavailable")
        self.swift = swclient.Connection(
            auth_version=conf.swift_auth_version,
            authurl=conf.swift_authurl,
            preauthtoken=conf.swift_preauthtoken,
            user=conf.swift_user,
            key=conf.swift_key,
            tenant_name=conf.swift_tenant_name,
            timeout=conf.swift_timeout,
            retries=0)
        self._container_prefix = conf.swift_container_prefix
        self.swift.put_container(self.MEASURE_PREFIX)

    def _container_name(self, metric):
        return '%s.%s' % (self._container_prefix, str(metric.id))

    @staticmethod
    def _object_name(split_key, aggregation, granularity):
        return '%s_%s_%s' % (split_key, aggregation, granularity)

    def _create_metric(self, metric):
        # TODO(jd) A container per user in their account?
        resp = {}
        self.swift.put_container(self._container_name(metric),
                                 response_dict=resp)
        # put_container() should return 201 Created; if it returns 204, that
        # means the metric was already created!
        if resp['status'] == 204:
            raise storage.MetricAlreadyExists(metric)

    def _store_measures(self, metric, data):
        now = datetime.datetime.utcnow().strftime("_%Y%m%d_%H:%M:%S")
        self.swift.put_object(
            self.MEASURE_PREFIX,
            six.text_type(metric.id) + "/" + six.text_type(uuid.uuid4()) + now,
            data)

    def _build_report(self, details):
        headers, files = self.swift.get_container(self.MEASURE_PREFIX,
                                                  delimiter='/',
                                                  full_listing=True)
        metrics = len(files)
        measures = int(headers.get('x-container-object-count'))
        metric_details = defaultdict(int)
        if details:
            headers, files = self.swift.get_container(self.MEASURE_PREFIX,
                                                      full_listing=True)
            for f in files:
                metric = f['name'].split('/', 1)[0]
                metric_details[metric] += 1
        return metrics, measures, metric_details if details else None

    def _list_metric_with_measures_to_process(self, block_size, full=False):
        limit = None
        if not full:
            limit = block_size * (self.partition + 1)
        headers, files = self.swift.get_container(self.MEASURE_PREFIX,
                                                  delimiter='/',
                                                  full_listing=full,
                                                  limit=limit)
        if not full:
            files = files[block_size * self.partition:]
        return set(f['subdir'][:-1] for f in files if 'subdir' in f)

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
        headers, body = self.swift.post_account(
            headers=self.POST_HEADERS, query_string='bulk-delete',
            data=b''.join(obj.encode('utf-8') + b'\n' for obj in objects))
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
            measures.extend(self._unserialize_measures(data))

        yield measures

        # Now clean objects
        self._bulk_delete(self.MEASURE_PREFIX, files)

    def _store_metric_measures(self, metric, timestamp_key,
                               aggregation, granularity, data):
        self.swift.put_object(
            self._container_name(metric),
            self._object_name(timestamp_key, aggregation, granularity),
            data)

    def _delete_metric_measures(self, metric, timestamp_key, aggregation,
                                granularity):
        self.swift.delete_object(
            self._container_name(metric),
            self._object_name(timestamp_key, aggregation, granularity))

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

    @retrying.retry(stop_max_attempt_number=4,
                    wait_fixed=500,
                    retry_on_result=retry_if_result_empty)
    def _get_measures(self, metric, timestamp_key, aggregation, granularity):
        try:
            headers, contents = self.swift.get_object(
                self._container_name(metric), self._object_name(
                    timestamp_key, aggregation, granularity))
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

    def _list_split_keys_for_metric(self, metric, aggregation, granularity):
        container = self._container_name(metric)
        try:
            headers, files = self.swift.get_container(
                container, full_listing=True)
        except swclient.ClientException as e:
            if e.http_status == 404:
                raise storage.MetricDoesNotExist(metric)
            raise
        keys = []
        for f in files:
            try:
                key, agg, g = f['name'].split('_', 2)
            except ValueError:
                # Might be "none", or any other file. Be resilient.
                continue
            if aggregation == agg and granularity == float(g):
                keys.append(key)
        return keys

    @retrying.retry(stop_max_attempt_number=4,
                    wait_fixed=500,
                    retry_on_result=retry_if_result_empty)
    def _get_unaggregated_timeserie(self, metric):
        try:
            headers, contents = self.swift.get_object(
                self._container_name(metric), "none")
        except swclient.ClientException as e:
            if e.http_status == 404:
                raise storage.MetricDoesNotExist(metric)
            raise
        return contents

    def _store_unaggregated_timeserie(self, metric, data):
        self.swift.put_object(self._container_name(metric), "none", data)

    def _delete_unaggregated_timeserie(self, metric):
        try:
            self.swift.delete_object(self._container_name(metric), "none")
        except swclient.ClientException as e:
            if e.http_status != 404:
                raise

    # The following methods deal with Gnocchi <= 1.3 archives
    def _get_metric_archive(self, metric, aggregation):
        """Retrieve data in the place we used to store TimeSerieArchive."""
        try:
            headers, contents = self.swift.get_object(
                self._container_name(metric), aggregation)
        except swclient.ClientException as e:
            if e.http_status == 404:
                raise storage.AggregationDoesNotExist(metric, aggregation)
            raise
        return contents

    def _store_metric_archive(self, metric, aggregation, data):
        """Stores data in the place we used to store TimeSerieArchive."""
        self.swift.put_object(self._container_name(metric), aggregation, data)

    def _delete_metric_archives(self, metric):
        for aggregation in metric.archive_policy.aggregation_methods:
            try:
                self.swift.delete_object(self._container_name(metric),
                                         aggregation)
            except swclient.ClientException as e:
                if e.http_status != 404:
                    raise
