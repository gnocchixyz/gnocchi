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
import contextlib
import datetime
import uuid

from oslo_config import cfg
import retrying
import six
from swiftclient import client as swclient

from gnocchi import storage
from gnocchi.storage import _carbonara


OPTS = [
    cfg.StrOpt('swift_auth_version',
               default='1',
               help='Swift authentication version to user.'),
    cfg.StrOpt('swift_preauthurl',
               default=None,
               help='Swift pre-auth URL.'),
    cfg.StrOpt('swift_authurl',
               default="http://localhost:8080/auth/v1.0",
               help='Swift auth URL.'),
    cfg.StrOpt('swift_preauthtoken',
               secret=True,
               default=None,
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
    def __init__(self, conf):
        super(SwiftStorage, self).__init__(conf)
        self.swift = swclient.Connection(
            auth_version=conf.swift_auth_version,
            authurl=conf.swift_authurl,
            preauthtoken=conf.swift_preauthtoken,
            user=conf.swift_user,
            key=conf.swift_key,
            tenant_name=conf.swift_tenant_name,
            timeout=conf.swift_timeout)
        self._lock = _carbonara.CarbonaraBasedStorageToozLock(conf)
        self._container_prefix = conf.swift_container_prefix
        self.swift.put_container(self.MEASURE_PREFIX)

    def stop(self):
        self._lock.stop()

    def _container_name(self, metric):
        return '%s.%s' % (self._container_prefix, str(metric.id))

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
        now = datetime.datetime.utcnow().strftime("_%Y%M%d_%H:%M:%S")
        self.swift.put_object(
            self.MEASURE_PREFIX,
            six.text_type(metric.id) + "/" + six.text_type(uuid.uuid4()) + now,
            data)

    def _list_metric_with_measures_to_process(self):
        headers, files = self.swift.get_container(self.MEASURE_PREFIX,
                                                  delimiter='/')
        return set(f['subdir'][:-1] for f in files if 'subdir' in f)

    def _list_measure_files_for_metric_id(self, metric_id):
        headers, files = self.swift.get_container(
            self.MEASURE_PREFIX, path=six.text_type(metric_id))
        return files

    def _pending_measures_to_process_count(self, metric_id):
        return len(self._list_measure_files_for_metric_id(metric_id))

    def _delete_unprocessed_measures_for_metric_id(self, metric_id):
        files = self._list_measure_files_for_metric(metric_id)
        for f in files:
            self.swift.delete_object(self.MEASURE_PREFIX, f['name'])

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
        for f in files:
            self.swift.delete_object(self.MEASURE_PREFIX, f['name'])

    def _store_metric_measures(self, metric, aggregation, data):
        self.swift.put_object(self._container_name(metric), aggregation, data)

    def _delete_metric(self, metric):
        self._delete_unaggregated_timeserie(metric)
        for aggregation in metric.archive_policy.aggregation_methods:
            try:
                self.swift.delete_object(self._container_name(metric),
                                         aggregation)
            except swclient.ClientException as e:
                if e.http_status != 404:
                    raise
        try:
            self.swift.delete_container(self._container_name(metric))
        except swclient.ClientException as e:
            if e.http_status != 404:
                # Maybe it never has been created (no measure)
                raise

    @retrying.retry(stop_max_attempt_number=4,
                    wait_fixed=500,
                    retry_on_result=retry_if_result_empty)
    def _get_measures(self, metric, aggregation):
        try:
            headers, contents = self.swift.get_object(
                self._container_name(metric), aggregation)
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

    def _delete_unaggregated_timeserie(self, metric):
        try:
            self.swift.delete_object(self._container_name(metric), "none")
        except swclient.ClientException as e:
            if e.http_status != 404:
                raise
