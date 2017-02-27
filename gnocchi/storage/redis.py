# -*- encoding: utf-8 -*-
#
# Copyright © 2017 Red Hat
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
import os

from oslo_config import cfg

from gnocchi import storage
from gnocchi.storage import _carbonara
from gnocchi.storage.common import redis


OPTS = [
    cfg.StrOpt('redis_url',
               default='redis://localhost:6379/',
               help='Redis URL'),
]


class RedisStorage(_carbonara.CarbonaraBasedStorage):
    WRITE_FULL = True

    STORAGE_PREFIX = "timeseries"

    def __init__(self, conf, incoming):
        super(RedisStorage, self).__init__(conf, incoming)
        self._client = redis.get_client(conf)

    def _build_metric_dir(self, metric):
        return os.path.join(self.STORAGE_PREFIX, str(metric.id))

    def _build_unaggregated_timeserie_path(self, metric, version=3):
        return os.path.join(
            self._build_metric_dir(metric),
            'none' + ("_v%s" % version if version else ""))

    def _build_metric_path(self, metric, aggregation):
        return os.path.join(self._build_metric_dir(metric),
                            "agg_" + aggregation)

    def _build_metric_path_for_split(self, metric, aggregation,
                                     timestamp_key, granularity, version=3):
        path = os.path.join(self._build_metric_path(metric, aggregation),
                            timestamp_key + "_" + str(granularity))
        return path + '_v%s' % version if version else path

    def _create_metric(self, metric):
        path = self._build_metric_dir(metric)
        ret = self._client.set(path.encode("utf-8"), "created", nx=True)
        if ret is None:
            raise storage.MetricAlreadyExists(metric)

    def _store_unaggregated_timeserie(self, metric, data, version=3):
        path = self._build_unaggregated_timeserie_path(metric, version)
        self._client.set(path.encode("utf8"), data)

    def _get_unaggregated_timeserie(self, metric, version=3):
        path = self._build_unaggregated_timeserie_path(metric, version)
        data = self._client.get(path.encode("utf8"))
        if data is None:
            raise storage.MetricDoesNotExist(metric)
        return data

    def _delete_unaggregated_timeserie(self, metric, version=3):
        path = self._build_unaggregated_timeserie_path(metric, version)
        data = self._client.get(path.encode("utf8"))
        if data is None:
            raise storage.MetricDoesNotExist(metric)
        self._client.delete(path.encode("utf8"))

    def _list_split_keys_for_metric(self, metric, aggregation, granularity,
                                    version=None):
        path = self._build_metric_dir(metric)
        if self._client.get(path.encode("utf8")) is None:
            raise storage.MetricDoesNotExist(metric)
        match = os.path.join(self._build_metric_path(metric, aggregation),
                             "*")
        split_keys = set()
        for key in self._client.scan_iter(match=match.encode("utf8")):
            key = key.decode("utf8")
            key = key.split(os.path.sep)[-1]
            meta = key.split("_")
            if meta[1] == str(granularity) and self._version_check(key,
                                                                   version):
                split_keys.add(meta[0])
        return split_keys

    def _delete_metric_measures(self, metric, timestamp_key, aggregation,
                                granularity, version=3):
        path = self._build_metric_path_for_split(
            metric, aggregation, timestamp_key, granularity, version)
        self._client.delete(path.encode("utf8"))

    def _store_metric_measures(self, metric, timestamp_key, aggregation,
                               granularity, data, offset=None, version=3):
        path = self._build_metric_path_for_split(metric, aggregation,
                                                 timestamp_key, granularity,
                                                 version)
        self._client.set(path.encode("utf8"), data)

    def _delete_metric(self, metric):
        path = self._build_metric_dir(metric)
        self._client.delete(path.encode("utf8"))

    # Carbonara API

    def _get_measures(self, metric, timestamp_key, aggregation, granularity,
                      version=3):
        path = self._build_metric_path_for_split(
            metric, aggregation, timestamp_key, granularity, version)
        data = self._client.get(path.encode("utf8"))
        if data is None:
            fpath = self._build_metric_dir(metric)
            if self._client.get(fpath.encode("utf8")) is None:
                raise storage.MetricDoesNotExist(metric)
            raise storage.AggregationDoesNotExist(metric, aggregation)
        return data
