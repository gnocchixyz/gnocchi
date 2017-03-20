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
    FIELD_SEP = '_'

    def __init__(self, conf, incoming):
        super(RedisStorage, self).__init__(conf, incoming)
        self._client = redis.get_client(conf)

    def _metric_key(self, metric):
        return os.path.join(self.STORAGE_PREFIX, str(metric.id))

    @staticmethod
    def _unaggregated_field(version=3):
        return 'none' + ("_v%s" % version if version else "")

    @classmethod
    def _aggregated_field_for_split(cls, aggregation, timestamp_key,
                                    granularity, version=3):
        path = cls.FIELD_SEP.join([timestamp_key, aggregation,
                                   str(granularity)])
        return path + '_v%s' % version if version else path

    def _create_metric(self, metric):
        key = self._metric_key(metric).encode("utf8")
        if self._client.exists(key):
            raise storage.MetricAlreadyExists(metric)
        self._client.hset(
            key, self._unaggregated_field().encode("utf8"), None)

    def _store_unaggregated_timeserie(self, metric, data, version=3):
        self._client.hset(self._metric_key(metric).encode("utf8"),
                          self._unaggregated_field(version).encode("utf8"),
                          data)

    def _get_unaggregated_timeserie(self, metric, version=3):
        data = self._client.hget(
            self._metric_key(metric).encode("utf8"),
            self._unaggregated_field(version).encode("utf8"))
        if data is None:
            raise storage.MetricDoesNotExist(metric)
        return data

    def _list_split_keys_for_metric(self, metric, aggregation, granularity,
                                    version=None):
        key = self._metric_key(metric).encode("utf8")
        if not self._client.exists(key):
            raise storage.MetricDoesNotExist(metric)
        split_keys = set()
        # FIXME(gordc): version shouldn't be None but it's not used anywhere
        hashes = self._client.hscan_iter(
            key, match=self._aggregated_field_for_split(aggregation, '*',
                                                        granularity, 3))
        for f, __ in hashes:
            meta = f.decode("utf8").split(self.FIELD_SEP, 1)
            split_keys.add(meta[0])
        return split_keys

    def _delete_metric_measures(self, metric, timestamp_key, aggregation,
                                granularity, version=3):
        key = self._metric_key(metric)
        field = self._aggregated_field_for_split(
            aggregation, timestamp_key, granularity, version)
        self._client.hdel(key.encode("utf8"), field.encode("utf8"))

    def _store_metric_measures(self, metric, timestamp_key, aggregation,
                               granularity, data, offset=None, version=3):
        key = self._metric_key(metric)
        field = self._aggregated_field_for_split(
            aggregation, timestamp_key, granularity, version)
        self._client.hset(key.encode("utf8"), field.encode("utf8"), data)

    def _delete_metric(self, metric):
        self._client.delete(self._metric_key(metric).encode("utf8"))

    # Carbonara API

    def _get_measures(self, metric, timestamp_key, aggregation, granularity,
                      version=3):
        key = self._metric_key(metric)
        field = self._aggregated_field_for_split(
            aggregation, timestamp_key, granularity, version)
        data = self._client.hget(key.encode("utf8"), field.encode("utf8"))
        if data is None:
            if not self._client.exists(key.encode("utf8")):
                raise storage.MetricDoesNotExist(metric)
            raise storage.AggregationDoesNotExist(metric, aggregation)
        return data
