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

import six

from gnocchi.common import redis
from gnocchi import storage
from gnocchi import utils


class RedisStorage(storage.StorageDriver):
    WRITE_FULL = True

    STORAGE_PREFIX = b"timeseries"
    FIELD_SEP = '_'

    _SCRIPTS = {
        "list_split_keys": """
local metric_key = KEYS[1]
local ids = {}
local cursor = 0
local substring = "([^" .. ARGV[2] .. "]*)"
repeat
    local result = redis.call("HSCAN", metric_key, cursor, "MATCH", ARGV[1])
    cursor = tonumber(result[1])
    for i, v in ipairs(result[2]) do
        -- Only return keys, not values
        if i % 2 ~= 0 then
            ids[#ids + 1] = v:gmatch(substring)()
        end
    end
until cursor == 0
if #ids == 0 and redis.call("EXISTS", metric_key) == 0 then
    return -1
end
return ids
""",
    }

    def __init__(self, conf):
        super(RedisStorage, self).__init__(conf)
        self._client = redis.get_client(conf)
        self._scripts = {
            name: self._client.register_script(code)
            for name, code in six.iteritems(self._SCRIPTS)
        }

    def __str__(self):
        return "%s: %s" % (self.__class__.__name__, self._client)

    def _metric_key(self, metric):
        return redis.SEP.join([self.STORAGE_PREFIX, str(metric.id).encode()])

    @staticmethod
    def _unaggregated_field(version=3):
        return 'none' + ("_v%s" % version if version else "")

    @classmethod
    def _aggregated_field_for_split(cls, aggregation, key, version=3,
                                    granularity=None):
        path = cls.FIELD_SEP.join([
            str(key), aggregation,
            str(utils.timespan_total_seconds(granularity or key.sampling))])
        return path + '_v%s' % version if version else path

    def _create_metric(self, metric):
        key = self._metric_key(metric)
        if self._client.exists(key):
            raise storage.MetricAlreadyExists(metric)
        self._client.hset(key, self._unaggregated_field(), '')

    def _store_unaggregated_timeserie(self, metric, data, version=3):
        self._client.hset(self._metric_key(metric),
                          self._unaggregated_field(version), data)

    def _get_unaggregated_timeserie(self, metric, version=3):
        data = self._client.hget(self._metric_key(metric),
                                 self._unaggregated_field(version))
        if data is None:
            raise storage.MetricDoesNotExist(metric)
        return data

    def _list_split_keys(self, metric, aggregation, granularity, version=3):
        key = self._metric_key(metric)
        split_keys = self._scripts["list_split_keys"](
            keys=[key], args=[self._aggregated_field_for_split(
                aggregation, '*', version, granularity), self.FIELD_SEP])
        if split_keys == -1:
            raise storage.MetricDoesNotExist(metric)
        return set(split_keys)

    def _delete_metric_measures(self, metric, key, aggregation, version=3):
        field = self._aggregated_field_for_split(aggregation, key, version)
        self._client.hdel(self._metric_key(metric), field)

    def _store_metric_measures(self, metric, key, aggregation,
                               data, offset=None, version=3):
        field = self._aggregated_field_for_split(
            aggregation, key, version)
        self._client.hset(self._metric_key(metric), field, data)

    def _delete_metric(self, metric):
        self._client.delete(self._metric_key(metric))

    def _get_measures(self, metric, keys, aggregation, version=3):
        if not keys:
            return []
        redis_key = self._metric_key(metric)
        fields = [
            self._aggregated_field_for_split(aggregation, key, version)
            for key in keys
        ]
        results = self._client.hmget(redis_key, fields)
        for key, data in six.moves.zip(keys, results):
            if data is None:
                if not self._client.exists(redis_key):
                    raise storage.MetricDoesNotExist(metric)
                raise storage.AggregationDoesNotExist(
                    metric, aggregation, key.sampling)
        return results
