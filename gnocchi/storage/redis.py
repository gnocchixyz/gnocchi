# -*- encoding: utf-8 -*-
#
# Copyright © 2017-2018 Red Hat
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
    FIELD_SEP_B = b'_'

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
        "get_measures": """
local results = redis.call("HMGET", KEYS[1], unpack(ARGV))
local final = {}
for i, result in ipairs(results) do
    if result == false then
        local field = ARGV[i]
        if redis.call("EXISTS", KEYS[1]) == 1 then
            return {-1, field}
        end
        return {-2, field}
    end
    final[#final + 1] = result
end
return {0, final}
""",
    }

    def __init__(self, conf):
        super(RedisStorage, self).__init__(conf)
        self._client, self._scripts = redis.get_client(conf, self._SCRIPTS)

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

    def _store_unaggregated_timeserie(self, metric, data, version=3):
        self._client.hset(self._metric_key(metric),
                          self._unaggregated_field(version), data)

    def _get_or_create_unaggregated_timeseries(self, metrics, version=3):
        pipe = self._client.pipeline(transaction=False)
        for metric in metrics:
            metric_key = self._metric_key(metric)
            unagg_key = self._unaggregated_field(version)
            # Create the metric if it was not created
            pipe.hsetnx(metric_key, unagg_key, "")
            # Get the data
            pipe.hget(metric_key, unagg_key)
        ts = {
            # Replace "" by None
            metric: data or None
            for metric, (created, data)
            in six.moves.zip(metrics, utils.grouper(pipe.execute(), 2))
        }
        return ts

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

    def _store_metric_splits(self, metric, keys_and_data_and_offset,
                             aggregation, version=3):
        pipe = self._client.pipeline(transaction=False)
        metric_key = self._metric_key(metric)
        for key, data, offset in keys_and_data_and_offset:
            key = self._aggregated_field_for_split(aggregation, key, version)
            pipe.hset(metric_key, key, data)
        pipe.execute()

    def _delete_metric(self, metric):
        self._client.delete(self._metric_key(metric))

    def _get_measures(self, metric, keys, aggregation, version=3):
        if not keys:
            return []
        fields = [
            self._aggregated_field_for_split(aggregation, key, version)
            for key in keys
        ]
        code, result = self._scripts['get_measures'](
            keys=[self._metric_key(metric)],
            args=fields,
        )
        if code == -1:
            sampling = utils.to_timespan(result.split(self.FIELD_SEP_B)[2])
            raise storage.AggregationDoesNotExist(
                metric, aggregation, sampling)
        if code == -2:
            raise storage.MetricDoesNotExist(metric)
        return result
