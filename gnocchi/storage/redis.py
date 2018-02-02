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

from gnocchi.common import redis
from gnocchi import storage
from gnocchi import utils


class RedisStorage(storage.StorageDriver):
    WRITE_FULL = True

    STORAGE_PREFIX = "timeseries"

    _SCRIPTS = {
        "list_split_keys": """
local metric_key = KEYS[1]
local ids = {}
local cursor = 0
local substring = "[^%s]*%s[^%s]*%s([^%s]*)"
repeat
    local result = redis.call("SCAN", cursor, "MATCH", ARGV[1])
    cursor = tonumber(result[1])
    for i, v in ipairs(result[2]) do
        ids[#ids + 1] = v:gmatch(substring)()
    end
until cursor == 0
if #ids == 0 and redis.call("EXISTS", metric_key) == 0 then
    return -1
end
return ids
""" % (redis.SEP, redis.SEP, redis.SEP, redis.SEP, redis.SEP),
        "get_measures": """
local results = redis.call("MGET", unpack(ARGV))
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
        "delete_metric": """
redis.replicate_commands()
local cursor = 0
local ids = {}
repeat
    local result = redis.call("SCAN", cursor, "MATCH", ARGV[1])
    cursor = tonumber(result[1])
    for i, v in ipairs(result[2]) do
        ids[#ids + 1] = v
    end
until cursor == 0
redis.call("DEL", KEYS[1], unpack(ids))
"""
    }

    def __init__(self, conf):
        super(RedisStorage, self).__init__(conf)
        self._client, self._scripts = redis.get_client(conf, self._SCRIPTS)

    def __str__(self):
        return "%s: %s" % (self.__class__.__name__, self._client)

    def _metric_key(self, metric):
        return redis.SEP.join([self.STORAGE_PREFIX, str(metric.id)])

    def _aggregated_field_for_split(self, metric_id, aggregation, key,
                                    version=3, granularity=None):
        path = redis.SEP.join([
            self.STORAGE_PREFIX,
            str(metric_id), str(key), aggregation,
            str(utils.timespan_total_seconds(granularity or key.sampling))])
        return path + redis.SEP + 'v%s' % version if version else path

    def _create_metric(self, metric):
        if self._client.setnx(self._metric_key(metric), "") == 0:
            raise storage.MetricAlreadyExists(metric)

    def _store_unaggregated_timeserie(self, metric, data, version=3):
        self._client.set(self._metric_key(metric), data)

    def _get_unaggregated_timeserie(self, metric, version=3):
        data = self._client.get(self._metric_key(metric))
        if data is None:
            raise storage.MetricDoesNotExist(metric)
        return data

    def _list_split_keys(self, metric, aggregation, granularity, version=3):
        key = self._metric_key(metric)
        split_keys = self._scripts["list_split_keys"](
            keys=[key], args=[self._aggregated_field_for_split(
                metric.id,
                aggregation, '*', version, granularity)])
        if split_keys == -1:
            raise storage.MetricDoesNotExist(metric)
        return set(split_keys)

    def _delete_metric_measures(self, metric, key, aggregation, version=3):
        self._client.delete(self._aggregated_field_for_split(
            metric.id, aggregation, key, version))

    def _store_metric_measures(self, metric, key, aggregation,
                               data, offset=None, version=3):
        self._client.set(self._aggregated_field_for_split(
            metric.id, aggregation, key, version), data)

    def _delete_metric(self, metric):
        self._scripts['delete_metric'](
            keys=[self._metric_key(metric)],
            args=[redis.SEP.join([self.STORAGE_PREFIX, str(metric.id),
                                  "*", "*", "v3"])],
        )

    def _get_measures(self, metric, keys, aggregation, version=3):
        if not keys:
            return []
        fields = [
            self._aggregated_field_for_split(
                metric.id, aggregation, key, version)
            for key in keys
        ]
        code, result = self._scripts['get_measures'](
            keys=[self._metric_key(metric)],
            args=fields,
        )
        if code == -1:
            sampling = utils.to_timespan(result.split(redis.SEP_B)[4])
            raise storage.AggregationDoesNotExist(
                metric, aggregation, sampling)
        if code == -2:
            raise storage.MetricDoesNotExist(metric)
        return result
