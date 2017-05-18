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
import collections
import contextlib

import six

from gnocchi.storage.common import redis
from gnocchi.storage.incoming import _carbonara


class RedisStorage(_carbonara.CarbonaraBasedStorage):

    def __init__(self, conf):
        super(RedisStorage, self).__init__(conf)
        self._client = redis.get_client(conf)

    def get_storage_sacks(self):
        return self._client.hget(self.CFG_PREFIX, self.CFG_SACKS)

    def set_storage_settings(self, num_sacks):
        self._client.hset(self.CFG_PREFIX, self.CFG_SACKS, num_sacks)

    def _build_measure_path(self, metric_id):
        return redis.SEP.join([
            self.get_sack_name(self.sack_for_metric(metric_id)),
            six.text_type(metric_id)])

    def _store_new_measures(self, metric, data):
        path = self._build_measure_path(metric.id)
        self._client.rpush(path, data)

    def _build_report(self, details):
        match = redis.SEP.join([self.get_sack_name("*"), "*"])
        metric_details = collections.defaultdict(int)
        for key in self._client.scan_iter(match=match, count=1000):
            metric = key.decode('utf8').split(redis.SEP)[1]
            metric_details[metric] = self._client.llen(key)
        return (len(metric_details.keys()), sum(metric_details.values()),
                metric_details if details else None)

    def list_metric_with_measures_to_process(self, sack):
        match = redis.SEP.join([self.get_sack_name(sack), "*"])
        keys = self._client.scan_iter(match=match, count=1000)
        return set([k.decode('utf8').split(redis.SEP)[1] for k in keys])

    def delete_unprocessed_measures_for_metric_id(self, metric_id):
        self._client.delete(self._build_measure_path(metric_id))

    @contextlib.contextmanager
    def process_measure_for_metric(self, metric):
        key = self._build_measure_path(metric.id)
        item_len = self._client.llen(key)
        # lrange is inclusive on both ends, decrease to grab exactly n items
        item_len = item_len - 1 if item_len else item_len
        measures = []
        for i, data in enumerate(self._client.lrange(key, 0, item_len)):
            measures.extend(self._unserialize_measures(
                '%s-%s' % (metric.id, i), data))

        yield measures

        # ltrim is inclusive, bump 1 to remove up to and including nth item
        self._client.ltrim(key, item_len + 1, -1)
