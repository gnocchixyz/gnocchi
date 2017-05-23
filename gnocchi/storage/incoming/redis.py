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

    @staticmethod
    def remove_sack_group(num_sacks):
        # NOTE(gordc): redis doesn't maintain keys with empty values
        pass

    def _build_measure_path(self, metric_id):
        return redis.SEP.join([
            self.get_sack_name(self.sack_for_metric(metric_id)),
            six.text_type(metric_id)])

    def add_measures_batch(self, metrics_and_measures):
        pipe = self._client.pipeline(transaction=False)
        for metric, measures in six.iteritems(metrics_and_measures):
            path = self._build_measure_path(metric.id)
            pipe.rpush(path, self._encode_measures(measures))
        pipe.execute()

    def _build_report(self, details):
        report_vars = {'measures': 0, 'metric_details': {}}

        def update_report(results, m_list):
            report_vars['measures'] += sum(results)
            if details:
                report_vars['metric_details'].update(
                    dict(six.moves.zip(m_list, results)))

        match = redis.SEP.join([self.get_sack_name("*"), "*"])
        metrics = 0
        m_list = []
        pipe = self._client.pipeline()
        for key in self._client.scan_iter(match=match, count=1000):
            metrics += 1
            pipe.llen(key)
            if details:
                m_list.append(key.decode('utf8').split(redis.SEP)[1])
            # group 100 commands/call
            if metrics % 100 == 0:
                results = pipe.execute()
                update_report(results, m_list)
                m_list = []
                pipe = self._client.pipeline()
        else:
            results = pipe.execute()
            update_report(results, m_list)
        return (metrics, report_vars['measures'],
                report_vars['metric_details'] if details else None)

    def list_metric_with_measures_to_process(self, sack):
        match = redis.SEP.join([self.get_sack_name(sack), "*"])
        keys = self._client.scan_iter(match=match, count=1000)
        return set([k.decode('utf8').split(redis.SEP)[1] for k in keys])

    def delete_unprocessed_measures_for_metric_id(self, metric_id):
        self._client.delete(self._build_measure_path(metric_id))

    def has_unprocessed(self, metric):
        return bool(self._client.exists(self._build_measure_path(metric.id)))

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
