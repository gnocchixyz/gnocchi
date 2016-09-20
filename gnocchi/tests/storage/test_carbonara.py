# -*- encoding: utf-8 -*-
#
# Copyright © 2015-2016 Red Hat, Inc.
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
import datetime
import itertools
import uuid

import mock
import msgpack
import six

from gnocchi import carbonara
from gnocchi import storage
from gnocchi.storage import _carbonara
from gnocchi.tests import base as tests_base
from gnocchi import utils


def _serialize_v2(split):
    d = {'values': dict((timestamp.value, float(v))
                        for timestamp, v
                        in six.iteritems(split.ts.dropna()))}
    return msgpack.dumps(d)


class TestCarbonaraMigration(tests_base.TestCase):
    def setUp(self):
        super(TestCarbonaraMigration, self).setUp()
        if not isinstance(self.storage, _carbonara.CarbonaraBasedStorage):
            self.skipTest("This driver is not based on Carbonara")

        self.metric = storage.Metric(uuid.uuid4(),
                                     self.archive_policies['low'])

        self.storage._create_metric(self.metric)

        with mock.patch('gnocchi.carbonara.SplitKey.'
                        'POINTS_PER_SPLIT', 14400):
            bts = carbonara.BoundTimeSerie(
                block_size=self.metric.archive_policy.max_block_size,
                back_window=self.metric.archive_policy.back_window)
            # NOTE: there is a split at 2016-07-18 on granularity 300
            values = ((datetime.datetime(2016, 7, 17, 23, 59, 0), 4),
                      (datetime.datetime(2016, 7, 17, 23, 59, 4), 5),
                      (datetime.datetime(2016, 7, 17, 23, 59, 9), 6),
                      (datetime.datetime(2016, 7, 18, 0, 0, 0), 7),
                      (datetime.datetime(2016, 7, 18, 0, 0, 4), 8),
                      (datetime.datetime(2016, 7, 18, 0, 0, 9), 9))

            def _before_truncate(bound_timeserie):
                for d, agg in itertools.product(
                        self.metric.archive_policy.definition,
                        ['mean', 'max']):
                    grouped = bound_timeserie.group_serie(
                        d.granularity, carbonara.round_timestamp(
                            bound_timeserie.first, d.granularity * 10e8))

                    aggts = carbonara.AggregatedTimeSerie.from_grouped_serie(
                        grouped, d.granularity, agg, max_size=d.points)

                    for key, split in aggts.split():
                        self.storage._store_metric_measures(
                            self.metric,
                            str(key),
                            agg, d.granularity,
                            _serialize_v2(split), offset=None, version=None)

            bts.set_values(values, before_truncate_callback=_before_truncate)
            self.storage._store_unaggregated_timeserie(self.metric,
                                                       _serialize_v2(bts),
                                                       version=None)

    def upgrade(self):
        with mock.patch.object(self.index, 'list_metrics') as f:
            f.side_effect = [[self.metric], []]
            self.storage.upgrade(self.index)

    def test_get_measures(self):
        with mock.patch.object(
                self.storage, '_get_measures_and_unserialize',
                side_effect=self.storage._get_measures_and_unserialize_v2):
            self.assertEqual([
                (utils.datetime_utc(2016, 7, 17), 86400, 5),
                (utils.datetime_utc(2016, 7, 18), 86400, 8),
                (utils.datetime_utc(2016, 7, 17, 23), 3600, 5),
                (utils.datetime_utc(2016, 7, 18, 0), 3600, 8),
                (utils.datetime_utc(2016, 7, 17, 23, 55), 300, 5),
                (utils.datetime_utc(2016, 7, 18, 0), 300, 8)
            ], self.storage.get_measures(self.metric))

            self.assertEqual([
                (utils.datetime_utc(2016, 7, 17), 86400, 6),
                (utils.datetime_utc(2016, 7, 18), 86400, 9),
                (utils.datetime_utc(2016, 7, 17, 23), 3600, 6),
                (utils.datetime_utc(2016, 7, 18, 0), 3600, 9),
                (utils.datetime_utc(2016, 7, 17, 23, 55), 300, 6),
                (utils.datetime_utc(2016, 7, 18, 0), 300, 9)
            ], self.storage.get_measures(self.metric, aggregation='max'))

        self.upgrade()

        self.assertEqual([
            (utils.datetime_utc(2016, 7, 17), 86400, 5),
            (utils.datetime_utc(2016, 7, 18), 86400, 8),
            (utils.datetime_utc(2016, 7, 17, 23), 3600, 5),
            (utils.datetime_utc(2016, 7, 18, 0), 3600, 8),
            (utils.datetime_utc(2016, 7, 17, 23, 55), 300, 5),
            (utils.datetime_utc(2016, 7, 18, 0), 300, 8)
        ], self.storage.get_measures(self.metric))

        self.assertEqual([
            (utils.datetime_utc(2016, 7, 17), 86400, 6),
            (utils.datetime_utc(2016, 7, 18), 86400, 9),
            (utils.datetime_utc(2016, 7, 17, 23), 3600, 6),
            (utils.datetime_utc(2016, 7, 18, 0), 3600, 9),
            (utils.datetime_utc(2016, 7, 17, 23, 55), 300, 6),
            (utils.datetime_utc(2016, 7, 18, 0), 300, 9)
        ], self.storage.get_measures(self.metric, aggregation='max'))

        with mock.patch.object(
                self.storage, '_get_measures_and_unserialize',
                side_effect=self.storage._get_measures_and_unserialize_v2):
            self.assertRaises(
                storage.AggregationDoesNotExist,
                self.storage.get_measures, self.metric)

            self.assertRaises(
                storage.AggregationDoesNotExist,
                self.storage.get_measures, self.metric, aggregation='max')

        self.storage.add_measures(self.metric, [
            storage.Measure(utils.datetime_utc(2016, 7, 18), 69),
            storage.Measure(utils.datetime_utc(2016, 7, 18, 1, 1), 64),
        ])

        with mock.patch.object(self.index, 'list_metrics') as f:
            f.side_effect = [[self.metric], []]
            self.storage.process_background_tasks(
                self.index, [str(self.metric.id)], sync=True)

        self.assertEqual([
            (utils.datetime_utc(2016, 7, 17), 86400, 6),
            (utils.datetime_utc(2016, 7, 18), 86400, 69),
            (utils.datetime_utc(2016, 7, 17, 23), 3600, 6),
            (utils.datetime_utc(2016, 7, 18, 0), 3600, 69),
            (utils.datetime_utc(2016, 7, 18, 1), 3600, 64),
            (utils.datetime_utc(2016, 7, 18, 0), 300, 69),
            (utils.datetime_utc(2016, 7, 18, 1), 300, 64)
        ], self.storage.get_measures(self.metric, aggregation='max'))

    def test_upgrade_upgraded_storage(self):
        with mock.patch.object(
                self.storage, '_get_measures_and_unserialize',
                side_effect=self.storage._get_measures_and_unserialize_v2):
            self.assertEqual([
                (utils.datetime_utc(2016, 7, 17), 86400, 5),
                (utils.datetime_utc(2016, 7, 18), 86400, 8),
                (utils.datetime_utc(2016, 7, 17, 23), 3600, 5),
                (utils.datetime_utc(2016, 7, 18, 0), 3600, 8),
                (utils.datetime_utc(2016, 7, 17, 23, 55), 300, 5),
                (utils.datetime_utc(2016, 7, 18, 0), 300, 8)
            ], self.storage.get_measures(self.metric))

            self.assertEqual([
                (utils.datetime_utc(2016, 7, 17), 86400, 6),
                (utils.datetime_utc(2016, 7, 18), 86400, 9),
                (utils.datetime_utc(2016, 7, 17, 23), 3600, 6),
                (utils.datetime_utc(2016, 7, 18, 0), 3600, 9),
                (utils.datetime_utc(2016, 7, 17, 23, 55), 300, 6),
                (utils.datetime_utc(2016, 7, 18, 0), 300, 9)
            ], self.storage.get_measures(self.metric, aggregation='max'))

        self.upgrade()
        self.upgrade()

        self.assertEqual([
            (utils.datetime_utc(2016, 7, 17), 86400, 5),
            (utils.datetime_utc(2016, 7, 18), 86400, 8),
            (utils.datetime_utc(2016, 7, 17, 23), 3600, 5),
            (utils.datetime_utc(2016, 7, 18, 0), 3600, 8),
            (utils.datetime_utc(2016, 7, 17, 23, 55), 300, 5),
            (utils.datetime_utc(2016, 7, 18, 0), 300, 8)
        ], self.storage.get_measures(self.metric))

        self.assertEqual([
            (utils.datetime_utc(2016, 7, 17), 86400, 6),
            (utils.datetime_utc(2016, 7, 18), 86400, 9),
            (utils.datetime_utc(2016, 7, 17, 23), 3600, 6),
            (utils.datetime_utc(2016, 7, 18, 0), 3600, 9),
            (utils.datetime_utc(2016, 7, 17, 23, 55), 300, 6),
            (utils.datetime_utc(2016, 7, 18, 0), 300, 9)
        ], self.storage.get_measures(self.metric, aggregation='max'))

    def test_delete_metric_not_upgraded(self):
        # Make sure that we delete everything (e.g. objects + container)
        # correctly even if the metric has not been upgraded.
        self.storage.delete_metric(self.metric)
        self.assertEqual([], self.storage.get_measures(self.metric))
