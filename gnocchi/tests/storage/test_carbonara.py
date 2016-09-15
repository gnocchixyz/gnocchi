# -*- encoding: utf-8 -*-
#
# Copyright © 2015 Red Hat, Inc.
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
            # NOTE(jd) This is just to have an unaggregated timserie for
            # the upgrade code, I don't think the values are correct lol
            ts = carbonara.BoundTimeSerie(
                block_size=self.metric.archive_policy.max_block_size,
                back_window=self.metric.archive_policy.back_window)
            ts.set_values([
                storage.Measure(
                    datetime.datetime(2016, 7, 17, 23, 59, 0), 23),
            ])
            self.storage._store_unaggregated_timeserie(self.metric,
                                                       ts.serialize())

            for d, agg in itertools.product(
                    self.metric.archive_policy.definition, ['mean', 'max']):

                # NOTE: there is a split at 2016-07-18 on granularity 300
                ts = carbonara.TimeSerie.from_data(
                    [datetime.datetime(2016, 7, 17, 23, 59, 0),
                     datetime.datetime(2016, 7, 17, 23, 59, 4),
                     datetime.datetime(2016, 7, 17, 23, 59, 9),
                     datetime.datetime(2016, 7, 18, 0, 0, 0),
                     datetime.datetime(2016, 7, 18, 0, 0, 4),
                     datetime.datetime(2016, 7, 18, 0, 0, 9)],
                    [4, 5, 6, 7, 8, 9])
                grouped = ts.group_serie(d.granularity)
                ts = carbonara.AggregatedTimeSerie.from_grouped_serie(
                    grouped, d.granularity, agg, max_size=d.points)

                for key, split in ts.split():
                    self.storage._store_metric_measures(
                        self.metric,
                        str(key),
                        agg, d.granularity,
                        _serialize_v2(split), offset=None, version=None)

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

    def test_get_measures_upgrade_limit(self):
        self.metric2 = storage.Metric(uuid.uuid4(),
                                      self.archive_policies['low'])
        self.storage._create_metric(self.metric2)

        # serialise in old format
        with mock.patch('gnocchi.carbonara.SplitKey.POINTS_PER_SPLIT',
                        14400):

            # NOTE(jd) This is just to have an unaggregated timserie for
            # the upgrade code, I don't think the values are correct lol
            ts = carbonara.BoundTimeSerie(
                block_size=self.metric2.archive_policy.max_block_size,
                back_window=self.metric2.archive_policy.back_window)
            ts.set_values([
                storage.Measure(
                    datetime.datetime(2016, 7, 17, 23, 59, 0), 23),
            ])
            self.storage._store_unaggregated_timeserie(self.metric2,
                                                       ts.serialize())

            for d, agg in itertools.product(
                    self.metric2.archive_policy.definition, ['mean', 'max']):

                # NOTE: there is a split at 2016-07-18 on granularity 300
                ts = carbonara.TimeSerie.from_data(
                    [datetime.datetime(2016, 7, 17, 23, 59, 0),
                     datetime.datetime(2016, 7, 17, 23, 59, 4),
                     datetime.datetime(2016, 7, 17, 23, 59, 9),
                     datetime.datetime(2016, 7, 18, 0, 0, 0),
                     datetime.datetime(2016, 7, 18, 0, 0, 4),
                     datetime.datetime(2016, 7, 18, 0, 0, 9)],
                    [4, 5, 6, 7, 8, 9])
                grouped = ts.group_serie(d.granularity)
                ts = carbonara.AggregatedTimeSerie.from_grouped_serie(
                    grouped, d.granularity, agg, max_size=d.points)

                for key, split in ts.split():
                    self.storage._store_metric_measures(
                        self.metric2, str(key), agg, d.granularity,
                        _serialize_v2(split), offset=0, version=None)

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
            ], self.storage.get_measures(self.metric2))

        with mock.patch.object(self.index, 'list_metrics') as f:
            f.side_effect = [[self.metric], [self.metric2], []]
            with mock.patch.object(self.storage, 'UPGRADE_BATCH_SIZE', 1):
                self.storage.upgrade(self.index)

        self.assertEqual([
            (utils.datetime_utc(2016, 7, 17), 86400, 5),
            (utils.datetime_utc(2016, 7, 18), 86400, 8),
            (utils.datetime_utc(2016, 7, 17, 23), 3600, 5),
            (utils.datetime_utc(2016, 7, 18, 0), 3600, 8),
            (utils.datetime_utc(2016, 7, 17, 23, 55), 300, 5),
            (utils.datetime_utc(2016, 7, 18, 0), 300, 8)
        ], self.storage.get_measures(self.metric2))

    def test_delete_metric_not_upgraded(self):
        # Make sure that we delete everything (e.g. objects + container)
        # correctly even if the metric has not been upgraded.
        self.storage.delete_metric(self.metric)
        self.assertEqual([], self.storage.get_measures(self.metric))
