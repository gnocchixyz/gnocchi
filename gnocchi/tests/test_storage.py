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
import datetime
import uuid

import mock
from oslo_utils import timeutils
from oslotest import base
import six.moves

from gnocchi import carbonara
from gnocchi import storage
from gnocchi.storage import _carbonara
from gnocchi.storage import null
from gnocchi.tests import base as tests_base
from gnocchi import utils


class TestStorageDriver(tests_base.TestCase):
    def setUp(self):
        super(TestStorageDriver, self).setUp()
        # A lot of tests wants a metric, create one
        self.metric, __ = self._create_metric()

    def _create_metric(self, archive_policy_name="low"):
        m = storage.Metric(uuid.uuid4(),
                           self.archive_policies[archive_policy_name])
        m_sql = self.index.create_metric(m.id, str(uuid.uuid4()),
                                         str(uuid.uuid4()),
                                         archive_policy_name)
        return m, m_sql

    def test_get_driver(self):
        self.conf.set_override('driver', 'null', 'storage')
        driver = storage.get_driver(self.conf)
        self.assertIsInstance(driver, null.NullStorage)

    @mock.patch('gnocchi.storage._carbonara.LOG')
    def test_corrupted_data(self, logger):
        if not isinstance(self.storage, _carbonara.CarbonaraBasedStorage):
            self.skipTest("This driver is not based on Carbonara")

        self.storage.add_measures(self.metric, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
        ])
        self.storage.process_background_tasks(self.index, sync=True)

        self.storage.add_measures(self.metric, [
            storage.Measure(datetime.datetime(2014, 1, 1, 13, 0, 1), 1),
        ])
        with mock.patch('gnocchi.carbonara.msgpack.unpack',
                        side_effect=ValueError("boom!")):
            with mock.patch('gnocchi.carbonara.msgpack.loads',
                            side_effect=ValueError("boom!")):
                self.storage.process_background_tasks(self.index, sync=True)

        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1), 86400.0, 1),
            (utils.datetime_utc(2014, 1, 1, 13), 3600.0, 1),
            (utils.datetime_utc(2014, 1, 1, 13), 300.0, 1),
        ], self.storage.get_measures(self.metric))

    def test_delete_nonempty_metric(self):
        self.storage.add_measures(self.metric, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
        ])
        self.storage.process_background_tasks(self.index, sync=True)
        self.storage.delete_metric(self.metric)
        self.storage.process_background_tasks(self.index, sync=True)

    def test_delete_nonempty_metric_unprocessed(self):
        self.storage.add_measures(self.metric, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
        ])
        self.storage.delete_metric(self.metric)
        self.storage.process_background_tasks(self.index, sync=True)

    def test_measures_reporting(self):
        report = self.storage.measures_report(True)
        self.assertIsInstance(report, dict)
        self.assertIn('summary', report)
        self.assertIn('metrics', report['summary'])
        self.assertIn('measures', report['summary'])
        self.assertIn('details', report)
        self.assertIsInstance(report['details'], dict)
        report = self.storage.measures_report(False)
        self.assertIsInstance(report, dict)
        self.assertIn('summary', report)
        self.assertIn('metrics', report['summary'])
        self.assertIn('measures', report['summary'])
        self.assertNotIn('details', report)

    def test_add_measures_big(self):
        m, __ = self._create_metric('high')
        self.storage.add_measures(m, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, i, j), 100)
            for i in six.moves.range(0, 60) for j in six.moves.range(0, 60)])
        self.storage.process_background_tasks(self.index, sync=True)

        self.assertEqual(3661, len(self.storage.get_measures(m)))

    @mock.patch('gnocchi.carbonara.AggregatedTimeSerie.POINTS_PER_SPLIT', 48)
    def test_add_measures_update_subset_split(self):
        m, m_sql = self._create_metric('medium')
        measures = [
            storage.Measure(datetime.datetime(2014, 1, 6, i, j, 0), 100)
            for i in six.moves.range(2) for j in six.moves.range(0, 60, 2)]
        self.storage.add_measures(m, measures)
        self.storage.process_background_tasks(self.index, sync=True)

        # add measure to end, in same aggregate time as last point.
        self.storage.add_measures(m, [
            storage.Measure(datetime.datetime(2014, 1, 6, 1, 58, 1), 100)])

        with mock.patch.object(self.storage, '_store_metric_measures') as c:
            # should only resample last aggregate
            self.storage.process_background_tasks(self.index, sync=True)
        count = 0
        for call in c.mock_calls:
            # policy is 60 points and split is 48. should only update 2nd half
            if mock.call(m_sql, mock.ANY, 'mean', 60.0, mock.ANY) == call:
                count += 1
        self.assertEqual(1, count)

    def test_add_measures_update_subset(self):
        m, m_sql = self._create_metric('medium')
        measures = [
            storage.Measure(datetime.datetime(2014, 1, 6, i, j, 0), 100)
            for i in six.moves.range(2) for j in six.moves.range(0, 60, 2)]
        self.storage.add_measures(m, measures)
        self.storage.process_background_tasks(self.index, sync=True)

        # add measure to end, in same aggregate time as last point.
        new_point = datetime.datetime(2014, 1, 6, 1, 58, 1)
        self.storage.add_measures(m, [storage.Measure(new_point, 100)])

        with mock.patch.object(self.storage, '_add_measures') as c:
            self.storage.process_background_tasks(self.index, sync=True)
        for __, args, __ in c.mock_calls:
            self.assertEqual(
                args[3].first, carbonara.TimeSerie.round_timestamp(
                    new_point, args[1].granularity * 10e8))

    def test_delete_old_measures(self):
        self.storage.add_measures(self.metric, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 7, 31), 42),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 9, 31), 4),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 12, 45), 44),
        ])
        self.storage.process_background_tasks(self.index, sync=True)

        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1), 86400.0, 39.75),
            (utils.datetime_utc(2014, 1, 1, 12), 3600.0, 39.75),
            (utils.datetime_utc(2014, 1, 1, 12), 300.0, 69.0),
            (utils.datetime_utc(2014, 1, 1, 12, 5), 300.0, 23.0),
            (utils.datetime_utc(2014, 1, 1, 12, 10), 300.0, 44.0),
        ], self.storage.get_measures(self.metric))

        # One year later…
        self.storage.add_measures(self.metric, [
            storage.Measure(datetime.datetime(2015, 1, 1, 12, 0, 1), 69),
        ])
        self.storage.process_background_tasks(self.index, sync=True)

        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1), 86400.0, 39.75),
            (utils.datetime_utc(2015, 1, 1), 86400.0, 69),
            (utils.datetime_utc(2015, 1, 1, 12), 3600.0, 69),
            (utils.datetime_utc(2015, 1, 1, 12), 300.0, 69),
        ], self.storage.get_measures(self.metric))

    def test_updated_measures(self):
        self.storage.add_measures(self.metric, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 7, 31), 42),
        ])
        self.storage.process_background_tasks(self.index, sync=True)

        self.storage.add_measures(self.metric, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 9, 31), 4),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 12, 45), 44),
        ])
        self.storage.process_background_tasks(self.index, sync=True)

        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1), 86400.0, 39.75),
            (utils.datetime_utc(2014, 1, 1, 12), 3600.0, 39.75),
            (utils.datetime_utc(2014, 1, 1, 12), 300.0, 69.0),
            (utils.datetime_utc(2014, 1, 1, 12, 5), 300.0, 23.0),
            (utils.datetime_utc(2014, 1, 1, 12, 10), 300.0, 44.0),
        ], self.storage.get_measures(self.metric))

        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1), 86400.0, 69),
            (utils.datetime_utc(2014, 1, 1, 12), 3600.0, 69.0),
            (utils.datetime_utc(2014, 1, 1, 12), 300.0, 69.0),
            (utils.datetime_utc(2014, 1, 1, 12, 5), 300.0, 42.0),
            (utils.datetime_utc(2014, 1, 1, 12, 10), 300.0, 44.0),
        ], self.storage.get_measures(self.metric, aggregation='max'))

        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1), 86400.0, 4),
            (utils.datetime_utc(2014, 1, 1, 12), 3600.0, 4),
            (utils.datetime_utc(2014, 1, 1, 12), 300.0, 69.0),
            (utils.datetime_utc(2014, 1, 1, 12, 5), 300.0, 4.0),
            (utils.datetime_utc(2014, 1, 1, 12, 10), 300.0, 44.0),
        ], self.storage.get_measures(self.metric, aggregation='min'))

    def test_add_and_get_measures(self):
        self.storage.add_measures(self.metric, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 7, 31), 42),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 9, 31), 4),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 12, 45), 44),
        ])
        self.storage.process_background_tasks(self.index, sync=True)

        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1), 86400.0, 39.75),
            (utils.datetime_utc(2014, 1, 1, 12), 3600.0, 39.75),
            (utils.datetime_utc(2014, 1, 1, 12), 300.0, 69.0),
            (utils.datetime_utc(2014, 1, 1, 12, 5), 300.0, 23.0),
            (utils.datetime_utc(2014, 1, 1, 12, 10), 300.0, 44.0),
        ], self.storage.get_measures(self.metric))

        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1), 86400.0, 39.75),
            (utils.datetime_utc(2014, 1, 1, 12), 3600.0, 39.75),
            (utils.datetime_utc(2014, 1, 1, 12, 10), 300.0, 44.0),
        ], self.storage.get_measures(
            self.metric,
            from_timestamp=datetime.datetime(2014, 1, 1, 12, 10, 0)))

        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1), 86400.0, 39.75),
            (utils.datetime_utc(2014, 1, 1, 12), 3600.0, 39.75),
            (utils.datetime_utc(2014, 1, 1, 12), 300.0, 69.0),
            (utils.datetime_utc(2014, 1, 1, 12, 5), 300.0, 23.0),
        ], self.storage.get_measures(
            self.metric,
            to_timestamp=datetime.datetime(2014, 1, 1, 12, 6, 0)))

        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1), 86400.0, 39.75),
            (utils.datetime_utc(2014, 1, 1, 12), 3600.0, 39.75),
            (utils.datetime_utc(2014, 1, 1, 12, 10), 300.0, 44.0),
        ], self.storage.get_measures(
            self.metric,
            to_timestamp=datetime.datetime(2014, 1, 1, 12, 10, 10),
            from_timestamp=datetime.datetime(2014, 1, 1, 12, 10, 10)))

        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1), 86400.0, 39.75),
            (utils.datetime_utc(2014, 1, 1, 12), 3600.0, 39.75),
            (utils.datetime_utc(2014, 1, 1, 12), 300.0, 69.0),
        ], self.storage.get_measures(
            self.metric,
            from_timestamp=datetime.datetime(2014, 1, 1, 12, 0, 0),
            to_timestamp=datetime.datetime(2014, 1, 1, 12, 0, 2)))

        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1), 86400.0, 39.75),
            (utils.datetime_utc(2014, 1, 1, 12), 3600.0, 39.75),
            (utils.datetime_utc(2014, 1, 1, 12), 300.0, 69.0),
        ], self.storage.get_measures(
            self.metric,
            from_timestamp=timeutils.parse_isotime("2014-1-1 13:00:00+01:00"),
            to_timestamp=datetime.datetime(2014, 1, 1, 12, 0, 2)))

        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1, 12), 3600.0, 39.75),
        ], self.storage.get_measures(
            self.metric,
            from_timestamp=datetime.datetime(2014, 1, 1, 12, 0, 0),
            to_timestamp=datetime.datetime(2014, 1, 1, 12, 0, 2),
            granularity=3600.0))

        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1, 12), 300.0, 69.0),
        ], self.storage.get_measures(
            self.metric,
            from_timestamp=datetime.datetime(2014, 1, 1, 12, 0, 0),
            to_timestamp=datetime.datetime(2014, 1, 1, 12, 0, 2),
            granularity=300.0))

        self.assertRaises(storage.GranularityDoesNotExist,
                          self.storage.get_measures,
                          self.metric,
                          granularity=42)

    def test_get_cross_metric_measures_unknown_metric(self):
        self.assertEqual([],
                         self.storage.get_cross_metric_measures(
                             [storage.Metric(uuid.uuid4(),
                                             self.archive_policies['low']),
                              storage.Metric(uuid.uuid4(),
                                             self.archive_policies['low'])]))

    def test_get_measure_unknown_aggregation(self):
        self.storage.add_measures(self.metric, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 7, 31), 42),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 9, 31), 4),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 12, 45), 44),
        ])
        self.assertRaises(storage.AggregationDoesNotExist,
                          self.storage.get_measures,
                          self.metric, aggregation='last')

    def test_get_cross_metric_measures_unknown_aggregation(self):
        metric2 = storage.Metric(uuid.uuid4(),
                                 self.archive_policies['low'])
        self.storage.add_measures(self.metric, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 7, 31), 42),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 9, 31), 4),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 12, 45), 44),
        ])
        self.storage.add_measures(metric2, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 7, 31), 42),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 9, 31), 4),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 12, 45), 44),
        ])
        self.assertRaises(storage.AggregationDoesNotExist,
                          self.storage.get_cross_metric_measures,
                          [self.metric, metric2],
                          aggregation='last')

    def test_get_cross_metric_measures_unknown_granularity(self):
        metric2 = storage.Metric(uuid.uuid4(),
                                 self.archive_policies['low'])
        self.storage.add_measures(self.metric, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 7, 31), 42),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 9, 31), 4),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 12, 45), 44),
        ])
        self.storage.add_measures(metric2, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 7, 31), 42),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 9, 31), 4),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 12, 45), 44),
        ])
        self.assertRaises(storage.GranularityDoesNotExist,
                          self.storage.get_cross_metric_measures,
                          [self.metric, metric2],
                          granularity=12345.456)

    def test_add_and_get_cross_metric_measures_different_archives(self):
        metric2 = storage.Metric(uuid.uuid4(),
                                 self.archive_policies['no_granularity_match'])
        self.storage.add_measures(self.metric, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 7, 31), 42),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 9, 31), 4),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 12, 45), 44),
        ])
        self.storage.add_measures(metric2, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 7, 31), 42),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 9, 31), 4),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 12, 45), 44),
        ])

        self.assertRaises(storage.MetricUnaggregatable,
                          self.storage.get_cross_metric_measures,
                          [self.metric, metric2])

    def test_add_and_get_cross_metric_measures(self):
        metric2, __ = self._create_metric()
        self.storage.add_measures(self.metric, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 7, 31), 42),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 9, 31), 4),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 12, 45), 44),
        ])
        self.storage.add_measures(metric2, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 5), 9),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 7, 41), 2),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 10, 31), 4),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 13, 10), 4),
        ])
        self.storage.process_background_tasks(self.index, sync=True)

        values = self.storage.get_cross_metric_measures([self.metric, metric2])
        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1, 0, 0, 0), 86400.0, 22.25),
            (utils.datetime_utc(2014, 1, 1, 12, 0, 0), 3600.0, 22.25),
            (utils.datetime_utc(2014, 1, 1, 12, 0, 0), 300.0, 39.0),
            (utils.datetime_utc(2014, 1, 1, 12, 5, 0), 300.0, 12.5),
            (utils.datetime_utc(2014, 1, 1, 12, 10, 0), 300.0, 24.0)
        ], values)

        values = self.storage.get_cross_metric_measures(
            [self.metric, metric2],
            from_timestamp=utils.to_timestamp('2014-01-01 12:10:00'))
        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1), 86400.0, 22.25),
            (utils.datetime_utc(2014, 1, 1, 12), 3600.0, 22.25),
            (utils.datetime_utc(2014, 1, 1, 12, 10, 0), 300.0, 24.0),
        ], values)

        values = self.storage.get_cross_metric_measures(
            [self.metric, metric2],
            to_timestamp=utils.to_timestamp('2014-01-01 12:05:00'))

        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1, 0, 0, 0), 86400.0, 22.25),
            (utils.datetime_utc(2014, 1, 1, 12, 0, 0), 3600.0, 22.25),
            (utils.datetime_utc(2014, 1, 1, 12, 0, 0), 300.0, 39.0),
        ], values)

        values = self.storage.get_cross_metric_measures(
            [self.metric, metric2],
            to_timestamp=utils.to_timestamp('2014-01-01 12:10:10'),
            from_timestamp=utils.to_timestamp('2014-01-01 12:10:10'))
        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1), 86400.0, 22.25),
            (utils.datetime_utc(2014, 1, 1, 12), 3600.0, 22.25),
            (utils.datetime_utc(2014, 1, 1, 12, 10), 300.0, 24.0),
        ], values)

        values = self.storage.get_cross_metric_measures(
            [self.metric, metric2],
            from_timestamp=utils.to_timestamp('2014-01-01 12:00:00'),
            to_timestamp=utils.to_timestamp('2014-01-01 12:00:01'))

        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1), 86400.0, 22.25),
            (utils.datetime_utc(2014, 1, 1, 12, 0, 0), 3600.0, 22.25),
            (utils.datetime_utc(2014, 1, 1, 12, 0, 0), 300.0, 39.0),
        ], values)

        values = self.storage.get_cross_metric_measures(
            [self.metric, metric2],
            from_timestamp=utils.to_timestamp('2014-01-01 12:00:00'),
            to_timestamp=utils.to_timestamp('2014-01-01 12:00:01'),
            granularity=300.0)

        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1, 12, 0, 0), 300.0, 39.0),
        ], values)

    def test_add_and_get_cross_metric_measures_with_holes(self):
        metric2, __ = self._create_metric()
        self.storage.add_measures(self.metric, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 7, 31), 42),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 5, 31), 8),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 9, 31), 4),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 12, 45), 42),
        ])
        self.storage.add_measures(metric2, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 5), 9),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 7, 31), 2),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 9, 31), 6),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 13, 10), 2),
        ])
        self.storage.process_background_tasks(self.index, sync=True)

        values = self.storage.get_cross_metric_measures([self.metric, metric2])
        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1, 0, 0, 0), 86400.0, 18.875),
            (utils.datetime_utc(2014, 1, 1, 12, 0, 0), 3600.0, 18.875),
            (utils.datetime_utc(2014, 1, 1, 12, 0, 0), 300.0, 39.0),
            (utils.datetime_utc(2014, 1, 1, 12, 5, 0), 300.0, 11.0),
            (utils.datetime_utc(2014, 1, 1, 12, 10, 0), 300.0, 22.0)
        ], values)

    def test_search_value(self):
        metric2, __ = self._create_metric()
        self.storage.add_measures(self.metric, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1,), 69),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 7, 31), 42),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 5, 31), 8),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 9, 31), 4),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 12, 45), 42),
        ])

        self.storage.add_measures(metric2, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 5), 9),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 7, 31), 2),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 9, 31), 6),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 13, 10), 2),
        ])
        self.storage.process_background_tasks(self.index, sync=True)

        self.assertEqual(
            {metric2: [],
             self.metric: [
                 (utils.datetime_utc(2014, 1, 1), 86400, 33),
                 (utils.datetime_utc(2014, 1, 1, 12), 3600, 33),
                 (utils.datetime_utc(2014, 1, 1, 12), 300, 69),
                 (utils.datetime_utc(2014, 1, 1, 12, 10), 300, 42)]},
            self.storage.search_value(
                [metric2, self.metric],
                {u"≥": 30}))

        self.assertEqual(
            {metric2: [], self.metric: []},
            self.storage.search_value(
                [metric2, self.metric],
                {u"∧": [
                    {u"eq": 100},
                    {u"≠": 50}]}))


class TestMeasureQuery(base.BaseTestCase):
    def test_equal(self):
        q = storage.MeasureQuery({"=": 4})
        self.assertTrue(q(4))
        self.assertFalse(q(40))

    def test_gt(self):
        q = storage.MeasureQuery({">": 4})
        self.assertTrue(q(40))
        self.assertFalse(q(4))

    def test_and(self):
        q = storage.MeasureQuery({"and": [{">": 4}, {"<": 10}]})
        self.assertTrue(q(5))
        self.assertFalse(q(40))
        self.assertFalse(q(1))

    def test_or(self):
        q = storage.MeasureQuery({"or": [{"=": 4}, {"=": 10}]})
        self.assertTrue(q(4))
        self.assertTrue(q(10))
        self.assertFalse(q(-1))

    def test_modulo(self):
        q = storage.MeasureQuery({"=": [{"%": 5}, 0]})
        self.assertTrue(q(5))
        self.assertTrue(q(10))
        self.assertFalse(q(-1))
        self.assertFalse(q(6))

    def test_math(self):
        q = storage.MeasureQuery(
            {
                u"and": [
                    # v+5 is bigger 0
                    {u"≥": [{u"+": 5}, 0]},
                    # v-6 is not 5
                    {u"≠": [5, {u"-": 6}]},
                ],
            }
        )
        self.assertTrue(q(5))
        self.assertTrue(q(10))
        self.assertFalse(q(11))

    def test_empty(self):
        q = storage.MeasureQuery({})
        self.assertFalse(q(5))
        self.assertFalse(q(10))

    def test_bad_format(self):
        self.assertRaises(storage.InvalidQuery,
                          storage.MeasureQuery,
                          {"foo": [{"=": 4}, {"=": 10}]})

        self.assertRaises(storage.InvalidQuery,
                          storage.MeasureQuery,
                          {"=": [1, 2, 3]})
