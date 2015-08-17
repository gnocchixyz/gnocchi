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

from gnocchi import storage
from gnocchi.storage import null
from gnocchi.tests import base as tests_base
from gnocchi import utils


class TestStorageDriver(tests_base.TestCase):
    def setUp(self):
        super(TestStorageDriver, self).setUp()
        # A lot of tests wants a metric, create one
        self.metric = storage.Metric(uuid.uuid4(),
                                     self.archive_policies['low'])

    def test_get_driver(self):
        self.conf.set_override('driver', 'null', 'storage')
        driver = storage.get_driver(self.conf)
        self.assertIsInstance(driver, null.NullStorage)

    def test_delete_nonempty_metric(self):
        self.storage.add_measures(self.metric, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
        ])
        with mock.patch.object(self.index, 'get_metrics') as f:
            f.return_value = [self.metric]
            self.storage.process_measures(self.index)
        self.storage.delete_metric(self.metric)

    def test_delete_nonempty_metric_unprocessed(self):
        self.storage.add_measures(self.metric, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
        ])
        self.storage.delete_metric(self.metric)

    def test_delete_nonempty_metric_with_process(self):
        self.storage.add_measures(self.metric, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
        ])
        with mock.patch.object(self.index, 'get_metrics') as f:
            f.return_value = [self.metric]
            self.storage.process_measures(self.index)
        self.storage.delete_metric(self.metric)

    def test_add_and_get_measures(self):
        self.storage.add_measures(self.metric, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 7, 31), 42),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 9, 31), 4),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 12, 45), 44),
        ])
        with mock.patch.object(self.index, 'get_metrics') as f:
            f.return_value = [self.metric]
            self.storage.process_measures(self.index)

        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1), 86400.0, 39.75),
            (utils.datetime_utc(2014, 1, 1, 12), 3600.0, 39.75),
            (utils.datetime_utc(2014, 1, 1, 12), 300.0, 69.0),
            (utils.datetime_utc(2014, 1, 1, 12, 5), 300.0, 23.0),
            (utils.datetime_utc(2014, 1, 1, 12, 10), 300.0, 44.0),
        ], self.storage.get_measures(self.metric))

        self.assertEqual([
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

        self.assertEqual(
            [],
            self.storage.get_measures(
                self.metric,
                to_timestamp=datetime.datetime(2014, 1, 1, 12, 10, 10),
                from_timestamp=datetime.datetime(2014, 1, 1, 12, 10, 10)))

        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1, 12), 3600.0, 39.75),
            (utils.datetime_utc(2014, 1, 1, 12), 300.0, 69.0),
        ], self.storage.get_measures(
            self.metric,
            from_timestamp=datetime.datetime(2014, 1, 1, 12, 0, 0),
            to_timestamp=datetime.datetime(2014, 1, 1, 12, 0, 2)))

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
        metric2 = storage.Metric(uuid.uuid4(),
                                 self.archive_policies['low'])
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
        with mock.patch.object(self.index, 'get_metrics') as f:
            f.return_value = [self.metric, metric2]
            self.storage.process_measures(self.index)

        values = self.storage.get_cross_metric_measures([self.metric, metric2])
        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1, 0, 0, 0), 86400.0, 22.25),
            (utils.datetime_utc(2014, 1, 1, 12, 0, 0), 3600.0, 22.25),
            (utils.datetime_utc(2014, 1, 1, 12, 0, 0), 300.0, 39.0),
            (utils.datetime_utc(2014, 1, 1, 12, 5, 0), 300.0, 12.5),
            (utils.datetime_utc(2014, 1, 1, 12, 10, 0), 300.0, 24.0)
        ], values)

        values = self.storage.get_cross_metric_measures(
            [self.metric, metric2], from_timestamp='2014-01-01 12:10:00')
        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1, 12, 10, 0), 300.0, 24.0)
        ], values)

        values = self.storage.get_cross_metric_measures(
            [self.metric, metric2], to_timestamp='2014-01-01 12:05:00')

        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1, 0, 0, 0), 86400.0, 22.25),
            (utils.datetime_utc(2014, 1, 1, 12, 0, 0), 3600.0, 22.25),
            (utils.datetime_utc(2014, 1, 1, 12, 0, 0), 300.0, 39.0),
        ], values)

        values = self.storage.get_cross_metric_measures(
            [self.metric, metric2],
            to_timestamp='2014-01-01 12:10:10',
            from_timestamp='2014-01-01 12:10:10')
        self.assertEqual([], values)

        values = self.storage.get_cross_metric_measures(
            [self.metric, metric2],
            from_timestamp='2014-01-01 12:00:00',
            to_timestamp='2014-01-01 12:00:01')

        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1, 12, 0, 0), 3600.0, 22.25),
            (utils.datetime_utc(2014, 1, 1, 12, 0, 0), 300.0, 39.0),
        ], values)

    def test_add_and_get_cross_metric_measures_with_holes(self):
        metric2 = storage.Metric(uuid.uuid4(),
                                 self.archive_policies['low'])
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
        with mock.patch.object(self.index, 'get_metrics') as f:
            f.return_value = [self.metric, metric2]
            self.storage.process_measures(self.index)

        values = self.storage.get_cross_metric_measures([self.metric, metric2])
        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1, 0, 0, 0), 86400.0, 18.875),
            (utils.datetime_utc(2014, 1, 1, 12, 0, 0), 3600.0, 18.875),
            (utils.datetime_utc(2014, 1, 1, 12, 0, 0), 300.0, 39.0),
            (utils.datetime_utc(2014, 1, 1, 12, 5, 0), 300.0, 11.0),
            (utils.datetime_utc(2014, 1, 1, 12, 10, 0), 300.0, 22.0)
        ], values)

    def test_search_value(self):
        metric2 = storage.Metric(uuid.uuid4(),
                                 self.archive_policies['low'])
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
        with mock.patch.object(self.index, 'get_metrics') as f:
            f.return_value = [self.metric, metric2]
            self.storage.process_measures(self.index)

        self.assertEqual(
            {metric2: [],
             self.metric: [(utils.datetime_utc(2014, 1, 1, 12), 300, 69)]},
            self.storage.search_value(
                [metric2, self.metric],
                {u"≥": 50}))

        self.assertEqual(
            {metric2: [], self.metric: []},
            self.storage.search_value(
                [metric2, self.metric],
                {u"∧": [
                    {u"eq": 100},
                    {u"≠": 50}]}))
