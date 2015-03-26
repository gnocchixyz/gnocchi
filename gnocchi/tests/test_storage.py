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

from gnocchi import archive_policy
from gnocchi import storage
from gnocchi.storage import null
from gnocchi.tests import base as tests_base


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

    def test_create_metric(self):
        self.storage.create_metric(self.metric)

    def test_create_metric_already_exists(self):
        self.storage.create_metric(self.metric)
        self.assertRaises(storage.MetricAlreadyExists,
                          self.storage.create_metric, self.metric)

    def test_create_metric_already_exists_new_aggregation_methods_change(self):
        # NOTE(sileht): this is not really possible currently throught the API
        # but we even check that storage driver doesn't use the
        # aggregation_methods to check the presence or not of a meter,
        # just in case of the API change to not have to rewrite driver just
        # for that.
        # NOTE(jd) Copy the archive policy, DO NOT MODIFY THE GLOBAL ONE!
        self.metric.archive_policy = archive_policy.ArchivePolicy(
            self.metric.archive_policy.name,
            self.metric.archive_policy.back_window,
            self.metric.archive_policy.definition)
        self.metric.archive_policy.aggregation_methods = ['mean']
        self.storage.create_metric(self.metric)
        self.metric.archive_policy.aggregation_methods = ['sum']
        self.assertRaises(storage.MetricAlreadyExists,
                          self.storage.create_metric,
                          self.metric)

    def test_delete_inexistent_metric(self):
        self.assertRaises(storage.MetricDoesNotExist,
                          self.storage.delete_metric,
                          self.metric)

    def test_delete_empty_metric(self):
        self.storage.create_metric(self.metric)
        self.storage.delete_metric(self.metric)

    def test_delete_nonempty_metric(self):
        self.storage.create_metric(self.metric)
        self.storage.add_measures(self.metric, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
        ])
        self.storage.delete_metric(self.metric)

    def test_add_and_get_measures(self):
        self.storage.create_metric(self.metric)
        self.storage.add_measures(self.metric, [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 7, 31), 42),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 9, 31), 4),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 12, 45), 44),
        ])

        self.assertEqual([
            (datetime.datetime(2014, 1, 1), 86400.0, 39.75),
            (datetime.datetime(2014, 1, 1, 12), 3600.0, 39.75),
            (datetime.datetime(2014, 1, 1, 12), 300.0, 69.0),
            (datetime.datetime(2014, 1, 1, 12, 5), 300.0, 23.0),
            (datetime.datetime(2014, 1, 1, 12, 10), 300.0, 44.0),
        ], self.storage.get_measures(self.metric))

        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 12, 10), 300.0, 44.0),
        ], self.storage.get_measures(
            self.metric,
            from_timestamp=datetime.datetime(2014, 1, 1, 12, 10, 0)))

        self.assertEqual([
            (datetime.datetime(2014, 1, 1), 86400.0, 39.75),
            (datetime.datetime(2014, 1, 1, 12), 3600.0, 39.75),
            (datetime.datetime(2014, 1, 1, 12), 300.0, 69.0),
            (datetime.datetime(2014, 1, 1, 12, 5), 300.0, 23.0),
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
            (datetime.datetime(2014, 1, 1, 12), 3600.0, 39.75),
            (datetime.datetime(2014, 1, 1, 12), 300.0, 69.0),
        ], self.storage.get_measures(
            self.metric,
            from_timestamp=datetime.datetime(2014, 1, 1, 12, 0, 0),
            to_timestamp=datetime.datetime(2014, 1, 1, 12, 0, 2)))

    def test_get_measure_unknown_metric(self):
        self.assertRaises(storage.MetricDoesNotExist,
                          self.storage.get_measures,
                          self.metric)

    def test_get_cross_metric_measures_unknown_metric(self):
        self.assertRaises(storage.MetricDoesNotExist,
                          self.storage.get_cross_metric_measures,
                          [storage.Metric(uuid.uuid4(), None),
                           storage.Metric(uuid.uuid4(), None)])

    def test_get_measure_unknown_aggregation(self):
        self.storage.create_metric(self.metric)
        self.assertRaises(storage.AggregationDoesNotExist,
                          self.storage.get_measures,
                          self.metric, aggregation='last')

    def test_get_cross_metric_measures_unknown_aggregation(self):
        metric2 = storage.Metric(uuid.uuid4(),
                                 self.archive_policies['low'])
        self.storage.create_metric(self.metric)
        self.storage.create_metric(metric2)
        self.assertRaises(storage.AggregationDoesNotExist,
                          self.storage.get_cross_metric_measures,
                          [self.metric, metric2],
                          aggregation='last')

    def test_add_and_get_cross_metric_measures_different_archives(self):
        metric2 = storage.Metric(uuid.uuid4(),
                                 self.archive_policies['no_granularity_match'])
        self.storage.create_metric(self.metric)
        self.storage.create_metric(metric2)
        self.assertRaises(storage.MetricUnaggregatable,
                          self.storage.get_cross_metric_measures,
                          [self.metric, metric2])

    def test_add_and_get_cross_metric_measures(self):
        metric2 = storage.Metric(uuid.uuid4(),
                                 self.archive_policies['low'])
        self.storage.create_metric(self.metric)
        self.storage.create_metric(metric2)
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

        values = self.storage.get_cross_metric_measures([self.metric, metric2])
        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 0, 0, 0), 86400.0, 22.25),
            (datetime.datetime(2014, 1, 1, 12, 0, 0), 3600.0, 22.25),
            (datetime.datetime(2014, 1, 1, 12, 0, 0), 300.0, 39.0),
            (datetime.datetime(2014, 1, 1, 12, 5, 0), 300.0, 12.5),
            (datetime.datetime(2014, 1, 1, 12, 10, 0), 300.0, 24.0)
        ], values)

        values = self.storage.get_cross_metric_measures(
            [self.metric, metric2], from_timestamp='2014-01-01 12:10:00')
        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 12, 10, 0), 300.0, 24.0)
        ], values)

        values = self.storage.get_cross_metric_measures(
            [self.metric, metric2], to_timestamp='2014-01-01 12:05:00')

        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 0, 0, 0), 86400.0, 22.25),
            (datetime.datetime(2014, 1, 1, 12, 0, 0), 3600.0, 22.25),
            (datetime.datetime(2014, 1, 1, 12, 0, 0), 300.0, 39.0),
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
            (datetime.datetime(2014, 1, 1, 12, 0, 0), 3600.0, 22.25),
            (datetime.datetime(2014, 1, 1, 12, 0, 0), 300.0, 39.0),
        ], values)

    def test_add_and_get_cross_metric_measures_with_holes(self):
        metric2 = storage.Metric(uuid.uuid4(),
                                 self.archive_policies['low'])
        self.storage.create_metric(self.metric)
        self.storage.create_metric(metric2)
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

        values = self.storage.get_cross_metric_measures([self.metric, metric2])
        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 0, 0, 0), 86400.0, 18.875),
            (datetime.datetime(2014, 1, 1, 12, 0, 0), 3600.0, 18.875),
            (datetime.datetime(2014, 1, 1, 12, 0, 0), 300.0, 39.0),
            (datetime.datetime(2014, 1, 1, 12, 5, 0), 300.0, 11.0),
            (datetime.datetime(2014, 1, 1, 12, 10, 0), 300.0, 22.0)
        ], values)

    def test_search_value(self):
        metric2 = storage.Metric(uuid.uuid4(),
                                 self.archive_policies['low'])
        self.storage.create_metric(self.metric)
        self.storage.create_metric(metric2)
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

        self.assertEqual(
            {metric2: [],
             self.metric: [(datetime.datetime(2014, 1, 1, 12), 300, 69)]},
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
