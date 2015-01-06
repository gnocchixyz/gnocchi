# -*- encoding: utf-8 -*-
#
# Copyright © 2014 eNovance
#
# Authors: Julien Danjou <julien@danjou.info>
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

import testscenarios

from gnocchi import storage
from gnocchi.storage import null
from gnocchi.tests import base as tests_base

load_tests = testscenarios.load_tests_apply_scenarios


class TestStorageDriver(tests_base.TestCase):
    def test_get_driver(self):
        self.conf.set_override('driver', 'null', 'storage')
        driver = storage.get_driver(self.conf)
        self.assertIsInstance(driver, null.NullStorage)

    def test_create_metric(self):
        self.storage.create_metric("foo", self.archive_policies['low'])

    def test_create_metric_already_exists(self):
        self.storage.create_metric("foo", self.archive_policies['low'])
        self.assertRaises(storage.MetricAlreadyExists,
                          self.storage.create_metric,
                          "foo", self.archive_policies['low'])

    def test_delete_empty_metric(self):
        self.storage.create_metric("foo", self.archive_policies['low'])
        self.storage.delete_metric("foo")

    def test_delete_nonempty_metric(self):
        self.storage.create_metric("foo", self.archive_policies['low'])
        self.storage.add_measures('foo', [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
        ])
        self.storage.delete_metric("foo")

    def test_add_and_get_measures(self):
        self.storage.create_metric("foo", self.archive_policies['low'])
        self.storage.add_measures('foo', [
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
        ], self.storage.get_measures('foo'))

        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 12, 10), 300.0, 44.0),
        ], self.storage.get_measures(
            'foo',
            from_timestamp=datetime.datetime(2014, 1, 1, 12, 10, 0)))

        self.assertEqual([
            (datetime.datetime(2014, 1, 1), 86400.0, 39.75),
            (datetime.datetime(2014, 1, 1, 12), 3600.0, 39.75),
            (datetime.datetime(2014, 1, 1, 12), 300.0, 69.0),
            (datetime.datetime(2014, 1, 1, 12, 5), 300.0, 23.0),
        ], self.storage.get_measures(
            'foo',
            to_timestamp=datetime.datetime(2014, 1, 1, 12, 6, 0)))

        self.assertEqual(
            [],
            self.storage.get_measures(
                'foo',
                to_timestamp=datetime.datetime(2014, 1, 1, 12, 10, 10),
                from_timestamp=datetime.datetime(2014, 1, 1, 12, 10, 10)))

        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 12), 3600.0, 39.75),
            (datetime.datetime(2014, 1, 1, 12), 300.0, 69.0),
        ], self.storage.get_measures(
            'foo',
            from_timestamp=datetime.datetime(2014, 1, 1, 12, 0, 0),
            to_timestamp=datetime.datetime(2014, 1, 1, 12, 0, 2)))

    def test_get_measure_unknown_metric(self):
        self.assertRaises(storage.MetricDoesNotExist,
                          self.storage.get_measures,
                          'foo', 0)

    def test_get_cross_metric_measures_unknown_metric(self):
        self.assertRaises(storage.MetricDoesNotExist,
                          self.storage.get_cross_metric_measures,
                          ['foo', 'bar'])

    def test_add_and_get_cross_metric_measures_different_archives(self):
        self.storage.create_metric("foo", self.archive_policies['low'])
        self.storage.create_metric(
            "bar", self.archive_policies['no_granularity_match'])
        self.assertRaises(storage.MetricUnaggregatable,
                          self.storage.get_cross_metric_measures,
                          ['foo', 'bar'])

    def test_add_and_get_cross_metric_measures(self):
        self.storage.create_metric("foo", self.archive_policies['low'])
        self.storage.create_metric("bar", self.archive_policies['low'])
        self.storage.add_measures('foo', [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 7, 31), 42),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 9, 31), 4),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 12, 45), 44),
        ])
        self.storage.add_measures('bar', [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 5), 9),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 7, 41), 2),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 10, 31), 4),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 13, 10), 4),
        ])

        values = self.storage.get_cross_metric_measures(['foo', 'bar'])
        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 0, 0, 0), 86400.0, 22.25),
            (datetime.datetime(2014, 1, 1, 12, 0, 0), 3600.0, 22.25),
            (datetime.datetime(2014, 1, 1, 12, 0, 0), 300.0, 39.0),
            (datetime.datetime(2014, 1, 1, 12, 5, 0), 300.0, 12.5),
            (datetime.datetime(2014, 1, 1, 12, 10, 0), 300.0, 24.0)
        ], values)

        values = self.storage.get_cross_metric_measures(
            ['foo', 'bar'], from_timestamp='2014-01-01 12:10:00')
        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 12, 10, 0), 300.0, 24.0)
        ], values)

        values = self.storage.get_cross_metric_measures(
            ['foo', 'bar'], to_timestamp='2014-01-01 12:05:00')

        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 0, 0, 0), 86400.0, 22.25),
            (datetime.datetime(2014, 1, 1, 12, 0, 0), 3600.0, 22.25),
            (datetime.datetime(2014, 1, 1, 12, 0, 0), 300.0, 39.0),
        ], values)

        values = self.storage.get_cross_metric_measures(
            ['foo', 'bar'],
            to_timestamp='2014-01-01 12:10:10',
            from_timestamp='2014-01-01 12:10:10')
        self.assertEqual([], values)

        values = self.storage.get_cross_metric_measures(
            ['foo', 'bar'],
            from_timestamp='2014-01-01 12:00:00',
            to_timestamp='2014-01-01 12:00:01')

        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 12, 0, 0), 3600.0, 22.25),
            (datetime.datetime(2014, 1, 1, 12, 0, 0), 300.0, 39.0),
        ], values)

    def test_add_and_get_cross_metric_measures_with_holes(self):
        self.storage.create_metric("foo", self.archive_policies['low'])
        self.storage.create_metric("bar", self.archive_policies['low'])
        self.storage.add_measures('foo', [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 7, 31), 42),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 5, 31), 8),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 9, 31), 4),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 12, 45), 42),
        ])
        self.storage.add_measures('bar', [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 5), 9),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 7, 31), 2),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 9, 31), 6),
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 13, 10), 2),
        ])

        values = self.storage.get_cross_metric_measures(['foo', 'bar'])
        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 0, 0, 0), 86400.0, 18.875),
            (datetime.datetime(2014, 1, 1, 12, 0, 0), 3600.0, 18.875),
            (datetime.datetime(2014, 1, 1, 12, 0, 0), 300.0, 39.0),
            (datetime.datetime(2014, 1, 1, 12, 5, 0), 300.0, 11.0),
            (datetime.datetime(2014, 1, 1, 12, 10, 0), 300.0, 22.0)
        ], values)
