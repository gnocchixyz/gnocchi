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

    def test_create_entity(self):
        self.storage.create_entity("foo", self.archive_policies['low'])

    def test_create_entity_already_exists(self):
        self.storage.create_entity("foo", self.archive_policies['low'])
        self.assertRaises(storage.EntityAlreadyExists,
                          self.storage.create_entity,
                          "foo", self.archive_policies['low'])

    def test_delete_empty_entity(self):
        self.storage.create_entity("foo", self.archive_policies['low'])
        self.storage.delete_entity("foo")

    def test_delete_nonempty_entity(self):
        self.storage.create_entity("foo", self.archive_policies['low'])
        self.storage.add_measures('foo', [
            storage.Measure(datetime.datetime(2014, 1, 1, 12, 0, 1), 69),
        ])
        self.storage.delete_entity("foo")

    def test_add_and_get_measures(self):
        self.storage.create_entity("foo", self.archive_policies['low'])
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

    def test_get_measure_unknown_entity(self):
        self.assertRaises(storage.EntityDoesNotExist,
                          self.storage.get_measures,
                          'foo', 0)
