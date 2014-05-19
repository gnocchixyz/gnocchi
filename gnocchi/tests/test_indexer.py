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
import uuid

import testscenarios

from gnocchi import indexer
from gnocchi.indexer import null
from gnocchi import tests


load_tests = testscenarios.load_tests_apply_scenarios


class TestIndexer(tests.TestCase):
    def test_get_driver(self):
        self.conf.set_override('driver', 'null', 'indexer')
        driver = indexer.get_driver(self.conf)
        self.assertIsInstance(driver, null.NullIndexer)


class TestIndexerDriver(tests.TestCase):

    def test_create_resource(self):
        r1 = uuid.uuid4()
        rc = self.index.create_resource(r1, "foo", "bar")
        self.assertIsNotNone(rc['started_at'])
        del rc['started_at']
        self.assertEqual({"id": str(r1),
                          "user_id": "foo",
                          "project_id": "bar",
                          "ended_at": None,
                          "entities": {}},
                         rc)
        rg = self.index.get_resource(r1)
        self.assertEqual(str(rc['id']), rg['id'])
        self.assertEqual(rc['entities'], rg['entities'])

    def test_delete_resource(self):
        r1 = uuid.uuid4()
        self.index.create_resource(r1, "foo", "bar")
        self.index.delete_resource(r1)

    def test_delete_resource_non_existent(self):
        r1 = uuid.uuid4()
        self.assertRaises(indexer.NoSuchResource,
                          self.index.delete_resource,
                          r1)

    def test_create_entity_twice(self):
        e1 = str(uuid.uuid4())
        self.index.create_entity(e1)
        self.assertRaises(indexer.EntityAlreadyExists,
                          self.index.create_entity,
                          e1)

    def test_create_resource_with_start_timestamp(self):
        r1 = uuid.uuid4()
        ts = datetime.datetime(2014, 1, 1, 23, 34, 23, 1234)
        rc = self.index.create_resource(
            r1, "foo", "bar",
            started_at=ts)
        self.assertEqual({"id": str(r1),
                          "user_id": "foo",
                          "project_id": "bar",
                          "started_at": ts,
                          "ended_at": None,
                          "entities": {}}, rc)
        r = self.index.get_resource(r1)
        self.assertEqual({"id": str(r1),
                          "user_id": "foo",
                          "project_id": "bar",
                          "started_at": ts,
                          "ended_at": None,
                          "entities": {}}, r)

    def test_create_resource_with_entities(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        e2 = uuid.uuid4()
        self.index.create_entity(e1)
        self.index.create_entity(e2)
        rc = self.index.create_resource(r1, "foo", "bar",
                                        entities={'foo': e1, 'bar': e2})
        self.assertIsNotNone(rc['started_at'])
        del rc['started_at']
        self.assertEqual({"id": str(r1),
                          "user_id": "foo",
                          "project_id": "bar",
                          "ended_at": None,
                          "entities": {'foo': str(e1), 'bar': str(e2)}}, rc)
        r = self.index.get_resource(r1)
        self.assertIsNotNone(r['started_at'])
        del r['started_at']
        self.assertEqual({"id": str(r1),
                          "ended_at": None,
                          "user_id": "foo",
                          "project_id": "bar",
                          "entities": {'foo': str(e1), 'bar': str(e2)}}, r)

    def test_update_non_existent_resource_end_timestamp(self):
        r1 = uuid.uuid4()
        self.assertRaises(
            indexer.NoSuchResource,
            self.index.update_resource,
            r1,
            ended_at=datetime.datetime(2014, 1, 1, 2, 3, 4))

    def test_update_resource_end_timestamp(self):
        r1 = uuid.uuid4()
        self.index.create_resource(r1, "foo", "bar")
        self.index.update_resource(
            r1,
            ended_at=datetime.datetime(2043, 1, 1, 2, 3, 4))
        r = self.index.get_resource(r1)
        self.assertIsNotNone(r['started_at'])
        del r['started_at']
        self.assertEqual({"id": str(r1),
                          "ended_at": datetime.datetime(2043, 1, 1, 2, 3, 4),
                          "user_id": "foo",
                          "project_id": "bar",
                          "entities": {}}, r)
        self.index.update_resource(
            r1,
            ended_at=None)
        r = self.index.get_resource(r1)
        self.assertIsNotNone(r['started_at'])
        del r['started_at']
        self.assertEqual({"id": str(r1),
                          "ended_at": None,
                          "user_id": "foo",
                          "project_id": "bar",
                          "entities": {}}, r)

    def test_update_resource_entities(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        e2 = uuid.uuid4()
        self.index.create_entity(e1)
        self.index.create_resource(r1, "foo", "bar",
                                   entities={'foo': e1})
        self.index.create_entity(e2)
        rc = self.index.update_resource(r1, entities={'bar': e2})
        r = self.index.get_resource(r1)
        self.assertEqual(rc, r)

    def test_update_non_existent_entity(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        self.index.create_resource(r1, "foo", "bar")
        self.assertRaises(indexer.NoSuchEntity,
                          self.index.update_resource,
                          r1, entities={'bar': e1})

    def test_update_non_existent_resource(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        self.index.create_entity(e1)
        self.assertRaises(indexer.NoSuchResource,
                          self.index.update_resource,
                          r1, entities={'bar': e1})

    def test_create_resource_with_non_existent_entities(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        self.assertRaises(indexer.NoSuchEntity,
                          self.index.create_resource,
                          r1, "foo", "bar",
                          entities={'foo': e1})

    def test_delete_entity(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        e2 = uuid.uuid4()
        self.index.create_entity(e1)
        self.index.create_entity(e2)
        self.index.create_resource(r1, "foo", "bar",
                                   entities={'foo': e1, 'bar': e2})
        self.index.delete_entity(e1)
        r = self.index.get_resource(r1)
        self.assertIsNotNone(r['started_at'])
        del r['started_at']
        self.assertEqual({"id": str(r1),
                          "ended_at": None,
                          "user_id": "foo",
                          "project_id": "bar",
                          "entities": {'bar': str(e2)}}, r)
