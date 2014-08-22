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
        rc = self.index.create_resource('generic', r1, "foo", "bar")
        self.assertIsNotNone(rc['started_at'])
        del rc['started_at']
        self.assertEqual({"id": str(r1),
                          "user_id": "foo",
                          "project_id": "bar",
                          "ended_at": None,
                          "type": "generic",
                          "entities": {}},
                         rc)
        rg = self.index.get_resource('generic', r1)
        self.assertEqual(str(rc['id']), rg['id'])
        self.assertEqual(rc['entities'], rg['entities'])

    def test_create_non_existent_entity(self):
        e = uuid.uuid4()
        try:
            self.index.create_resource(
                'generic', uuid.uuid4(), "foo", "bar",
                entities={"foo": e})
        except indexer.NoSuchEntity as ex:
            self.assertEqual(e, ex.entity)
        else:
            self.fail("Exception not raised")

    def test_create_resource_already_exists(self):
        r1 = uuid.uuid4()
        self.index.create_resource('generic', r1, "foo", "bar")
        self.assertRaises(indexer.ResourceAlreadyExists,
                          self.index.create_resource,
                          'generic', r1, "foo", "bar")

    def test_create_instance(self):
        r1 = uuid.uuid4()
        rc = self.index.create_resource('instance', r1, "foo", "bar",
                                        flavor_id=1,
                                        image_ref="http://foo/bar",
                                        host="foo",
                                        display_name="lol")
        self.assertIsNotNone(rc['started_at'])
        del rc['started_at']
        self.assertEqual({"id": str(r1),
                          "user_id": "foo",
                          "type": "instance",
                          "project_id": "bar",
                          "ended_at": None,
                          "display_name": "lol",
                          "host": "foo",
                          "image_ref": "http://foo/bar",
                          "flavor_id": 1,
                          "entities": {}},
                         rc)
        rg = self.index.get_resource('generic', r1)
        self.assertEqual(str(rc['id']), rg['id'])
        self.assertEqual(rc['entities'], rg['entities'])

    def test_delete_resource(self):
        r1 = uuid.uuid4()
        self.index.create_resource('generic', r1, "foo", "bar")
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
            'generic',
            r1, "foo", "bar",
            started_at=ts)
        self.assertEqual({"id": str(r1),
                          "user_id": "foo",
                          "project_id": "bar",
                          "started_at": ts,
                          "ended_at": None,
                          "type": "generic",
                          "entities": {}}, rc)
        r = self.index.get_resource('generic', r1)
        self.assertEqual({"id": str(r1),
                          "user_id": "foo",
                          "project_id": "bar",
                          "started_at": ts,
                          "ended_at": None,
                          "type": "generic",
                          "entities": {}}, r)

    def test_create_resource_with_entities(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        e2 = uuid.uuid4()
        self.index.create_entity(e1)
        self.index.create_entity(e2)
        rc = self.index.create_resource('generic', r1, "foo", "bar",
                                        entities={'foo': e1, 'bar': e2})
        self.assertIsNotNone(rc['started_at'])
        del rc['started_at']
        self.assertEqual({"id": str(r1),
                          "user_id": "foo",
                          "project_id": "bar",
                          "ended_at": None,
                          "type": "generic",
                          "entities": {'foo': str(e1), 'bar': str(e2)}}, rc)
        r = self.index.get_resource('generic', r1)
        self.assertIsNotNone(r['started_at'])
        del r['started_at']
        self.assertEqual({"id": str(r1),
                          "type": "generic",
                          "ended_at": None,
                          "user_id": "foo",
                          "project_id": "bar",
                          "entities": {'foo': str(e1), 'bar': str(e2)}}, r)

    def test_update_non_existent_resource_end_timestamp(self):
        r1 = uuid.uuid4()
        self.assertRaises(
            indexer.NoSuchResource,
            self.index.update_resource,
            'generic',
            r1,
            ended_at=datetime.datetime(2014, 1, 1, 2, 3, 4))

    def test_update_resource_end_timestamp(self):
        r1 = uuid.uuid4()
        self.index.create_resource('generic', r1, "foo", "bar")
        self.index.update_resource(
            'generic',
            r1,
            ended_at=datetime.datetime(2043, 1, 1, 2, 3, 4))
        r = self.index.get_resource('generic', r1)
        self.assertIsNotNone(r['started_at'])
        del r['started_at']
        self.assertEqual({"id": str(r1),
                          "ended_at": datetime.datetime(2043, 1, 1, 2, 3, 4),
                          "user_id": "foo",
                          "project_id": "bar",
                          "type": "generic",
                          "entities": {}}, r)
        self.index.update_resource(
            'generic',
            r1,
            ended_at=None)
        r = self.index.get_resource('generic', r1)
        self.assertIsNotNone(r['started_at'])
        del r['started_at']
        self.assertEqual({"id": str(r1),
                          "ended_at": None,
                          "user_id": "foo",
                          "project_id": "bar",
                          "type": "generic",
                          "entities": {}}, r)

    def test_update_resource_entities(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        e2 = uuid.uuid4()
        self.index.create_entity(e1)
        self.index.create_resource('generic', r1, "foo", "bar",
                                   entities={'foo': e1})
        self.index.create_entity(e2)
        rc = self.index.update_resource('generic', r1, entities={'bar': e2})
        r = self.index.get_resource('generic', r1)
        self.assertEqual(rc, r)

    def test_update_resource_attribute(self):
        r1 = uuid.uuid4()
        rc = self.index.create_resource('instance', r1, "foo", "bar",
                                        flavor_id=1,
                                        image_ref="http://foo/bar",
                                        host="foo",
                                        display_name="lol")
        rc = self.index.update_resource('instance', r1, host="bar")
        r = self.index.get_resource('instance', r1)
        rc['host'] = "bar"
        self.assertEqual(rc, r)

    def test_update_resource_unknown_attribute(self):
        r1 = uuid.uuid4()
        self.index.create_resource('instance', r1, "foo", "bar",
                                   flavor_id=1,
                                   image_ref="http://foo/bar",
                                   host="foo",
                                   display_name="lol")
        self.assertRaises(indexer.ResourceAttributeError,
                          self.index.update_resource,
                          'instance',
                          r1, foo="bar")

    def test_update_non_existent_entity(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        self.index.create_resource('generic', r1, "foo", "bar")
        self.assertRaises(indexer.NoSuchEntity,
                          self.index.update_resource,
                          'generic',
                          r1, entities={'bar': e1})

    def test_update_non_existent_resource(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        self.index.create_entity(e1)
        self.assertRaises(indexer.NoSuchResource,
                          self.index.update_resource,
                          'generic',
                          r1, entities={'bar': e1})

    def test_create_resource_with_non_existent_entities(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        self.assertRaises(indexer.NoSuchEntity,
                          self.index.create_resource,
                          'generic',
                          r1, "foo", "bar",
                          entities={'foo': e1})

    def test_delete_entity(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        e2 = uuid.uuid4()
        self.index.create_entity(e1)
        self.index.create_entity(e2)
        self.index.create_resource('generic', r1, "foo", "bar",
                                   entities={'foo': e1, 'bar': e2})
        self.index.delete_entity(e1)
        r = self.index.get_resource('generic', r1)
        self.assertIsNotNone(r['started_at'])
        del r['started_at']
        self.assertEqual({"id": str(r1),
                          "ended_at": None,
                          "user_id": "foo",
                          "project_id": "bar",
                          "type": "generic",
                          "entities": {'bar': str(e2)}}, r)

    def test_delete_instance(self):
        r1 = uuid.uuid4()
        created = self.index.create_resource('instance', r1, "foo", "bar",
                                             flavor_id=123,
                                             image_ref="foo",
                                             host="dwq",
                                             display_name="foobar")
        got = self.index.get_resource('instance', r1)
        self.assertEqual(created, got)
        self.index.delete_resource(r1)
        got = self.index.get_resource('instance', r1)
        self.assertIsNone(got)

    def test_list_resources_by_unknown_field(self):
        self.assertRaises(indexer.ResourceAttributeError,
                          self.index.list_resources,
                          'generic',
                          attributes_filter={"fern": "bar"})

    def test_list_resources_by_user(self):
        r1 = uuid.uuid4()
        u1 = str(uuid.uuid4())
        g = self.index.create_resource('generic', r1, u1, "bar")
        resources = self.index.list_resources(
            'generic',
            attributes_filter={"user_id": u1})
        self.assertEqual(len(resources), 1)
        self.assertEqual(g, resources[0])
        resources = self.index.list_resources(
            'generic',
            attributes_filter={"user_id": str(uuid.uuid4())})
        self.assertEqual(len(resources), 0)

    def test_list_resources_by_user_with_details(self):
        r1 = uuid.uuid4()
        u1 = str(uuid.uuid4())
        g = self.index.create_resource('generic', r1, u1, "bar")
        r2 = uuid.uuid4()
        i = self.index.create_resource('instance', r2, u1, "bar",
                                       flavor_id=123,
                                       image_ref="foo",
                                       host="dwq",
                                       display_name="foobar")
        resources = self.index.list_resources(
            'generic',
            attributes_filter={"user_id": u1},
            details=True,
        )
        self.assertEqual(len(resources), 2)
        self.assertEqual(sorted(resources), sorted([g, i]))

    def test_list_resources_by_project(self):
        r1 = uuid.uuid4()
        p1 = str(uuid.uuid4())
        g = self.index.create_resource('generic', r1, "foo", p1)
        resources = self.index.list_resources(
            'generic',
            attributes_filter={"project_id": p1})
        self.assertEqual(len(resources), 1)
        self.assertEqual(g, resources[0])
        resources = self.index.list_resources(
            'generic',
            attributes_filter={"project_id": str(uuid.uuid4())})
        self.assertEqual(len(resources), 0)

    def test_list_resources(self):
        # NOTE(jd) So this test is a bit fuzzy right now as we uses the same
        # database for all tests and the tests are running concurrently, but
        # for now it'll be better than nothing.
        r1 = uuid.uuid4()
        g = self.index.create_resource('generic', r1, "foo", "bar")
        r2 = uuid.uuid4()
        i = self.index.create_resource('instance', r2, "foo", "bar",
                                       flavor_id=123,
                                       image_ref="foo",
                                       host="dwq",
                                       display_name="foobar")
        resources = self.index.list_resources('generic')
        self.assertGreaterEqual(len(resources), 2)
        g_found = False
        i_found = False
        for r in resources:
            if r['id'] == str(r1):
                self.assertEqual(g, r)
                g_found = True
            elif r['id'] == str(r2):
                i_found = True
            if i_found and g_found:
                break
        else:
            self.fail("Some resources were not found")

        resources = self.index.list_resources('instance')
        self.assertGreaterEqual(len(resources), 1)
        for r in resources:
            if r['id'] == str(r2):
                self.assertEqual(i, r)
                break
        else:
            self.fail("Some resources were not found")

    def test_list_resources_started_after_ended_before(self):
        # NOTE(jd) So this test is a bit fuzzy right now as we uses the same
        # database for all tests and the tests are running concurrently, but
        # for now it'll be better than nothing.
        r1 = uuid.uuid4()
        g = self.index.create_resource(
            'generic', r1, "foo", "bar",
            started_at=datetime.datetime(2000, 1, 1, 23, 23, 23),
            ended_at=datetime.datetime(2000, 1, 3, 23, 23, 23))
        r2 = uuid.uuid4()
        i = self.index.create_resource(
            'instance', r2, "foo", "bar",
            flavor_id=123,
            image_ref="foo",
            host="dwq",
            display_name="foobar",
            started_at=datetime.datetime(2000, 1, 1, 23, 23, 23),
            ended_at=datetime.datetime(2000, 1, 4, 23, 23, 23))
        resources = self.index.list_resources(
            'generic',
            started_after=datetime.datetime(2000, 1, 1, 23, 23, 23),
            ended_before=datetime.datetime(2000, 1, 5, 23, 23, 23))
        self.assertGreaterEqual(len(resources), 2)
        g_found = False
        i_found = False
        for r in resources:
            if r['id'] == str(r1):
                self.assertEqual(g, r)
                g_found = True
            elif r['id'] == str(r2):
                i_found = True
            if i_found and g_found:
                break
        else:
            self.fail("Some resources were not found")

        resources = self.index.list_resources(
            'instance',
            started_after=datetime.datetime(2000, 1, 1, 23, 23, 23))
        self.assertGreaterEqual(len(resources), 1)
        for r in resources:
            if r['id'] == str(r2):
                self.assertEqual(i, r)
                break
        else:
            self.fail("Some resources were not found")

        resources = self.index.list_resources(
            'generic',
            ended_before=datetime.datetime(1999, 1, 1, 23, 23, 23))
        self.assertEqual(len(resources), 0)
