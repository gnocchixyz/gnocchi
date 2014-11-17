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

import six
import testscenarios

from gnocchi import indexer
from gnocchi.indexer import null
from gnocchi.tests import base as tests_base


load_tests = testscenarios.load_tests_apply_scenarios


class TestIndexer(tests_base.TestCase):
    def test_get_driver(self):
        self.conf.set_override('driver', 'null', 'indexer')
        driver = indexer.get_driver(self.conf)
        self.assertIsInstance(driver, null.NullIndexer)


class TestIndexerDriver(tests_base.TestCase):

    def test_create_archive_policy_already_exists(self):
        # NOTE(jd) This archive policy
        # is created by gnocchi.tests on setUp() :)
        self.assertRaises(indexer.ArchivePolicyAlreadyExists,
                          self.index.create_archive_policy, "high", {})

    def test_create_resource(self):
        r1 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        rc = self.index.create_resource('generic', r1, user, project)
        self.assertIsNotNone(rc['started_at'])
        del rc['started_at']
        self.assertEqual({"id": str(r1),
                          "user_id": six.text_type(user),
                          "project_id": six.text_type(project),
                          "ended_at": None,
                          "type": "generic",
                          "entities": {}},
                         rc)
        rg = self.index.get_resource('generic', r1)
        self.assertEqual(str(rc['id']), rg['id'])
        self.assertEqual(rc['entities'], rg['entities'])

    def test_create_resource_unknown_attribute_fkey(self):
        r1 = uuid.uuid4()
        try:
            self.index.create_resource('entity', r1,
                                       uuid.uuid4(), uuid.uuid4(),
                                       archive_policy="foobar")
        except indexer.ResourceValueError as e:
            self.assertEqual('entity', e.resource_type)
            self.assertEqual('archive_policy', e.attribute)
            self.assertEqual("foobar", e.value)
        else:
            self.fail("No exception raised")

    def test_create_non_existent_entity(self):
        e = uuid.uuid4()
        try:
            self.index.create_resource(
                'generic', uuid.uuid4(), uuid.uuid4(), uuid.uuid4(),
                entities={"foo": e})
        except indexer.NoSuchEntity as ex:
            self.assertEqual(e, ex.entity)
        else:
            self.fail("Exception not raised")

    def test_create_resource_already_exists(self):
        r1 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        self.index.create_resource('generic', r1, user, project)
        self.assertRaises(indexer.ResourceAlreadyExists,
                          self.index.create_resource,
                          'generic', r1, user, project)

    def _do_test_create_instance(self, server_group=None):
        r1 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        kwargs = {'server_group': server_group} if server_group else {}

        rc = self.index.create_resource('instance', r1, user, project,
                                        flavor_id=1,
                                        image_ref="http://foo/bar",
                                        host="foo",
                                        display_name="lol", **kwargs)
        self.assertIsNotNone(rc['started_at'])
        del rc['started_at']
        self.assertEqual({"id": str(r1),
                          "user_id": six.text_type(user),
                          "type": "instance",
                          "project_id": six.text_type(project),
                          "ended_at": None,
                          "display_name": "lol",
                          "server_group": server_group,
                          "host": "foo",
                          "image_ref": "http://foo/bar",
                          "flavor_id": 1,
                          "entities": {}},
                         rc)
        rg = self.index.get_resource('generic', r1)
        self.assertEqual(str(rc['id']), rg['id'])
        self.assertEqual(rc['entities'], rg['entities'])

    def test_create_instance(self):
        self._do_test_create_instance()

    def test_create_instance_with_server_group(self):
        self._do_test_create_instance('my_autoscaling_group')

    def test_delete_resource(self):
        r1 = uuid.uuid4()
        self.index.create_resource('generic', r1, uuid.uuid4(), uuid.uuid4())
        self.index.delete_resource(r1)

    def test_delete_resource_non_existent(self):
        r1 = uuid.uuid4()
        self.assertRaises(indexer.NoSuchResource,
                          self.index.delete_resource,
                          r1)

    def test_create_resource_with_start_timestamp(self):
        r1 = uuid.uuid4()
        ts = datetime.datetime(2014, 1, 1, 23, 34, 23, 1234)
        user = uuid.uuid4()
        project = uuid.uuid4()
        rc = self.index.create_resource(
            'generic',
            r1, user, project,
            started_at=ts)
        self.assertEqual({"id": str(r1),
                          "user_id": six.text_type(user),
                          "project_id": six.text_type(project),
                          "started_at": ts,
                          "ended_at": None,
                          "type": "generic",
                          "entities": {}}, rc)
        r = self.index.get_resource('generic', r1)
        self.assertEqual({"id": str(r1),
                          "user_id": six.text_type(user),
                          "project_id": six.text_type(project),
                          "started_at": ts,
                          "ended_at": None,
                          "type": "generic",
                          "entities": {}}, r)

    def test_create_resource_with_entities(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        e2 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        self.index.create_resource('entity', e1,
                                   user, project,
                                   archive_policy="low")
        self.index.create_resource('entity', e2,
                                   user, project,
                                   archive_policy="low")
        rc = self.index.create_resource('generic', r1, user, project,
                                        entities={'foo': e1, 'bar': e2})
        self.assertIsNotNone(rc['started_at'])
        del rc['started_at']
        self.assertEqual({"id": str(r1),
                          "user_id": six.text_type(user),
                          "project_id": six.text_type(project),
                          "ended_at": None,
                          "type": "generic",
                          "entities": {'foo': str(e1), 'bar': str(e2)}}, rc)
        r = self.index.get_resource('generic', r1)
        self.assertIsNotNone(r['started_at'])
        del r['started_at']
        self.assertEqual({"id": str(r1),
                          "type": "generic",
                          "ended_at": None,
                          "user_id": six.text_type(user),
                          "project_id": six.text_type(project),
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
        user = uuid.uuid4()
        project = uuid.uuid4()
        self.index.create_resource('generic', r1, user, project)
        self.index.update_resource(
            'generic',
            r1,
            ended_at=datetime.datetime(2043, 1, 1, 2, 3, 4))
        r = self.index.get_resource('generic', r1)
        self.assertIsNotNone(r['started_at'])
        del r['started_at']
        self.assertEqual({"id": str(r1),
                          "ended_at": datetime.datetime(2043, 1, 1, 2, 3, 4),
                          "user_id": six.text_type(user),
                          "project_id": six.text_type(project),
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
                          "user_id": six.text_type(user),
                          "project_id": six.text_type(project),
                          "type": "generic",
                          "entities": {}}, r)

    def test_update_resource_entities(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        e2 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        self.index.create_resource('entity', e1, user, project,
                                   archive_policy="low")
        self.index.create_resource('generic', r1, user, project,
                                   entities={'foo': e1})
        self.index.create_resource('entity', e2, user, project,
                                   archive_policy="low")
        rc = self.index.update_resource('generic', r1, entities={'bar': e2})
        r = self.index.get_resource('generic', r1)
        self.assertEqual(rc, r)

    def test_update_resource_entities_append(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        e2 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        self.index.create_resource('entity', e1, user, project,
                                   archive_policy="low")
        self.index.create_resource('entity', e2, user, project,
                                   archive_policy="low")
        self.index.create_resource('generic', r1, user, project,
                                   entities={'foo': e1})
        rc = self.index.update_resource('generic', r1, entities={'bar': e2},
                                        append_entities=True)
        r = self.index.get_resource('generic', r1)
        self.assertEqual(rc, r)
        self.assertIn('foo', rc['entities'])
        self.assertIn('bar', rc['entities'])

    def test_update_resource_entities_append_fail(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        e2 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        self.index.create_resource('entity', e1, user, project,
                                   archive_policy="low")
        self.index.create_resource('entity', e2, user, project,
                                   archive_policy="low")
        self.index.create_resource('generic', r1, user, project,
                                   entities={'foo': e1})

        self.assertRaises(indexer.NamedEntityAlreadyExists,
                          self.index.update_resource,
                          'generic', r1, entities={'foo': e2},
                          append_entities=True)
        r = self.index.get_resource('generic', r1)
        self.assertEqual(str(e1), r['entities']['foo'])

    def test_update_resource_attribute(self):
        r1 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        rc = self.index.create_resource('instance', r1, user, project,
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
        self.index.create_resource('instance', r1, uuid.uuid4(), uuid.uuid4(),
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
        self.index.create_resource('generic', r1, uuid.uuid4(), uuid.uuid4())
        self.assertRaises(indexer.NoSuchEntity,
                          self.index.update_resource,
                          'generic',
                          r1, entities={'bar': e1})

    def test_update_non_existent_resource(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        self.index.create_resource('entity', e1, uuid.uuid4(), uuid.uuid4(),
                                   archive_policy="low")
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
                          r1, uuid.uuid4(), uuid.uuid4(),
                          entities={'foo': e1})

    def test_delete_entity(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        e2 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        self.index.create_resource('entity', e1, user, project,
                                   archive_policy="low")
        self.index.create_resource('entity', e2, user, project,
                                   archive_policy="low")
        self.index.create_resource('generic', r1, user, project,
                                   entities={'foo': e1, 'bar': e2})
        self.index.delete_entity(e1)
        r = self.index.get_resource('generic', r1)
        self.assertIsNotNone(r['started_at'])
        del r['started_at']
        self.assertEqual({"id": str(r1),
                          "ended_at": None,
                          "user_id": six.text_type(user),
                          "project_id": six.text_type(project),
                          "type": "generic",
                          "entities": {'bar': str(e2)}}, r)

    def test_delete_instance(self):
        r1 = uuid.uuid4()
        created = self.index.create_resource('instance', r1,
                                             uuid.uuid4(), uuid.uuid4(),
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
        user = uuid.uuid4()
        project = uuid.uuid4()
        g = self.index.create_resource('generic', r1, user, project)
        resources = self.index.list_resources(
            'generic',
            attributes_filter={"user_id": user})
        self.assertEqual(len(resources), 1)
        self.assertEqual(g, resources[0])
        resources = self.index.list_resources(
            'generic',
            attributes_filter={"user_id": uuid.uuid4()})
        self.assertEqual(len(resources), 0)

    def test_list_resources_by_user_with_details(self):
        r1 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        g = self.index.create_resource('generic', r1, user, project)
        r2 = uuid.uuid4()
        i = self.index.create_resource('instance', r2,
                                       user, project,
                                       flavor_id=123,
                                       image_ref="foo",
                                       host="dwq",
                                       display_name="foobar")
        resources = self.index.list_resources(
            'generic',
            attributes_filter={"user_id": user},
            details=True,
        )
        self.assertEqual(len(resources), 2)
        self.assertIn(g, resources)
        self.assertIn(i, resources)

    def test_list_resources_by_project(self):
        r1 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        g = self.index.create_resource('generic', r1, user, project)
        resources = self.index.list_resources(
            'generic',
            attributes_filter={"project_id": project})
        self.assertEqual(len(resources), 1)
        self.assertEqual(g, resources[0])
        resources = self.index.list_resources(
            'generic',
            attributes_filter={"project_id": uuid.uuid4()})
        self.assertEqual(len(resources), 0)

    def test_list_resources(self):
        # NOTE(jd) So this test is a bit fuzzy right now as we uses the same
        # database for all tests and the tests are running concurrently, but
        # for now it'll be better than nothing.
        r1 = uuid.uuid4()
        g = self.index.create_resource('generic', r1,
                                       uuid.uuid4(), uuid.uuid4())
        r2 = uuid.uuid4()
        i = self.index.create_resource('instance', r2,
                                       uuid.uuid4(), uuid.uuid4(),
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
        user = uuid.uuid4()
        project = uuid.uuid4()
        g = self.index.create_resource(
            'generic', r1, user, project,
            started_at=datetime.datetime(2000, 1, 1, 23, 23, 23),
            ended_at=datetime.datetime(2000, 1, 3, 23, 23, 23))
        r2 = uuid.uuid4()
        i = self.index.create_resource(
            'instance', r2, user, project,
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

    def test_get_entity(self):
        e1 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        self.index.create_resource('entity', e1,
                                   user, project,
                                   archive_policy="low")

        entity = self.index.get_entity(e1)
        self.assertIsNotNone(entity['started_at'])
        del entity['started_at']
        self.assertEqual({"id": str(e1),
                          "archive_policy": "low",
                          "user_id": six.text_type(user),
                          "project_id": six.text_type(project),
                          "ended_at": None,
                          "type": "entity"},
                         entity)

    def test_get_entity_with_details(self):
        e1 = uuid.uuid4()
        user = uuid.uuid4()
        project = uuid.uuid4()
        self.index.create_resource('entity', e1,
                                   user, project,
                                   archive_policy="low")

        entity = self.index.get_entity(e1, details=True)
        self.assertIsNotNone(entity['started_at'])
        del entity['started_at']
        self.assertEqual({"id": str(e1),
                          "archive_policy": {
                              "definition": [
                                  {u'granularity': 300, u'points': 12},
                                  {u'granularity': 3600, u'points': 24},
                                  {u'granularity': 86400, u'points': 30}],
                              "name": "low"},
                          "user_id": six.text_type(user),
                          "project_id": six.text_type(project),
                          "ended_at": None,
                          "type": "entity"},
                         entity)

    def test_get_entity_with_bad_uuid(self):
        e1 = uuid.uuid4()
        self.assertIsNone(self.index.get_entity(e1))
