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
import json
import uuid

from oslo.utils import timeutils
import pecan
import six
import testscenarios
import webtest

from gnocchi.rest import app
from gnocchi import tests


load_tests = testscenarios.load_tests_apply_scenarios


class FakeMemcache(object):
    VALID_TOKEN = '4562138218392831'
    USER_ID = str(uuid.uuid4())
    PROJECT_ID = str(uuid.uuid4())

    def get(self, key):
        if key == "tokens/%s" % self.VALID_TOKEN:
            dt = datetime.datetime(
                year=datetime.MAXYEAR, month=12, day=31,
                hour=23, minute=59, second=59)
            return json.dumps(({'access': {
                'token': {'id': self.VALID_TOKEN,
                          'expires': timeutils.isotime(dt)},
                'user': {
                    'id': self.USER_ID,
                    'name': 'myusername',
                    'tenantId': self.PROJECT_ID,
                    'tenantName': 'mytenant',
                    'roles': [
                        {'name': 'admin'},
                    ]},
            }}, timeutils.isotime(dt)))

    @staticmethod
    def set(key, value, **kwargs):
        pass


class TestingApp(webtest.TestApp):
    CACHE_NAME = 'fake.cache'

    def __init__(self, *args, **kwargs):
        super(TestingApp, self).__init__(*args, **kwargs)
        # Setup Keystone auth_token fake cache
        self.extra_environ.update({self.CACHE_NAME: FakeMemcache()})

    def do_request(self, req, *args, **kwargs):
        req.headers['X-Auth-Token'] = FakeMemcache.VALID_TOKEN
        return super(TestingApp, self).do_request(req, *args, **kwargs)


class RestTest(tests.TestCase):
    def setUp(self):
        super(RestTest, self).setUp()
        c = {}
        c.update(app.PECAN_CONFIG)
        c['conf'] = self.conf
        c['indexer'] = self.index
        c['storage'] = self.storage
        self.conf.import_opt("cache", "keystonemiddleware.auth_token",
                             group="keystone_authtoken")
        self.conf.set_override("cache", TestingApp.CACHE_NAME,
                               group='keystone_authtoken')
        self.app = TestingApp(pecan.load_app(c))

    def test_root(self):
        result = self.app.get("/", status=200)
        self.assertEqual(b"Nom nom nom.", result.body)
        self.assertEqual("text/plain", result.content_type)


class ArchivePolicyTest(RestTest):
    def test_post_archive_policy(self):
        name = str(uuid.uuid4())
        definition = [{
            "granularity": 10,
            "points": 20,
        }]
        result = self.app.post_json(
            "/v1/archive_policy",
            params={"name": name,
                    "definition": definition},
            status=201)
        self.assertEqual("application/json", result.content_type)
        ap = json.loads(result.text)
        self.assertEqual("http://localhost/v1/archive_policy/" + name,
                         result.headers['Location'])
        self.assertEqual(name, ap['name'])
        self.assertEqual(definition, ap['definition'])

    def test_post_archive_policy_and_entity(self):
        ap = str(uuid.uuid4())
        self.app.post_json(
            "/v1/archive_policy",
            params={"name": ap,
                    "definition": [{
                        "granularity": 10,
                        "points": 20,
                    }]},
            status=201)
        self.app.post_json(
            "/v1/entity",
            params={"archive_policy": ap},
            status=201)

    def test_post_archive_policy_wrong_value(self):
        result = self.app.post_json(
            "/v1/archive_policy",
            params={"name": "somenewname",
                    "definition": "foobar"},
            expect_errors=True,
            status=400)
        self.assertIn(b'Invalid input: expected a list '
                      b'for dictionary value @ data['
                      + repr(u'definition').encode('ascii') + b"]",
                      result.body)

    def test_post_archive_already_exists(self):
        result = self.app.post_json(
            "/v1/archive_policy",
            params={"name": "high",
                    "definition": [{
                        "granularity": 10,
                        "points": 20,
                    }]},
            expect_errors=True,
            status=409)
        self.assertIn('Archive policy high already exists', result.text)

    def test_get_archive_policy(self):
        result = self.app.get("/v1/archive_policy/medium")
        ap = json.loads(result.text)
        self.assertEqual({"name": "medium",
                          "definition": self.archive_policies['medium']},
                         ap)

    def test_list_archive_policy(self):
        result = self.app.get("/v1/archive_policy")
        aps = json.loads(result.text)
        for name, definition in six.iteritems(self.archive_policies):
            self.assertIn({"name": name,
                           "definition": definition}, aps)


class EntityTest(RestTest):
    def test_post_entity(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archive_policy": "medium"})
        self.assertEqual("application/json", result.content_type)
        self.assertEqual(201, result.status_code)
        entity = json.loads(result.text)
        self.assertEqual("http://localhost/v1/entity/" + entity['id'],
                         result.headers['Location'])
        self.assertEqual(entity['archive_policy'], "medium")

    def test_post_entity_wrong_archive_policy(self):
        policy = str(uuid.uuid4())
        result = self.app.post_json("/v1/entity",
                                    params={"archive_policy": policy},
                                    expect_errors=True,
                                    status=400)
        self.assertIn('Unknown archive policy %s' % policy, result.text)

    def test_get_entity_as_resource(self):
        result = self.app.post_json(
            "/v1/entity",
            params={"archive_policy": "medium"},
            status=201)
        self.assertEqual("application/json", result.content_type)
        entity = json.loads(result.text)
        result = self.app.get("/v1/resource/entity/%s" % entity['id'])
        self.assertDictContainsSubset(entity, json.loads(result.text))

    def test_post_entity_as_resource(self):
        self.app.post_json("/v1/resource/entity",
                           params={"archive_policy": "medium"},
                           status=403)

    def test_delete_entity(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archive_policy": "medium"})
        entity = json.loads(result.text)
        result = self.app.delete("/v1/entity/" + entity['id'])
        self.assertEqual(result.status_code, 204)

    def test_delete_entity_non_existent(self):
        e1 = str(uuid.uuid4())
        result = self.app.delete("/v1/entity/" + e1,
                                 expect_errors=True)
        self.assertEqual(result.status_code, 404)
        self.assertIn(
            b"Entity " + e1.encode('ascii') + b" does not exist",
            result.body)

    def test_post_entity_bad_archives(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archive_policy": 'foobar123'},
                                    expect_errors=True,
                                    status=400)
        self.assertEqual("text/plain", result.content_type)
        self.assertIn(b"Unknown archive policy foobar123", result.body)

    def test_add_measure(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archive_policy": "high"})
        entity = json.loads(result.text)
        result = self.app.post_json(
            "/v1/entity/%s/measures" % entity['id'],
            params=[{"timestamp": '2013-01-01 23:23:23',
                     "value": 1234.2}])
        self.assertEqual(result.status_code, 204)

    def test_add_multiple_measures_per_entity(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archive_policy": "high"})
        entity = json.loads(result.text)
        for x in range(5):
            result = self.app.post_json(
                "/v1/entity/%s/measures" % entity['id'],
                params=[{"timestamp": '2013-01-01 23:23:2%d' % x,
                         "value": 1234.2 + x}])
            self.assertEqual(result.status_code, 204)

    def test_add_measure_no_such_entity(self):
        e1 = str(uuid.uuid4())
        result = self.app.post_json(
            "/v1/entity/%s/measures" % e1,
            params=[{"timestamp": '2013-01-01 23:23:23',
                     "value": 1234.2}],
            expect_errors=True)
        self.assertEqual(result.status_code, 404)
        self.assertIn(
            b"Entity " + e1.encode('ascii') + b" does not exist",
            result.body)

    def test_get_measure(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archive_policy": "low"})
        entity = json.loads(result.text)
        self.app.post_json("/v1/entity/%s/measures" % entity['id'],
                           params=[{"timestamp": '2013-01-01 23:23:23',
                                    "value": 1234.2}])
        ret = self.app.get("/v1/entity/%s/measures" % entity['id'])
        self.assertEqual(ret.status_code, 200)
        result = json.loads(ret.text)
        self.assertEqual(
            {u'2013-01-01T00:00:00.000000': 1234.2,
             u'2013-01-01T23:00:00.000000': 1234.2,
             u'2013-01-01T23:20:00.000000': 1234.2},
            result)

    def test_get_measure_start(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archive_policy": "high"})
        entity = json.loads(result.text)
        self.app.post_json("/v1/entity/%s/measures" % entity['id'],
                           params=[{"timestamp": '2013-01-01 23:23:23',
                                    "value": 1234.2}])
        ret = self.app.get(
            "/v1/entity/%s/measures?start='2013-01-01 23:23:20"
            % entity['id'])
        self.assertEqual(ret.status_code, 200)
        result = json.loads(ret.text)
        self.assertEqual({'2013-01-01T23:23:23.000000': 1234.2},
                         result)

    def test_get_measure_stop(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archive_policy": "high"})
        entity = json.loads(result.text)
        self.app.post_json("/v1/entity/%s/measures" % entity['id'],
                           params=[{"timestamp": '2013-01-01 12:00:00',
                                    "value": 1234.2},
                                   {"timestamp": '2013-01-01 12:00:02',
                                    "value": 456}])
        ret = self.app.get("/v1/entity/%s/measures"
                           "?stop=2013-01-01 12:00:00" % entity['id'])
        self.assertEqual(ret.status_code, 200)
        result = json.loads(ret.text)
        self.assertEqual({'2013-01-01T12:00:00.000000': 1234.2},
                         result)

    def test_get_measure_aggregation(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archive_policy": "medium"})
        entity = json.loads(result.text)
        self.app.post_json("/v1/entity/%s/measures" % entity['id'],
                           params=[{"timestamp": '2013-01-01 12:00:01',
                                    "value": 123.2},
                                   {"timestamp": '2013-01-01 12:00:03',
                                    "value": 12345.2},
                                   {"timestamp": '2013-01-01 12:00:02',
                                    "value": 1234.2}])
        ret = self.app.get(
            "/v1/entity/%s/measures?aggregation=max" % entity['id'])
        self.assertEqual(ret.status_code, 200)
        result = json.loads(ret.text)
        self.assertEqual({'2013-01-01T12:00:00.000000': 12345.2,
                          '2013-01-01T00:00:00.000000': 12345.2},
                         result)


class ResourceTest(RestTest):

    resource_scenarios = [
        ('generic', dict(
            attributes={
                "started_at": "2014-01-03 02:02:02",
                "user_id": str(uuid.uuid4()),
                "project_id": str(uuid.uuid4()),
            },
            patchable_attributes={
                "ended_at": "2014-01-03 02:02:02",
            },
            resource_type='generic')),
        ('instance', dict(
            attributes={
                "started_at": "2014-01-03 02:02:02",
                "user_id": str(uuid.uuid4()),
                "project_id": str(uuid.uuid4()),
                "host": "foo",
                "image_ref": "imageref!",
                "flavor_id": 123,
                "display_name": "myinstance",
                "server_group": "as_group",
            },
            patchable_attributes={
                "ended_at": "2014-01-03 02:02:02",
                "host": "fooz",
                "image_ref": "imageref!z",
                "flavor_id": 1234,
                "display_name": "myinstancez",
                "server_group": "new_as_group",
            },
            resource_type='instance')),
        ('swift_account', dict(
            attributes={
                "started_at": "2014-01-03 02:02:02",
                "user_id": str(uuid.uuid4()),
                "project_id": str(uuid.uuid4()),
            },
            patchable_attributes={
                "ended_at": "2014-01-03 02:02:02",
            },
            resource_type='swift_account')),
    ]

    @classmethod
    def generate_scenarios(cls):
        cls.scenarios = testscenarios.multiply_scenarios(
            cls.scenarios,
            cls.resource_scenarios)

    def setUp(self):
        super(ResourceTest, self).setUp()
        # Copy attributes so we can modify them in each test :)
        self.attributes = self.attributes.copy()
        # Set an id in the attribute
        self.attributes['id'] = str(uuid.uuid4())

    def test_post_resource(self):
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes)
        self.assertEqual(201, result.status_code)
        resource = json.loads(result.text)
        self.assertEqual("http://localhost/v1/resource/"
                         + self.resource_type + "/" + self.attributes['id'],
                         result.headers['Location'])
        self.attributes['type'] = self.resource_type
        self.attributes['ended_at'] = None
        self.attributes['entities'] = {}
        self.assertEqual(resource, self.attributes)

    def test_post_resource_already_exist(self):
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes)
        self.assertEqual(201, result.status_code)
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes,
            expect_errors=True)
        self.assertEqual(409, result.status_code)
        self.assertIn("Resource %s already exists" % self.attributes['id'],
                      result.text)

    def test_post_unix_timestamp(self):
        self.attributes['started_at'] = "1400580045.856219"
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes)
        self.assertEqual(201, result.status_code)
        resource = json.loads(result.text)
        self.assertEqual(u"2014-05-20 10:00:45.856219",
                         resource['started_at'])

    def test_post_invalid_timestamp(self):
        self.attributes['started_at'] = "2014-01-01 02:02:02"
        self.attributes['ended_at'] = "2013-01-01 02:02:02"
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes,
            expect_errors=True)
        self.assertEqual(400, result.status_code)

    def test_post_invalid_no_user(self):
        del self.attributes['user_id']
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes,
            expect_errors=True)
        self.assertEqual(400, result.status_code)

    def test_post_invalid_no_project(self):
        del self.attributes['project_id']
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes,
            expect_errors=True)
        self.assertEqual(400, result.status_code)

    def test_get_resource(self):
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params=self.attributes)
        self.assertEqual(201, result.status_code)
        result = self.app.get("/v1/resource/"
                              + self.resource_type
                              + "/"
                              + self.attributes['id'])
        result = json.loads(result.text)
        self.attributes['type'] = self.resource_type
        self.attributes['entities'] = {}
        self.attributes['ended_at'] = None
        self.assertEqual(self.attributes, result)

    def test_get_resource_named_entity(self):
        self.attributes['entities'] = {'foo': {'archive_policy': "high"}}
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)
        result = self.app.get("/v1/resource/"
                              + self.resource_type
                              + "/"
                              + self.attributes['id']
                              + "/entity/foo/measures")
        self.assertEqual(200, result.status_code)

    def test_delete_resource_named_entity(self):
        self.attributes['entities'] = {'foo': {'archive_policy': "high"}}
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)
        result = self.app.delete("/v1/resource/"
                                 + self.resource_type
                                 + "/"
                                 + self.attributes['id']
                                 + "/entity/foo")
        self.assertEqual(204, result.status_code)
        result = self.app.delete("/v1/resource/"
                                 + self.resource_type
                                 + "/"
                                 + self.attributes['id']
                                 + "/entity/foo/measures",
                                 expect_errors=True)
        self.assertEqual(404, result.status_code)

    def test_get_resource_unknown_named_entity(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)
        result = self.app.get("/v1/resource/"
                              + self.resource_type
                              + "/"
                              + self.attributes['id']
                              + "/entity/foo",
                              expect_errors=True)
        self.assertEqual(404, result.status_code)

    def test_post_append_entities_already_exists(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)

        entities = {'foo': {'archive_policy': "high"}}
        self.app.post_json("/v1/resource/" + self.resource_type
                           + "/" + self.attributes['id'] + "/entity",
                           params=entities, status=204)
        entities = {'foo': {'archive_policy': "low"}}
        result = self.app.post_json("/v1/resource/" + self.resource_type
                                    + "/" + self.attributes['id']
                                    + "/entity",
                                    params=entities,
                                    expect_errors=True)
        self.assertEqual(409, result.status_code)

        result = self.app.get("/v1/resource/"
                              + self.resource_type + "/"
                              + self.attributes['id'])
        result = json.loads(result.text)
        self.assertTrue(uuid.UUID(result['entities']['foo']))

    def test_post_append_entities(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)

        entities = {'foo': {'archive_policy': "high"}}
        self.app.post_json("/v1/resource/" + self.resource_type
                           + "/" + self.attributes['id'] + "/entity",
                           params=entities, status=204)
        result = self.app.get("/v1/resource/"
                              + self.resource_type + "/"
                              + self.attributes['id'])
        result = json.loads(result.text)
        self.assertTrue(uuid.UUID(result['entities']['foo']))

    def test_patch_resource_entities(self):
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params=self.attributes)
        r = json.loads(result.text)
        self.assertEqual(201, result.status_code)
        new_entities = {'foo': {'archive_policy': "medium"}}
        result = self.app.patch_json(
            "/v1/resource/" + self.resource_type + "/"
            + self.attributes['id'],
            params={'entities': new_entities})
        self.assertEqual(result.status_code, 204)
        result = self.app.get("/v1/resource/"
                              + self.resource_type + "/"
                              + self.attributes['id'])
        result = json.loads(result.text)
        self.assertTrue(uuid.UUID(result['entities']['foo']))
        del result['entities']
        del r['entities']
        self.assertEqual(r, result)

    def test_patch_resource_non_existent_entities(self):
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params=self.attributes)
        self.assertEqual(201, result.status_code)
        e1 = str(uuid.uuid4())
        result = self.app.patch_json(
            "/v1/resource/"
            + self.resource_type
            + "/"
            + self.attributes['id'],
            params={'entities': {'foo': e1}},
            expect_errors=True)
        self.assertEqual(result.status_code, 400)
        self.assertIn("Entity %s does not exist" % e1, result.text)
        result = self.app.get("/v1/resource/"
                              + self.resource_type
                              + "/"
                              + self.attributes['id'])
        result = json.loads(result.text)
        self.assertEqual(result['entities'], {})

    def test_patch_resource_attributes(self):
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params=self.attributes)
        self.assertEqual(201, result.status_code)
        result = self.app.patch_json(
            "/v1/resource/" + self.resource_type
            + "/" + self.attributes['id'],
            params=self.patchable_attributes)
        self.assertEqual(result.status_code, 204)
        result = self.app.get("/v1/resource/" + self.resource_type
                              + "/" + self.attributes['id'])
        result = json.loads(result.text)
        for k, v in six.iteritems(self.patchable_attributes):
            self.assertEqual(v, result[k])

    def test_patch_resource_ended_at_before_started_at(self):
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params=self.attributes)
        self.assertEqual(201, result.status_code)
        result = self.app.patch_json(
            "/v1/resource/"
            + self.resource_type
            + "/"
            + self.attributes['id'],
            params={'ended_at': "2000-05-05 23:23:23"},
            expect_errors=True)
        self.assertEqual(result.status_code, 400)

    def test_patch_resource_no_partial_update(self):
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes)
        self.assertEqual(201, result.status_code)
        e1 = str(uuid.uuid4())
        result = self.app.patch_json(
            "/v1/resource/" + self.resource_type + "/"
            + self.attributes['id'],
            params={'ended_at': "2044-05-05 23:23:23",
                    'entities': {"foo": e1}},
            expect_errors=True)
        self.assertEqual(result.status_code, 400)
        self.assertIn("Entity %s does not exist" % e1, result.text)
        result = self.app.get("/v1/resource/"
                              + self.resource_type + "/"
                              + self.attributes['id'])
        result = json.loads(result.text)
        self.attributes['type'] = self.resource_type
        self.attributes['ended_at'] = None
        self.attributes['entities'] = {}
        self.assertEqual(self.attributes, result)

    def test_patch_resource_non_existent(self):
        result = self.app.patch_json(
            "/v1/resource/" + self.resource_type
            + "/" + str(uuid.uuid4()),
            params={},
            expect_errors=True)
        self.assertEqual(result.status_code, 404)

    def test_patch_resource_non_existent_with_body(self):
        result = self.app.patch_json(
            "/v1/resource/" + self.resource_type
            + "/" + str(uuid.uuid4()),
            params=self.patchable_attributes,
            expect_errors=True)
        self.assertEqual(result.status_code, 404)

    def test_patch_resource_unknown_field(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)
        result = self.app.patch_json(
            "/v1/resource/" + self.resource_type + "/"
            + self.attributes['id'],
            params={'foobar': 123},
            expect_errors=True)
        self.assertEqual(result.status_code, 400)
        self.assertIn(b'Invalid input: extra keys not allowed @ data['
                      + repr(u'foobar').encode('ascii') + b"]",
                      result.body)

    def test_delete_resource(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)
        result = self.app.delete("/v1/resource/" + self.resource_type + "/"
                                 + self.attributes['id'])
        self.assertEqual(204, result.status_code)

    def test_delete_resource_non_existent(self):
        result = self.app.delete("/v1/resource/" + self.resource_type + "/"
                                 + self.attributes['id'],
                                 expect_errors=True)
        self.assertEqual(400, result.status_code)
        self.assertIn(
            "Resource %s does not exist" % self.attributes['id'],
            result.text)

    def test_post_resource_invalid_uuid(self):
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params={"id": "foobar"},
                                    expect_errors=True)
        self.assertEqual(400, result.status_code)
        self.assertEqual("text/plain", result.content_type)
        self.assertIn(b"Invalid input: not a valid value "
                      b"for dictionary value @ data["
                      + repr(u'id').encode('ascii') + b"]",
                      result.body)

    def test_post_resource_with_entities(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archive_policy": "medium"})
        entity = json.loads(result.text)
        self.attributes['entities'] = {"foo": entity['id']}
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params=self.attributes)
        self.assertEqual(201, result.status_code)
        resource = json.loads(result.text)
        self.assertEqual("http://localhost/v1/resource/"
                         + self.resource_type + "/"
                         + self.attributes['id'],
                         result.headers['Location'])
        self.attributes['type'] = self.resource_type
        self.attributes['ended_at'] = None
        self.assertEqual(resource, self.attributes)

    def test_post_resource_with_null_entities(self):
        self.attributes['entities'] = {"foo": {"archive_policy": "low"}}
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params=self.attributes)
        self.assertEqual(201, result.status_code)
        resource = json.loads(result.text)
        self.assertEqual("http://localhost/v1/resource/"
                         + self.resource_type + "/"
                         + self.attributes['id'],
                         result.headers['Location'])
        self.assertEqual(resource["id"], self.attributes['id'])
        entity_id = uuid.UUID(resource['entities']['foo'])
        result = self.app.get("/v1/entity/" + str(entity_id) + "/measures")
        self.assertEqual(200, result.status_code)

    def test_list_resources_by_unknown_field(self):
        result = self.app.get("/v1/resource/" + self.resource_type,
                              params={"foo": "bar"},
                              expect_errors=True)
        self.assertEqual(400, result.status_code)
        self.assertEqual("text/plain", result.content_type)
        self.assertIn("Resource " + self.resource_type
                      + " has no foo attribute",
                      result.text)

    def test_list_resources_by_user(self):
        u1 = str(uuid.uuid4())
        self.attributes['user_id'] = u1
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes)
        created_resource = json.loads(result.text)
        result = self.app.get("/v1/resource/generic",
                              params={"user_id": u1})
        self.assertEqual(200, result.status_code)
        resources = json.loads(result.text)
        self.assertGreaterEqual(len(resources), 1)
        result = self.app.get("/v1/resource/" + self.resource_type,
                              params={"user_id": u1})
        self.assertEqual(200, result.status_code)
        resources = json.loads(result.text)
        self.assertGreaterEqual(len(resources), 1)
        self.assertEqual(created_resource, resources[0])

    def test_list_resources_by_project(self):
        p1 = str(uuid.uuid4())
        self.attributes['project_id'] = p1
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes)
        created_resource = json.loads(result.text)
        result = self.app.get("/v1/resource/generic",
                              params={"project_id": p1})
        self.assertEqual(200, result.status_code)
        resources = json.loads(result.text)
        self.assertGreaterEqual(len(resources), 1)
        result = self.app.get("/v1/resource/" + self.resource_type,
                              params={"project_id": p1})
        self.assertEqual(200, result.status_code)
        resources = json.loads(result.text)
        self.assertGreaterEqual(len(resources), 1)
        self.assertEqual(created_resource, resources[0])

    def test_list_resources(self):
        # NOTE(jd) So this test is a bit fuzzy right now as we uses the same
        # database for all tests and the tests are running concurrently, but
        # for now it'll be better than nothing.
        result = self.app.post_json(
            "/v1/resource/generic",
            params={
                "id": str(uuid.uuid4()),
                "started_at": "2014-01-01 02:02:02",
                "user_id": str(uuid.uuid4()),
                "project_id": str(uuid.uuid4()),
            })
        g = json.loads(result.text)
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes)
        i = json.loads(result.text)
        result = self.app.get("/v1/resource/generic")
        self.assertEqual(200, result.status_code)
        resources = json.loads(result.text)
        self.assertGreaterEqual(len(resources), 2)

        i_found = False
        g_found = False
        for r in resources:
            if r['id'] == str(g['id']):
                self.assertEqual(g, r)
                g_found = True
            elif r['id'] == str(i['id']):
                i_found = True
            if i_found and g_found:
                break
        else:
            self.fail("Some resources were not found")

        result = self.app.get("/v1/resource/" + self.resource_type)
        resources = json.loads(result.text)
        self.assertGreaterEqual(len(resources), 1)
        for r in resources:
            if r['id'] == str(i['id']):
                self.assertEqual(i, r)
                break
        else:
            self.fail("Some resources were not found")

    def test_list_resources_started_after(self):
        # NOTE(jd) So this test is a bit fuzzy right now as we uses the same
        # database for all tests and the tests are running concurrently, but
        # for now it'll be better than nothing.
        result = self.app.post_json(
            "/v1/resource/generic/",
            params={
                "id": str(uuid.uuid4()),
                "started_at": "2014-01-01 02:02:02",
                "user_id": str(uuid.uuid4()),
                "project_id": str(uuid.uuid4()),
            })
        g = json.loads(result.text)
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes)
        i = json.loads(result.text)
        result = self.app.get(
            "/v1/resource/generic/",
            params={"started_after": "2014-01-01"})
        self.assertEqual(200, result.status_code)
        resources = json.loads(result.text)
        self.assertGreaterEqual(len(resources), 2)

        i_found = False
        g_found = False
        for r in resources:
            if r['id'] == str(g['id']):
                self.assertEqual(g, r)
                g_found = True
            elif r['id'] == str(i['id']):
                i_found = True
            if i_found and g_found:
                break
        else:
            self.fail("Some resources were not found")

        result = self.app.get("/v1/resource/"
                              + self.resource_type
                              + "?started_after=2014-01-03")
        resources = json.loads(result.text)
        self.assertGreaterEqual(len(resources), 1)
        for r in resources:
            if r['id'] == str(i['id']):
                self.assertEqual(i, r)
                break
        else:
            self.fail("Some resources were not found")

    def test_list_resources_with_bad_details(self):
        result = self.app.get("/v1/resource/generic?details=awesome",
                              expect_errors=True)
        self.assertEqual(400, result.status_code)
        self.assertIn(
            b"Unable to parse details value in query: "
            b"Unrecognized value 'awesome', acceptable values are",
            result.body)

    def test_list_resources_with_bad_details_in_accept(self):
        result = self.app.get("/v1/resource/generic",
                              headers={
                                  "Accept": "application/json; details=foo",
                              },
                              expect_errors=True)
        self.assertEqual(400, result.status_code)
        self.assertIn(
            b"Unable to parse details value in Accept: "
            b"Unrecognized value 'foo', acceptable values are",
            result.body)

    def _do_test_list_resources_with_detail(self, request):
        # NOTE(jd) So this test is a bit fuzzy right now as we uses the same
        # database for all tests and the tests are running concurrently, but
        # for now it'll be better than nothing.
        result = self.app.post_json(
            "/v1/resource/generic",
            params={
                "id": str(uuid.uuid4()),
                "started_at": "2014-01-01 02:02:02",
                "user_id": str(uuid.uuid4()),
                "project_id": str(uuid.uuid4()),
            })
        g = json.loads(result.text)
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes)
        i = json.loads(result.text)
        result = request()
        self.assertEqual(200, result.status_code)
        resources = json.loads(result.text)
        self.assertGreaterEqual(len(resources), 2)

        i_found = False
        g_found = False
        for r in resources:
            if r['id'] == str(g['id']):
                self.assertEqual(g, r)
                g_found = True
            elif r['id'] == str(i['id']):
                i_found = True
                # Check we got all the details
                self.assertEqual(i, r)
            if i_found and g_found:
                break
        else:
            self.fail("Some resources were not found")

        result = self.app.get("/v1/resource/" + self.resource_type)
        resources = json.loads(result.text)
        self.assertGreaterEqual(len(resources), 1)
        for r in resources:
            if r['id'] == str(i['id']):
                self.assertEqual(i, r)
                break
        else:
            self.fail("Some resources were not found")

    def test_list_resources_with_details(self):
        self._do_test_list_resources_with_detail(
            lambda: self.app.get("/v1/resource/generic?details=true"))

    def test_list_resources_with_details_via_accept(self):
        self._do_test_list_resources_with_detail(
            lambda: self.app.get(
                "/v1/resource/generic",
                headers={"Accept": "application/json; details=true"}))

ResourceTest.generate_scenarios()
