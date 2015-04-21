# -*- encoding: utf-8 -*-
#
# Copyright © 2014-2015 eNovance
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
import calendar
import contextlib
import datetime
from email import utils as email_utils
import hashlib
import json
import uuid

import mock
from oslo_utils import timeutils
import pecan
import six
from six.moves.urllib import parse as urllib_parse
from stevedore import extension
import testscenarios
from testtools import testcase
import webtest

from gnocchi import archive_policy
from gnocchi.rest import app
from gnocchi import storage
from gnocchi.tests import base as tests_base


load_tests = testscenarios.load_tests_apply_scenarios


class FakeMemcache(object):
    VALID_TOKEN_ADMIN = '4562138218392830'
    USER_ID_ADMIN = str(uuid.uuid4())
    PROJECT_ID_ADMIN = str(uuid.uuid4())

    VALID_TOKEN = '4562138218392831'
    USER_ID = str(uuid.uuid4())
    PROJECT_ID = str(uuid.uuid4())

    VALID_TOKEN_2 = '4562138218392832'
    # We replace "-" to simulate a middleware that would send UUID in a non
    # normalized format.
    USER_ID_2 = str(uuid.uuid4()).replace("-", "")
    PROJECT_ID_2 = str(uuid.uuid4()).replace("-", "")

    def get(self, key):
        dt = datetime.datetime(
            year=datetime.MAXYEAR, month=12, day=31,
            hour=23, minute=59, second=59)
        if key == "tokens/%s" % self.VALID_TOKEN_ADMIN:
            return json.dumps(({'access': {
                'token': {'id': self.VALID_TOKEN_ADMIN,
                          'expires': timeutils.isotime(dt)},
                'user': {
                    'id': self.USER_ID_ADMIN,
                    'name': 'adminusername',
                    'tenantId': self.PROJECT_ID_ADMIN,
                    'tenantName': 'myadmintenant',
                    'roles': [
                        {'name': 'admin'},
                    ]},
            }}, timeutils.isotime(dt)))
        elif key == "tokens/%s" % self.VALID_TOKEN:
            return json.dumps(({'access': {
                'token': {'id': self.VALID_TOKEN,
                          'expires': timeutils.isotime(dt)},
                'user': {
                    'id': self.USER_ID,
                    'name': 'myusername',
                    'tenantId': self.PROJECT_ID,
                    'tenantName': 'mytenant',
                    'roles': [
                        {'name': 'member'},
                    ]},
            }}, timeutils.isotime(dt)))
        elif key == "tokens/%s" % self.VALID_TOKEN_2:
            return json.dumps(({'access': {
                'token': {'id': self.VALID_TOKEN_2,
                          'expires': timeutils.isotime(dt)},
                'user': {
                    'id': self.USER_ID_2,
                    'name': 'myusername2',
                    'tenantId': self.PROJECT_ID_2,
                    'tenantName': 'mytenant2',
                    'roles': [
                        {'name': 'member'},
                    ]},
            }}, timeutils.isotime(dt)))

    @staticmethod
    def set(key, value, **kwargs):
        pass


class TestingApp(webtest.TestApp):
    CACHE_NAME = 'fake.cache'

    def __init__(self, *args, **kwargs):
        self.auth = kwargs.pop('auth')
        super(TestingApp, self).__init__(*args, **kwargs)
        # Setup Keystone auth_token fake cache
        self.extra_environ.update({self.CACHE_NAME: FakeMemcache()})
        self.token = FakeMemcache.VALID_TOKEN

    @contextlib.contextmanager
    def use_admin_user(self):
        if not self.auth:
            raise testcase.TestSkipped("No auth enabled")
        old_token = self.token
        self.token = FakeMemcache.VALID_TOKEN_ADMIN
        try:
            yield
        finally:
            self.token = old_token

    @contextlib.contextmanager
    def use_another_user(self):
        if not self.auth:
            raise testcase.TestSkipped("No auth enabled")
        old_token = self.token
        self.token = FakeMemcache.VALID_TOKEN_2
        try:
            yield
        finally:
            self.token = old_token

    def do_request(self, req, *args, **kwargs):
        req.headers['X-Auth-Token'] = self.token
        return super(TestingApp, self).do_request(req, *args, **kwargs)


class RestTest(tests_base.TestCase, testscenarios.TestWithScenarios):

    scenarios = [
        ('noauth', dict(middlewares=[])),
        ('keystone', dict(
            middlewares=['keystonemiddleware.auth_token.AuthProtocol'])),
    ]

    def setUp(self):
        super(RestTest, self).setUp()
        c = {}
        c.update(app.PECAN_CONFIG)
        c['indexer'] = self.index
        c['storage'] = self.storage
        c['not_implemented_middleware'] = False
        self.conf.set_override("cache", TestingApp.CACHE_NAME,
                               group='keystone_authtoken')
        # TODO(jd) Override these options with values. They are not used, but
        # if they are None (their defaults), the keystone authtoken middleware
        # prints a warning… :( When the bug is fixed we can remove that!
        # See https://bugs.launchpad.net/keystonemiddleware/+bug/1429179
        self.conf.set_override("identity_uri", "foobar",
                               group="keystone_authtoken")
        self.conf.set_override("auth_uri", "foobar",
                               group="keystone_authtoken")

        if hasattr(self, "middlewares"):
            self.conf.set_override("middlewares",
                                   self.middlewares, group="api")

        self.app = TestingApp(pecan.load_app(c, cfg=self.conf),
                              auth=bool(self.conf.api.middlewares))

    def test_deserialize_force_json(self):
        with self.app.use_admin_user():
            self.app.post(
                "/v1/archive_policy",
                params="foo",
                status=415)

    def test_capabilities(self):
        custom_agg = extension.Extension('test_aggregation', None, None, None)
        aggregation_methods = set(
            archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS)
        aggregation_methods.add('test_aggregation')
        mgr = extension.ExtensionManager.make_test_instance(
            [custom_agg], 'gnocchi.aggregates')

        with mock.patch.object(extension, 'ExtensionManager',
                               return_value=mgr):
            result = self.app.get("/v1/capabilities")
            self.assertEqual(
                sorted(aggregation_methods),
                sorted(json.loads(result.text)['aggregation_methods']))

    @staticmethod
    def runTest():
        pass


class ArchivePolicyTest(RestTest):
    def test_policy_enforcement_before_request_validation(self):
        self.app.post_json(
            "/v1/archive_policy",
            params={"definition":
                    [{
                        "granularity": "1 minute",
                        "points": 20,
                    }]},
            status=403)

    def test_post_archive_policy(self):
        name = str(uuid.uuid4())
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params={"name": name,
                        "definition":
                        [{
                            "granularity": "1 minute",
                            "points": 20,
                        }]},
                status=201)
        self.assertEqual("application/json", result.content_type)
        ap = json.loads(result.text)
        self.assertEqual("http://localhost/v1/archive_policy/" + name,
                         result.headers['Location'])
        self.assertEqual(name, ap['name'])
        self.assertEqual([{
            "granularity": "0:01:00",
            "points": 20,
            "timespan": "0:20:00",
        }], ap['definition'])

    def test_post_archive_policy_with_agg_methods(self):
        name = str(uuid.uuid4())
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params={"name": name,
                        "aggregation_methods": ["mean"],
                        "definition":
                        [{
                            "granularity": "1 minute",
                            "points": 20,
                        }]},
                status=201)
        self.assertEqual("application/json", result.content_type)
        ap = json.loads(result.text)
        self.assertEqual(['mean'], ap['aggregation_methods'])

    def test_post_archive_policy_with_agg_methods_minus(self):
        name = str(uuid.uuid4())
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params={"name": name,
                        "aggregation_methods": ["-mean"],
                        "definition":
                        [{
                            "granularity": "1 minute",
                            "points": 20,
                        }]},
                status=201)
        self.assertEqual("application/json", result.content_type)
        ap = json.loads(result.text)
        self.assertEqual(
            (set(self.conf.archive_policy.default_aggregation_methods)
             - set(['mean'])),
            set(ap['aggregation_methods']))

    def test_post_archive_policy_as_non_admin(self):
        self.app.post_json(
            "/v1/archive_policy",
            params={"name": str(uuid.uuid4()),
                    "definition":
                    [{
                        "granularity": "1 minute",
                        "points": 20,
                    }]},
            status=403)

    def test_post_archive_policy_infinite_points(self):
        name = str(uuid.uuid4())
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params={"name": name,
                        "definition":
                        [{
                            "granularity": "2 minutes",
                        }]},
                status=201)
        self.assertEqual("application/json", result.content_type)
        ap = json.loads(result.text)
        self.assertEqual("http://localhost/v1/archive_policy/" + name,
                         result.headers['Location'])
        self.assertEqual(name, ap['name'])
        self.assertEqual([{
            "granularity": "0:02:00",
            "points": None,
            "timespan": None,
        }], ap['definition'])

    def test_post_archive_policy_invalid_multiple(self):
        name = str(uuid.uuid4())
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params={"name": name,
                        "definition":
                        [{
                            "granularity": "1 minute",
                            "points": 20,
                            "timespan": "3 hours",
                        }]},
                status=400)
        self.assertIn(u"timespan ≠ granularity × points".encode('utf-8'),
                      result.body)

    def test_post_archive_policy_unicode(self):
        name = u'æ' + str(uuid.uuid4())
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params={"name": name,
                        "definition":
                        [{
                            "granularity": "1 minute",
                            "points": 20,
                        }]},
                headers={'content-type': 'application/json; charset=UTF-8'},
                status=201)
        self.assertEqual("application/json", result.content_type)
        ap = json.loads(result.text)

        location = "/v1/archive_policy/" + name
        if six.PY2:
            location = location.encode('utf-8')
        self.assertEqual("http://localhost"
                         + urllib_parse.quote(location),
                         result.headers['Location'])
        self.assertEqual(name, ap['name'])
        self.assertEqual([{
            "granularity": "0:01:00",
            "points": 20,
            "timespan": "0:20:00",
        }], ap['definition'])

    def test_post_archive_policy_with_timespan(self):
        name = str(uuid.uuid4())
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params={"name": name,
                        "definition": [{
                            "granularity": "10s",
                            "timespan": "1 hour",
                        }]},
                status=201)
        self.assertEqual("application/json", result.content_type)
        ap = json.loads(result.text)
        self.assertEqual("http://localhost/v1/archive_policy/" + name,
                         result.headers['Location'])
        self.assertEqual(name, ap['name'])
        self.assertEqual([{'granularity': "0:00:10",
                           'points': 360,
                           'timespan': '1:00:00'}], ap['definition'])

    def test_post_archive_policy_with_timespan_float_points(self):
        name = str(uuid.uuid4())
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params={"name": name,
                        "definition": [{
                            "granularity": "7s",
                            "timespan": "1 hour",
                        }]},
                status=201)
        self.assertEqual("application/json", result.content_type)
        ap = json.loads(result.text)
        self.assertEqual("http://localhost/v1/archive_policy/" + name,
                         result.headers['Location'])
        self.assertEqual(name, ap['name'])
        self.assertEqual([{'granularity': "0:00:07",
                           'points': 514,
                           'timespan': '0:59:58'}], ap['definition'])

    def test_post_archive_policy_with_timespan_float_granularity(self):
        name = str(uuid.uuid4())
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params={"name": name,
                        "definition": [{
                            "points": 1000,
                            "timespan": "1 hour",
                        }]},
                status=201)
        self.assertEqual("application/json", result.content_type)
        ap = json.loads(result.text)
        self.assertEqual("http://localhost/v1/archive_policy/" + name,
                         result.headers['Location'])
        self.assertEqual(name, ap['name'])
        self.assertEqual([{'granularity': "0:00:04",
                           'points': 1000,
                           'timespan': '1:06:40'}], ap['definition'])

    def test_post_archive_policy_with_timespan_and_points(self):
        name = str(uuid.uuid4())
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params={"name": name,
                        "definition": [{
                            "points": 1800,
                            "timespan": "1 hour",
                        }]},
                status=201)
        self.assertEqual("application/json", result.content_type)
        ap = json.loads(result.text)
        self.assertEqual("http://localhost/v1/archive_policy/" + name,
                         result.headers['Location'])
        self.assertEqual(name, ap['name'])
        self.assertEqual([{'granularity': "0:00:02",
                           'points': 1800,
                           'timespan': '1:00:00'}], ap['definition'])

    def test_post_archive_policy_invalid_unit(self):
        with self.app.use_admin_user():
            self.app.post_json(
                "/v1/archive_policy",
                params={"name": str(uuid.uuid4()),
                        "definition": [{
                            "granularity": "10s",
                            "timespan": "1 shenanigan",
                        }]},
                status=400)

    def test_post_archive_policy_and_metric(self):
        ap = str(uuid.uuid4())
        with self.app.use_admin_user():
            self.app.post_json(
                "/v1/archive_policy",
                params={"name": ap,
                        "definition": [{
                            "granularity": "10s",
                            "points": 20,
                        }]},
                status=201)
        self.app.post_json(
            "/v1/metric",
            params={"archive_policy_name": ap},
            status=201)

    def test_post_archive_policy_wrong_value(self):
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params={"name": "somenewname",
                        "definition": "foobar"},
                status=400)
        self.assertIn(b'Invalid input: expected a list '
                      b'for dictionary value @ data['
                      + repr(u'definition').encode('ascii') + b"]",
                      result.body)

    def test_post_archive_already_exists(self):
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params={"name": "high",
                        "definition": [{
                            "granularity": "10s",
                            "points": 20,
                        }]},
                status=409)
        self.assertIn('Archive policy high already exists', result.text)

    def test_create_archive_policy_with_granularity_integer(self):
        params = {"name": str(uuid.uuid4()),
                  "back_window": 0,
                  "definition": [{
                      "granularity": 10,
                      "points": 20,
                  }]}
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params=params,
                status=201)
        ap = json.loads(result.text)
        params['definition'][0]['timespan'] = u'0:03:20'
        params['definition'][0]['granularity'] = u'0:00:10'
        self.assertEqual(
            set(self.conf.archive_policy.default_aggregation_methods),
            set(ap['aggregation_methods']))
        del ap['aggregation_methods']
        self.assertEqual(params, ap)

    def test_create_archive_policy_with_back_window(self):
        params = {"name": str(uuid.uuid4()),
                  "back_window": 1,
                  "definition": [{
                      "granularity": "10s",
                      "points": 20,
                  }]}
        with self.app.use_admin_user():
            result = self.app.post_json(
                "/v1/archive_policy",
                params=params,
                status=201)
        ap = json.loads(result.text)
        params['definition'][0]['timespan'] = u'0:03:20'
        params['definition'][0]['granularity'] = u'0:00:10'
        self.assertEqual(
            set(self.conf.archive_policy.default_aggregation_methods),
            set(ap['aggregation_methods']))
        del ap['aggregation_methods']
        self.assertEqual(params, ap)

    def test_get_archive_policy(self):
        result = self.app.get("/v1/archive_policy/medium")
        ap = json.loads(result.text)
        ap_dict = self.archive_policies['medium'].jsonify()
        ap_dict['definition'] = [
            archive_policy.ArchivePolicyItem(**d).jsonify()
            for d in ap_dict['definition']
        ]
        self.assertEqual(set(ap['aggregation_methods']),
                         ap_dict['aggregation_methods'])
        del ap['aggregation_methods']
        del ap_dict['aggregation_methods']
        self.assertEqual(ap_dict, ap)

    def test_delete_archive_policy(self):
        params = {"name": str(uuid.uuid4()),
                  "back_window": 1,
                  "definition": [{
                      "granularity": "10s",
                      "points": 20,
                  }]}
        with self.app.use_admin_user():
            self.app.post_json(
                "/v1/archive_policy",
                params=params)
            self.app.delete("/v1/archive_policy/%s" % params['name'],
                            status=204)

    def test_delete_archive_policy_non_existent(self):
        ap = str(uuid.uuid4())
        with self.app.use_admin_user():
            result = self.app.delete("/v1/archive_policy/%s" % ap,
                                     status=404)
        self.assertIn(
            b"Archive policy " + ap.encode('ascii') + b" does not exist",
            result.body)

    def test_delete_archive_policy_in_use(self):
        ap = str(uuid.uuid4())
        params = {"name": ap,
                  "back_window": 1,
                  "definition": [{
                      "granularity": "10s",
                      "points": 20,
                  }]}
        with self.app.use_admin_user():
            self.app.post_json(
                "/v1/archive_policy",
                params=params)
        self.app.post_json("/v1/metric",
                           params={"archive_policy_name": ap})
        with self.app.use_admin_user():
            result = self.app.delete("/v1/archive_policy/%s" % ap,
                                     status=400)
        self.assertIn(
            b"Archive policy " + ap.encode('ascii') + b" is still in use",
            result.body)

    def test_get_archive_policy_non_existent(self):
        with self.app.use_admin_user():
            self.app.get("/v1/archive_policy/" + str(uuid.uuid4()),
                         status=404)

    def test_list_archive_policy(self):
        result = self.app.get("/v1/archive_policy")
        aps = json.loads(result.text)
        # Transform list to set
        for ap in aps:
            ap['aggregation_methods'] = set(ap['aggregation_methods'])
        for name, ap in six.iteritems(self.archive_policies):
            apj = ap.jsonify()
            apj['definition'] = [
                archive_policy.ArchivePolicyItem(**d).jsonify()
                for d in ap.definition
            ]
            self.assertIn(apj, aps)


class MetricTest(RestTest):
    def test_post_metric(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"},
                                    status=201)
        self.assertEqual("application/json", result.content_type)
        metric = json.loads(result.text)
        self.assertEqual("http://localhost/v1/metric/" + metric['id'],
                         result.headers['Location'])
        self.assertEqual("medium", metric['archive_policy']['name'])

    def test_get_metric(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"},
                                    status=201)
        self.assertEqual("application/json", result.content_type)

        result = self.app.get(result.headers['Location'], status=200)
        metric = json.loads(result.text)
        self.assertNotIn('archive_policy_name', metric)
        ap = metric['archive_policy']
        ap_ref = self.archive_policies['medium']
        self.assertEqual(ap_ref.aggregation_methods,
                         set(ap['aggregation_methods']))
        self.assertEqual(ap_ref.back_window, ap['back_window'])
        self.assertEqual(ap_ref.name, ap['name'])
        self.assertEqual([d.jsonify() for d in ap_ref.definition],
                         ap['definition'])

    def test_get_metric_with_another_user(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"},
                                    status=201)
        self.assertEqual("application/json", result.content_type)

        with self.app.use_another_user():
            self.app.get(result.headers['Location'], status=403)

    def test_get_metric_with_wrong_metric_id(self):
        fake_metric_id = uuid.uuid4()
        self.app.get("/v1/metric/%s" % fake_metric_id, status=404)

    def test_post_metric_wrong_archive_policy(self):
        policy = str(uuid.uuid4())
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": policy},
                                    status=400)
        self.assertIn('Unknown archive policy %s' % policy, result.text)

    def test_post_archive_policy_no_mean(self):
        """Test that we have a 404 if mean is not in AP."""
        ap = str(uuid.uuid4())
        with self.app.use_admin_user():
            self.app.post_json(
                "/v1/archive_policy",
                params={"name": ap,
                        "aggregation_methods": ["max"],
                        "definition": [{
                            "granularity": "10s",
                            "points": 20,
                        }]},
                status=201)
        result = self.app.post_json(
            "/v1/metric",
            params={"archive_policy_name": ap},
            status=201)
        metric = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric['id'],
                           params=[{"timestamp": '2013-01-01 12:00:01',
                                    "value": 8},
                                   {"timestamp": '2013-01-01 12:00:02',
                                    "value": 16}])
        self.app.get("/v1/metric/%s/measures" % metric['id'],
                     status=404)

    def test_list_metric(self):
        result = self.app.post_json(
            "/v1/metric",
            params={"archive_policy_name": "medium"},
            status=201)
        metric = json.loads(result.text)
        result = self.app.get("/v1/metric")
        self.assertIn(metric['id'],
                      [r['id'] for r in json.loads(result.text)])
        # Only test that if we have auth enabled
        if self.middlewares:
            result = self.app.get("/v1/metric?user_id=" + FakeMemcache.USER_ID)
            self.assertIn(metric['id'],
                          [r['id'] for r in json.loads(result.text)])

    def test_list_metric_filter_as_admin(self):
        result = self.app.post_json(
            "/v1/metric",
            params={"archive_policy_name": "medium"})
        metric = json.loads(result.text)
        with self.app.use_admin_user():
            result = self.app.get("/v1/metric?user_id=" + FakeMemcache.USER_ID)
        self.assertIn(metric['id'],
                      [r['id'] for r in json.loads(result.text)])

    def test_list_metric_invalid_user(self):
        result = self.app.get("/v1/metric?user_id=" + FakeMemcache.USER_ID_2,
                              status=403)
        self.assertIn("Insufficient privileges to filter by user/project",
                      result.text)

    def test_delete_metric(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"})
        metric = json.loads(result.text)
        result = self.app.delete("/v1/metric/" + metric['id'], status=204)

    def test_delete_metric_another_user(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"})
        metric = json.loads(result.text)
        with self.app.use_another_user():
            self.app.delete("/v1/metric/" + metric['id'], status=403)

    def test_delete_metric_non_existent(self):
        e1 = str(uuid.uuid4())
        self.app.delete("/v1/metric/" + e1, status=404)

    def test_post_metric_bad_archives(self):
        result = self.app.post_json(
            "/v1/metric",
            params={"archive_policy_name": 'foobar123'},
            status=400)
        self.assertEqual("text/plain", result.content_type)
        self.assertIn(b"Unknown archive policy foobar123", result.body)

    def test_add_measure(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "high"})
        metric = json.loads(result.text)
        result = self.app.post_json(
            "/v1/metric/%s/measures" % metric['id'],
            params=[{"timestamp": '2013-01-01 23:23:23',
                     "value": 1234.2}],
            status=204)

    def test_add_measure_with_another_user(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "high"})
        metric = json.loads(result.text)
        with self.app.use_another_user():
            self.app.post_json(
                "/v1/metric/%s/measures" % metric['id'],
                params=[{"timestamp": '2013-01-01 23:23:23',
                         "value": 1234.2}],
                status=403)

    def test_add_multiple_measures_per_metric(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "high"},
                                    status=201)
        metric = json.loads(result.text)
        for x in range(5):
            result = self.app.post_json(
                "/v1/metric/%s/measures" % metric['id'],
                params=[{"timestamp": '2013-01-01 23:23:2%d' % x,
                         "value": 1234.2 + x}],
                status=204)

    def test_add_measure_no_such_metric(self):
        e1 = str(uuid.uuid4())
        self.app.post_json(
            "/v1/metric/%s/measures" % e1,
            params=[{"timestamp": '2013-01-01 23:23:23',
                     "value": 1234.2}],
            status=404)

    def test_add_measures_back_window(self):
        ap_name = str(uuid.uuid4())
        with self.app.use_admin_user():
            self.app.post_json(
                "/v1/archive_policy",
                params={"name": ap_name,
                        "back_window": 2,
                        "definition":
                        [{
                            "granularity": "1 minute",
                            "points": 20,
                        }]},
                status=201)
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": ap_name})
        metric = json.loads(result.text)
        self.app.post_json(
            "/v1/metric/%s/measures" % metric['id'],
            params=[{"timestamp": '2013-01-01 23:30:23',
                     "value": 1234.2}],
            status=204)
        self.app.post_json(
            "/v1/metric/%s/measures" % metric['id'],
            params=[{"timestamp": '2013-01-01 23:29:23',
                     "value": 1234.2}],
            status=204)
        self.app.post_json(
            "/v1/metric/%s/measures" % metric['id'],
            params=[{"timestamp": '2013-01-01 23:28:23',
                     "value": 1234.2}],
            status=204)
        result = self.app.post_json(
            "/v1/metric/%s/measures" % metric['id'],
            params=[{"timestamp": '2012-01-01 23:27:23',
                     "value": 1234.2}],
            status=400)
        self.assertIn(
            b"The measure for 2012-01-01 23:27:23 is too old considering "
            b"the archive policy used by this metric. "
            b"It can only go back to 2013-01-01 23:28:00.",
            result.body)

        ret = self.app.get("/v1/metric/%s/measures" % metric['id'])
        result = json.loads(ret.text)
        self.assertEqual(
            [[u'2013-01-01T23:28:00.000000Z', 60.0, 1234.2],
             [u'2013-01-01T23:29:00.000000Z', 60.0, 1234.2],
             [u'2013-01-01T23:30:00.000000Z', 60.0, 1234.2]],
            result)

    def test_add_measures_too_old(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "low"})
        metric = json.loads(result.text)
        result = self.app.post_json(
            "/v1/metric/%s/measures" % metric['id'],
            params=[{"timestamp": '2013-01-01 23:23:23',
                     "value": 1234.2}],
            status=204)

        result = self.app.post_json(
            "/v1/metric/%s/measures" % metric['id'],
            params=[{"timestamp": '2012-01-01 23:23:23',
                     "value": 1234.2}],
            status=400)
        self.assertIn(
            b"The measure for 2012-01-01 23:23:23 is too old considering "
            b"the archive policy used by this metric. "
            b"It can only go back to 2013-01-01 00:00:00",
            result.body)

    def test_get_measure(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "low"})
        metric = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric['id'],
                           params=[{"timestamp": '2013-01-01 23:23:23',
                                    "value": 1234.2}])
        ret = self.app.get("/v1/metric/%s/measures" % metric['id'], status=200)
        result = json.loads(ret.text)
        self.assertEqual(
            [[u'2013-01-01T00:00:00.000000Z', 86400.0, 1234.2],
             [u'2013-01-01T23:00:00.000000Z', 3600.0, 1234.2],
             [u'2013-01-01T23:20:00.000000Z', 300.0, 1234.2]],
            result)

    def test_get_measure_unknown_metric(self):
        metric_id = "cee6ef1f-52cc-4a16-bbb5-648aedfd1c37"
        self.app.get("/v1/metric/%s/measures" % metric_id, status=404)

    def test_get_measure_unknown_aggregation(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "low"})
        metric = json.loads(result.text)
        ret = self.app.get("/v1/metric/%s/measures?aggregation=last" %
                           metric['id'], status=404)
        self.assertIn("Aggregation method 'last' for metric %s does not "
                      "exist" % metric['id'], ret.text)

    def test_aggregation_get_measure_unknown_metric(self):
        metric_id = str(uuid.uuid4())
        ret = self.app.get("/v1/aggregation/metric?metric=%s" % metric_id,
                           status=404)
        self.assertIn('Metric %s does not exist' % metric_id, ret.text)

    def test_aggregation_get_measure_unknown_aggregation(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "low"})
        metric = json.loads(result.text)
        ret = self.app.get("/v1/aggregation/metric?metric=%s&"
                           "aggregation=last" % metric['id'], status=404)
        self.assertIn("Aggregation method 'last' for metric %s does not "
                      "exist" % metric['id'], ret.text)

    def test_get_measure_with_another_user(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "low"})
        metric = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric['id'],
                           params=[{"timestamp": '2013-01-01 23:23:23',
                                    "value": 1234.2}])
        with self.app.use_another_user():
            self.app.get("/v1/metric/%s/measures" % metric['id'],
                         status=403)

    def test_get_measure_start(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "high"})
        metric = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric['id'],
                           params=[{"timestamp": '2013-01-01 23:23:23',
                                    "value": 1234.2}])
        ret = self.app.get(
            "/v1/metric/%s/measures?start=2013-01-01 23:23:20"
            % metric['id'],
            status=200)
        result = json.loads(ret.text)
        self.assertEqual([['2013-01-01T23:23:23.000000Z', 1.0, 1234.2]],
                         result)

    def test_get_measure_start_relative(self):
        """Make sure the timestamps can be relative to now."""
        # TODO(jd) Use a fixture as soon as there's one
        timeutils.set_time_override(datetime.datetime(2014, 1, 1, 10, 23))
        self.addCleanup(timeutils.clear_time_override)
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "high"})
        metric = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric['id'],
                           params=[{"timestamp": timeutils.isotime(),
                                    "value": 1234.2}])
        ret = self.app.get(
            "/v1/metric/%s/measures?start=-10 minutes"
            % metric['id'],
            status=200)
        result = json.loads(ret.text)
        now = timeutils.utcnow()
        self.assertEqual([
            [timeutils.isotime(now
                               - datetime.timedelta(
                                   seconds=now.second,
                                   microseconds=now.microsecond),
                               subsecond=True),
             60.0, 1234.2],
            [timeutils.isotime(now
                               - datetime.timedelta(
                                   microseconds=now.microsecond),
                               subsecond=True), 1.0, 1234.2]], result)

    def test_get_measure_stop(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "high"})
        metric = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric['id'],
                           params=[{"timestamp": '2013-01-01 12:00:00',
                                    "value": 1234.2},
                                   {"timestamp": '2013-01-01 12:00:02',
                                    "value": 456}])
        ret = self.app.get("/v1/metric/%s/measures"
                           "?stop=2013-01-01 12:00:01" % metric['id'],
                           status=200)
        result = json.loads(ret.text)
        self.assertEqual(
            [[u'2013-01-01T12:00:00.000000Z', 3600.0, 845.1],
             [u'2013-01-01T12:00:00.000000Z', 60.0, 845.1],
             [u'2013-01-01T12:00:00.000000Z', 1.0, 1234.2]],
            result)

    def test_get_measure_aggregation(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"})
        metric = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric['id'],
                           params=[{"timestamp": '2013-01-01 12:00:01',
                                    "value": 123.2},
                                   {"timestamp": '2013-01-01 12:00:03',
                                    "value": 12345.2},
                                   {"timestamp": '2013-01-01 12:00:02',
                                    "value": 1234.2}])
        ret = self.app.get(
            "/v1/metric/%s/measures?aggregation=max" % metric['id'],
            status=200)
        result = json.loads(ret.text)
        self.assertEqual([[u'2013-01-01T00:00:00.000000Z', 86400.0, 12345.2],
                          [u'2013-01-01T12:00:00.000000Z', 3600.0, 12345.2],
                          [u'2013-01-01T12:00:00.000000Z', 60.0, 12345.2]],
                         result)

    def test_get_moving_average(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"})
        metric = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric['id'],
                           params=[{"timestamp": '2013-01-01 12:00:00',
                                    "value": 69},
                                   {"timestamp": '2013-01-01 12:00:20',
                                    "value": 42},
                                   {"timestamp": '2013-01-01 12:00:40',
                                    "value": 6},
                                   {"timestamp": '2013-01-01 12:01:00',
                                    "value": 44},
                                   {"timestamp": '2013-01-01 12:01:20',
                                    "value": 7}])

        path = "/v1/metric/%s/measures?aggregation=%s&window=%ds"
        ret = self.app.get(path % (metric['id'], 'moving-average', 120),
                           status=200)
        result = json.loads(ret.text)
        expected = [[u'2013-01-01T12:00:00.000000Z', 120.0, 32.25]]
        self.assertEqual(expected, result)
        ret = self.app.get(path % (metric['id'], 'moving-average', 90),
                           status=400)
        self.assertIn('No data available that is either full-res',
                      ret.text)
        path = "/v1/metric/%s/measures?aggregation=%s"
        ret = self.app.get(path % (metric['id'], 'moving-average'),
                           status=400)
        self.assertIn('Moving aggregate must have window specified',
                      ret.text)

    def test_get_moving_average_invalid_window(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"})
        metric = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric['id'],
                           params=[{"timestamp": '2013-01-01 12:00:00',
                                    "value": 69},
                                   {"timestamp": '2013-01-01 12:00:20',
                                    "value": 42},
                                   {"timestamp": '2013-01-01 12:00:40',
                                    "value": 6},
                                   {"timestamp": '2013-01-01 12:01:00',
                                    "value": 44},
                                   {"timestamp": '2013-01-01 12:01:20',
                                    "value": 7}])

        path = "/v1/metric/%s/measures?aggregation=%s&window=foobar"
        ret = self.app.get(path % (metric['id'], 'moving-average'),
                           status=400)
        self.assertIn('Invalid value for window', ret.text)

    def test_get_resource_missing_named_metric_measure_aggregation(self):
        attributes = {
            "started_at": "2014-01-03T02:02:02.000000",
            "host": "foo",
            "image_ref": "imageref!",
            "flavor_id": 123,
            "display_name": "myinstance",
            "server_group": str(uuid.uuid4()),
        }
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"})
        metric1 = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric1['id'],
                           params=[{"timestamp": '2013-01-01 12:00:01',
                                    "value": 8},
                                   {"timestamp": '2013-01-01 12:00:02',
                                    "value": 16}])

        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"})
        metric2 = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric2['id'],
                           params=[{"timestamp": '2013-01-01 12:00:01',
                                    "value": 0},
                                   {"timestamp": '2013-01-01 12:00:02',
                                    "value": 4}])

        attributes['id'] = str(uuid.uuid4())
        attributes['metrics'] = {'foo': metric1['id']}
        self.app.post_json("/v1/resource/instance",
                           params=attributes)

        attributes['id'] = str(uuid.uuid4())
        attributes['metrics'] = {'bar': metric2['id']}
        self.app.post_json("/v1/resource/instance",
                           params=attributes)

        result = self.app.post_json(
            "/v1/aggregation/resource/instance/metric/foo?aggregation=max",
            params={"=": {"server_group": attributes['server_group']}})

        measures = json.loads(result.text)
        self.assertEqual([[u'2013-01-01T00:00:00.000000Z', 86400.0, 16.0],
                          [u'2013-01-01T12:00:00.000000Z', 3600.0, 16.0],
                          [u'2013-01-01T12:00:00.000000Z', 60.0, 16.0]],
                         measures)

    def test_search_value(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "high"})
        metric = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric['id'],
                           params=[{"timestamp": '2013-01-01 12:00:00',
                                    "value": 1234.2},
                                   {"timestamp": '2013-01-01 12:00:02',
                                    "value": 456}])
        metric1 = metric['id']
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "high"})
        metric = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric['id'],
                           params=[{"timestamp": '2013-01-01 12:30:00',
                                    "value": 1234.2},
                                   {"timestamp": '2013-01-01 12:00:02',
                                    "value": 456}])
        metric2 = metric['id']

        ret = self.app.post_json(
            "/v1/search/metric?metric_id=%s&metric_id=%s"
            "&stop=2013-01-01 12:10:00" % (metric1, metric2),
            params={u"∧": [{u"≥": 1000}]},
            status=200)
        result = json.loads(ret.text)
        self.assertEqual(
            {metric1: [[u'2013-01-01T12:00:00.000000Z', 1.0, 1234.2]],
             metric2: []},
            result)


class ResourceTest(RestTest):

    resource_scenarios = [
        ('generic', dict(
            attributes={
                "started_at": "2014-01-03T02:02:02.000000Z",
                "user_id": str(uuid.uuid4()),
                "project_id": str(uuid.uuid4()),
            },
            patchable_attributes={
                "ended_at": "2014-01-03T02:02:02.000000Z",
            },
            resource_type='generic')),
        ('instance', dict(
            attributes={
                "started_at": "2014-01-03T02:02:02.000000Z",
                # NOTE(jd) We test this one without user_id/project_id!
                # Just to test that use case. :)
                "host": "foo",
                "image_ref": "imageref!",
                "flavor_id": 123,
                "display_name": "myinstance",
                "server_group": "as_group",
            },
            patchable_attributes={
                "ended_at": "2014-01-03T02:02:02.000000Z",
                "host": "fooz",
                "image_ref": "imageref!z",
                "flavor_id": 1234,
                "display_name": "myinstancez",
                "server_group": "new_as_group",
            },
            resource_type='instance')),
        # swift notifications contain UUID user_id
        ('swift_account', dict(
            attributes={
                "started_at": "2014-01-03T02:02:02.000000Z",
                "user_id": str(uuid.uuid4()),
                "project_id": str(uuid.uuid4()),
            },
            patchable_attributes={
                "ended_at": "2014-01-03T02:02:02.000000Z",
            },
            resource_type='swift_account')),
        # swift pollsters contain None user_id
        ('swift_account_none_user', dict(
            attributes={
                "started_at": "2014-01-03T02:02:02.000000Z",
                "user_id": None,
                "project_id": str(uuid.uuid4()),
            },
            patchable_attributes={
                "ended_at": "2014-01-03T02:02:02.000000Z",
            },
            resource_type='swift_account')),
        # TODO(dbelova): add tests with None project ID when we'll add kwapi,
        # ipmi, hardware, etc. resources that are passed without project ID
        ('volume', dict(
            attributes={
                "started_at": "2014-01-03T02:02:02.000000Z",
                "user_id": str(uuid.uuid4()),
                "project_id": str(uuid.uuid4()),
                "display_name": "test_volume",
            },
            patchable_attributes={
                "ended_at": "2014-01-03T02:02:02.000000Z",
                "display_name": "myvolume",
            },
            resource_type='volume')),
        ('ceph_account', dict(
            attributes={
                "started_at": "2014-01-03T02:02:02.000000Z",
                "user_id": str(uuid.uuid4()),
                "project_id": str(uuid.uuid4()),
            },
            patchable_attributes={
                "ended_at": "2014-01-03T02:02:02.000000Z",
            },
            resource_type='ceph_account')),
        ('network', dict(
            attributes={
                "started_at": "2014-01-03T02:02:02.000000Z",
                "user_id": str(uuid.uuid4()),
                "project_id": str(uuid.uuid4()),
            },
            patchable_attributes={
                "ended_at": "2014-01-03T02:02:02.000000Z",
            },
            resource_type='network')),
        ('identity', dict(
            attributes={
                "started_at": "2014-01-03T02:02:02.000000Z",
                "user_id": str(uuid.uuid4()),
                "project_id": str(uuid.uuid4()),
            },
            patchable_attributes={
                "ended_at": "2014-01-03T02:02:02.000000Z",
            },
            resource_type='identity')),
        ('ipmi', dict(
            attributes={
                "started_at": "2014-01-03T02:02:02.000000Z",
                "user_id": str(uuid.uuid4()),
                "project_id": str(uuid.uuid4()),
            },
            patchable_attributes={
                "ended_at": "2014-01-03T02:02:02.000000Z",
            },
            resource_type='ipmi')),
        ('stack', dict(
            attributes={
                "started_at": "2014-01-03T02:02:02.000000Z",
                "user_id": str(uuid.uuid4()),
                "project_id": str(uuid.uuid4()),
            },
            patchable_attributes={
                "ended_at": "2014-01-03T02:02:02.000000Z",
            },
            resource_type='stack')),
        # image pollsters contain UUID user_id
        ('image', dict(
            attributes={
                "started_at": "2014-01-03T02:02:02.000000Z",
                "user_id": str(uuid.uuid4()),
                "project_id": str(uuid.uuid4()),
                "name": "test-image",
                "container_format": "aki",
                "disk_format": "aki",
            },
            patchable_attributes={
                "ended_at": "2014-01-03T02:02:02.000000Z",
            },
            resource_type='image')),
        # image pollsters contain None user_id
        ('image_none_user', dict(
            attributes={
                "started_at": "2014-01-03T02:02:02.000000Z",
                "user_id": None,
                "project_id": str(uuid.uuid4()),
                "name": "test-image2",
                "container_format": "aki",
                "disk_format": "aki",
            },
            patchable_attributes={
                "ended_at": "2014-01-03T02:02:02.000000Z",
            },
            resource_type='image')),
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
        self.resource = self.attributes.copy()
        if self.middlewares:
            self.resource['created_by_user_id'] = FakeMemcache.USER_ID
            self.resource['created_by_project_id'] = FakeMemcache.PROJECT_ID
        else:
            self.resource['created_by_user_id'] = None
            self.resource['created_by_project_id'] = None
        self.resource['type'] = self.resource_type
        self.resource['ended_at'] = None
        self.resource['metrics'] = {}
        if 'user_id' not in self.resource:
            self.resource['user_id'] = None
        if 'project_id' not in self.resource:
            self.resource['project_id'] = None

    def test_post_resource(self):
        # TODO(jd) Use a fixture as soon as there's one
        timeutils.set_time_override(datetime.datetime(2014, 1, 1, 10, 23))
        self.addCleanup(timeutils.clear_time_override)
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes,
            status=201)
        resource = json.loads(result.text)
        self.assertEqual("http://localhost/v1/resource/"
                         + self.resource_type + "/" + self.attributes['id'],
                         result.headers['Location'])
        self.assertIsNone(resource['revision_end'])
        self.assertEqual(resource['revision_start'],
                         "2014-01-01T10:23:00.000000Z")
        self._check_etag(result, resource)
        del resource['revision_start']
        del resource['revision_end']
        self.assertEqual(self.resource, resource)

    def test_post_resource_with_invalid_metric(self):
        metric_id = str(uuid.uuid4())
        self.attributes['metrics'] = {"foo": metric_id}
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes,
            status=400)
        self.assertIn("Metric %s does not exist" % metric_id,
                      result.text)

    def test_post_resource_with_metric_from_other_user(self):
        with self.app.use_another_user():
            metric = self.app.post_json(
                "/v1/metric",
                params={'archive_policy_name': "high"})
        metric_id = json.loads(metric.text)['id']
        self.attributes['metrics'] = {"foo": metric_id}
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes,
            status=400)
        self.assertIn("Metric %s does not exist" % metric_id,
                      result.text)

    def test_post_resource_already_exist(self):
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes,
            status=201)
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes,
            status=409)
        self.assertIn("Resource %s already exists" % self.attributes['id'],
                      result.text)

    def test_post_unix_timestamp(self):
        self.attributes['started_at'] = "1400580045.856219"
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes,
            status=201)
        resource = json.loads(result.text)
        self.assertEqual(u"2014-05-20T10:00:45.856219Z",
                         resource['started_at'])

    def test_post_invalid_timestamp(self):
        self.attributes['started_at'] = "2014-01-01 02:02:02"
        self.attributes['ended_at'] = "2013-01-01 02:02:02"
        self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes,
            status=400)

    @staticmethod
    def _strtime_to_httpdate(dt):
        return email_utils.formatdate(calendar.timegm(
            timeutils.parse_isotime(dt).timetuple()), usegmt=True)

    def _check_etag(self, response, resource):
        lastmodified = self._strtime_to_httpdate(resource['revision_start'])
        etag = hashlib.sha1()
        etag.update(resource['id'].encode('utf-8'))
        etag.update(resource['revision_start'].encode('utf8'))
        self.assertEqual(response.headers['Last-Modified'], lastmodified)
        self.assertEqual(response.headers['ETag'], '"%s"' % etag.hexdigest())

    def test_get_resource(self):
        # TODO(jd) Use a fixture as soon as there's one
        timeutils.set_time_override(datetime.datetime(2014, 1, 1, 10, 23))
        self.addCleanup(timeutils.clear_time_override)
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params=self.attributes,
                                    status=201)
        result = self.app.get("/v1/resource/"
                              + self.resource_type
                              + "/"
                              + self.attributes['id'])
        resource = json.loads(result.text)
        self.assertIsNone(resource['revision_end'])
        self.assertEqual(resource['revision_start'],
                         "2014-01-01T10:23:00.000000Z")
        self._check_etag(result, resource)
        del resource['revision_start']
        del resource['revision_end']
        self.assertEqual(self.resource, resource)

    def test_get_resource_etag(self):
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params=self.attributes,
                                    status=201)
        result = self.app.get("/v1/resource/"
                              + self.resource_type
                              + "/"
                              + self.attributes['id'])
        resource = json.loads(result.text)
        etag = hashlib.sha1()
        etag.update(resource['id'].encode('utf-8'))
        etag.update(resource['revision_start'].encode('utf-8'))
        etag = etag.hexdigest()
        lastmodified = self._strtime_to_httpdate(resource['revision_start'])
        oldlastmodified = self._strtime_to_httpdate("2000-01-01 00:00:00")

        # if-match and if-unmodified-since
        self.app.get("/v1/resource/" + self.resource_type
                     + "/" + self.attributes['id'],
                     headers={'if-match': 'fake'},
                     status=412)
        self.app.get("/v1/resource/" + self.resource_type
                     + "/" + self.attributes['id'],
                     headers={'if-match': etag},
                     status=200)
        self.app.get("/v1/resource/" + self.resource_type
                     + "/" + self.attributes['id'],
                     headers={'if-unmodified-since': lastmodified},
                     status=200)
        self.app.get("/v1/resource/" + self.resource_type
                     + "/" + self.attributes['id'],
                     headers={'if-unmodified-since': oldlastmodified},
                     status=412)
        # Some case with '*'
        self.app.get("/v1/resource/" + self.resource_type
                     + "/" + self.attributes['id'],
                     headers={'if-none-match': '*'},
                     status=304)
        self.app.get("/v1/resource/" + self.resource_type
                     + "/wrongid",
                     headers={'if-none-match': '*'},
                     status=404)
        # always prefers if-match if both provided
        self.app.get("/v1/resource/" + self.resource_type
                     + "/" + self.attributes['id'],
                     headers={'if-match': etag,
                              'if-unmodified-since': lastmodified},
                     status=200)
        self.app.get("/v1/resource/" + self.resource_type
                     + "/" + self.attributes['id'],
                     headers={'if-match': etag,
                              'if-unmodified-since': oldlastmodified},
                     status=200)
        self.app.get("/v1/resource/" + self.resource_type
                     + "/" + self.attributes['id'],
                     headers={'if-match': '*',
                              'if-unmodified-since': oldlastmodified},
                     status=200)

        # if-none-match and if-modified-since
        self.app.get("/v1/resource/" + self.resource_type
                     + "/" + self.attributes['id'],
                     headers={'if-none-match': etag},
                     status=304)
        self.app.get("/v1/resource/" + self.resource_type
                     + "/" + self.attributes['id'],
                     headers={'if-none-match': 'fake'},
                     status=200)
        self.app.get("/v1/resource/" + self.resource_type
                     + "/" + self.attributes['id'],
                     headers={'if-modified-since': lastmodified},
                     status=304)
        self.app.get("/v1/resource/" + self.resource_type
                     + "/" + self.attributes['id'],
                     headers={'if-modified-since': oldlastmodified},
                     status=200)
        # always prefers if-none-match if both provided
        self.app.get("/v1/resource/" + self.resource_type
                     + "/" + self.attributes['id'],
                     headers={'if-modified-since': oldlastmodified,
                              'if-none-match': etag},
                     status=304)
        self.app.get("/v1/resource/" + self.resource_type
                     + "/" + self.attributes['id'],
                     headers={'if-modified-since': oldlastmodified,
                              'if-none-match': '*'},
                     status=304)
        self.app.get("/v1/resource/" + self.resource_type
                     + "/" + self.attributes['id'],
                     headers={'if-modified-since': lastmodified,
                              'if-none-match': '*'},
                     status=304)
        # Some case with '*'
        self.app.get("/v1/resource/" + self.resource_type
                     + "/" + self.attributes['id'],
                     headers={'if-match': '*'},
                     status=200)
        self.app.get("/v1/resource/" + self.resource_type
                     + "/wrongid",
                     headers={'if-match': '*'},
                     status=404)

        # if-none-match and if-match
        self.app.get("/v1/resource/" + self.resource_type
                     + "/" + self.attributes['id'],
                     headers={'if-none-match': etag,
                              'if-match': etag},
                     status=304)

        # if-none-match returns 412 instead 304 for PUT/PATCH/DELETE
        self.app.patch_json("/v1/resource/" + self.resource_type
                            + "/" + self.attributes['id'],
                            headers={'if-none-match': '*'},
                            status=412)
        self.app.delete("/v1/resource/" + self.resource_type
                        + "/" + self.attributes['id'],
                        headers={'if-none-match': '*'},
                        status=412)

        # if-modified-since is ignored with PATCH/PUT/DELETE
        self.app.patch_json("/v1/resource/" + self.resource_type
                            + "/" + self.attributes['id'],
                            params=self.patchable_attributes,
                            headers={'if-modified-since': lastmodified},
                            status=200)
        self.app.delete("/v1/resource/" + self.resource_type
                        + "/" + self.attributes['id'],
                        headers={'if-modified-since': lastmodified},
                        status=204)

    def test_get_resource_non_admin(self):
        with self.app.use_another_user():
            self.app.post_json("/v1/resource/" + self.resource_type,
                               params=self.attributes,
                               status=201)
            self.app.get("/v1/resource/"
                         + self.resource_type
                         + "/"
                         + self.attributes['id'],
                         status=200)

    def test_get_resource_unauthorized(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes,
                           status=201)
        with self.app.use_another_user():
            self.app.get("/v1/resource/"
                         + self.resource_type
                         + "/"
                         + self.attributes['id'],
                         status=403)

    def test_get_resource_named_metric(self):
        self.attributes['metrics'] = {'foo': {'archive_policy_name': "high"}}
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)
        self.app.get("/v1/resource/"
                     + self.resource_type
                     + "/"
                     + self.attributes['id']
                     + "/metric/foo/measures",
                     status=200)

    def test_delete_resource_named_metric(self):
        self.attributes['metrics'] = {'foo': {'archive_policy_name': "high"}}
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)
        self.app.delete("/v1/resource/"
                        + self.resource_type
                        + "/"
                        + self.attributes['id']
                        + "/metric/foo",
                        status=204)
        self.app.delete("/v1/resource/"
                        + self.resource_type
                        + "/"
                        + self.attributes['id']
                        + "/metric/foo/measures",
                        status=404)

    def test_get_resource_unknown_named_metric(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)
        self.app.get("/v1/resource/"
                     + self.resource_type
                     + "/"
                     + self.attributes['id']
                     + "/metric/foo",
                     status=404)

    def test_post_append_metrics_already_exists(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)

        metrics = {'foo': {'archive_policy_name': "high"}}
        self.app.post_json("/v1/resource/" + self.resource_type
                           + "/" + self.attributes['id'] + "/metric",
                           params=metrics, status=204)
        metrics = {'foo': {'archive_policy_name': "low"}}
        self.app.post_json("/v1/resource/" + self.resource_type
                           + "/" + self.attributes['id']
                           + "/metric",
                           params=metrics,
                           status=409)

        result = self.app.get("/v1/resource/"
                              + self.resource_type + "/"
                              + self.attributes['id'])
        result = json.loads(result.text)
        self.assertTrue(uuid.UUID(result['metrics']['foo']))

    def test_post_append_metrics(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)

        metrics = {'foo': {'archive_policy_name': "high"}}
        self.app.post_json("/v1/resource/" + self.resource_type
                           + "/" + self.attributes['id'] + "/metric",
                           params=metrics, status=204)
        result = self.app.get("/v1/resource/"
                              + self.resource_type + "/"
                              + self.attributes['id'])
        result = json.loads(result.text)
        self.assertTrue(uuid.UUID(result['metrics']['foo']))

    def test_post_append_metrics_created_by_different_user(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)
        with self.app.use_another_user():
            metric = self.app.post_json(
                "/v1/metric",
                params={'archive_policy_name': "high"})
        metric_id = json.loads(metric.text)['id']
        result = self.app.post_json("/v1/resource/" + self.resource_type
                                    + "/" + self.attributes['id'] + "/metric",
                                    params={str(uuid.uuid4()): metric_id},
                                    status=400)
        self.assertIn("Metric %s does not exist" % metric_id, result.text)

    def test_patch_resource_metrics(self):
        # TODO(jd) Use a fixture as soon as there's one
        timeutils.set_time_override(datetime.datetime(2014, 1, 1, 10, 23))
        self.addCleanup(timeutils.clear_time_override)
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params=self.attributes,
                                    status=201)
        r = json.loads(result.text)

        timeutils.set_time_override(datetime.datetime(2014, 1, 2, 6, 48))
        new_metrics = {'foo': {'archive_policy_name': "medium"}}
        self.app.patch_json(
            "/v1/resource/" + self.resource_type + "/"
            + self.attributes['id'],
            params={'metrics': new_metrics},
            status=200)
        result = self.app.get("/v1/resource/"
                              + self.resource_type + "/"
                              + self.attributes['id'])
        result = json.loads(result.text)
        self.assertTrue(uuid.UUID(result['metrics']['foo']))
        self.assertIsNone(result['revision_end'])
        self.assertIsNone(r['revision_end'])
        self.assertEqual(result['revision_start'],
                         "2014-01-02T06:48:00.000000Z")
        self.assertEqual(r['revision_start'], "2014-01-01T10:23:00.000000Z")

        del result['metrics']
        del result['revision_start']
        del result['revision_end']
        del r['metrics']
        del r['revision_start']
        del r['revision_end']
        self.assertEqual(r, result)

    def test_patch_resource_existent_metrics_from_another_user(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)
        with self.app.use_another_user():
            result = self.app.post_json(
                "/v1/metric",
                params={'archive_policy_name': "medium"})
        metric_id = json.loads(result.text)['id']
        result = self.app.patch_json(
            "/v1/resource/"
            + self.resource_type
            + "/"
            + self.attributes['id'],
            params={'metrics': {'foo': metric_id}},
            status=400)
        self.assertIn("Metric %s does not exist" % metric_id, result.text)
        result = self.app.get("/v1/resource/"
                              + self.resource_type
                              + "/"
                              + self.attributes['id'])
        result = json.loads(result.text)
        self.assertEqual({}, result['metrics'])

    def test_patch_resource_non_existent_metrics(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes,
                           status=201)
        e1 = str(uuid.uuid4())
        result = self.app.patch_json(
            "/v1/resource/"
            + self.resource_type
            + "/"
            + self.attributes['id'],
            params={'metrics': {'foo': e1}},
            status=400)
        self.assertIn("Metric %s does not exist" % e1, result.text)
        result = self.app.get("/v1/resource/"
                              + self.resource_type
                              + "/"
                              + self.attributes['id'])
        result = json.loads(result.text)
        self.assertEqual({}, result['metrics'])

    def test_patch_resource_attributes(self):
        # TODO(jd) Use a fixture as soon as there's one
        timeutils.set_time_override(datetime.datetime(2014, 1, 1, 10, 23))
        self.addCleanup(timeutils.clear_time_override)

        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes,
                           status=201)
        timeutils.set_time_override(datetime.datetime(2014, 1, 2, 6, 48))
        presponse = self.app.patch_json(
            "/v1/resource/" + self.resource_type
            + "/" + self.attributes['id'],
            params=self.patchable_attributes,
            status=200)
        response = self.app.get("/v1/resource/" + self.resource_type
                                + "/" + self.attributes['id'])
        result = json.loads(response.text)
        presult = json.loads(presponse.text)
        self.assertEqual(result, presult)
        for k, v in six.iteritems(self.patchable_attributes):
            self.assertEqual(v, result[k])
        self.assertIsNone(result['revision_end'])
        self.assertEqual(result['revision_start'],
                         "2014-01-02T06:48:00.000000Z")
        self._check_etag(response, result)

        # Check the history
        history = self.app.post_json(
            "/v1/search/resource/" + self.resource_type,
            headers={"Accept": "application/json; history=true"},
            params={"=": {"id": result['id']}},
            status=200)
        history = json.loads(history.text)
        self.assertGreaterEqual(len(history), 2)
        self.assertEqual(result, history[1])

        h = history[0]
        for k, v in six.iteritems(self.attributes):
            self.assertEqual(v, h[k])
        self.assertEqual(h['revision_end'],
                         "2014-01-02T06:48:00.000000Z")
        self.assertEqual(h['revision_start'],
                         "2014-01-01T10:23:00.000000Z")

    def test_patch_resource_attributes_unauthorized(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes,
                           status=201)
        with self.app.use_another_user():
            self.app.patch_json(
                "/v1/resource/" + self.resource_type
                + "/" + self.attributes['id'],
                params=self.patchable_attributes,
                status=403)

    def test_patch_resource_ended_at_before_started_at(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes,
                           status=201)
        self.app.patch_json(
            "/v1/resource/"
            + self.resource_type
            + "/"
            + self.attributes['id'],
            params={'ended_at': "2000-05-05 23:23:23"},
            status=400)

    def test_patch_resource_no_partial_update(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes,
                           status=201)
        e1 = str(uuid.uuid4())
        result = self.app.patch_json(
            "/v1/resource/" + self.resource_type + "/"
            + self.attributes['id'],
            params={'ended_at': "2044-05-05 23:23:23",
                    'metrics': {"foo": e1}},
            status=400)
        self.assertIn("Metric %s does not exist" % e1, result.text)
        result = self.app.get("/v1/resource/"
                              + self.resource_type + "/"
                              + self.attributes['id'])
        result = json.loads(result.text)
        del result['revision_start']
        del result['revision_end']
        self.assertEqual(self.resource, result)

    def test_patch_resource_non_existent(self):
        self.app.patch_json(
            "/v1/resource/" + self.resource_type
            + "/" + str(uuid.uuid4()),
            params={},
            status=404)

    def test_patch_resource_non_existent_with_body(self):
        self.app.patch_json(
            "/v1/resource/" + self.resource_type
            + "/" + str(uuid.uuid4()),
            params=self.patchable_attributes,
            status=404)

    def test_patch_resource_unknown_field(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)
        result = self.app.patch_json(
            "/v1/resource/" + self.resource_type + "/"
            + self.attributes['id'],
            params={'foobar': 123},
            status=400)
        self.assertIn(b'Invalid input: extra keys not allowed @ data['
                      + repr(u'foobar').encode('ascii') + b"]",
                      result.body)

    def test_delete_resource(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)
        self.app.get("/v1/resource/" + self.resource_type + "/"
                     + self.attributes['id'],
                     status=200)
        self.app.delete("/v1/resource/" + self.resource_type + "/"
                        + self.attributes['id'],
                        status=204)
        self.app.get("/v1/resource/" + self.resource_type + "/"
                     + self.attributes['id'],
                     status=404)

    def test_delete_resource_with_metrics(self):
        metric = self.app.post_json(
            "/v1/metric",
            params={'archive_policy_name': "high"})
        metric_id = json.loads(metric.text)['id']
        metric_name = six.text_type(uuid.uuid4())
        self.attributes['metrics'] = {metric_name: metric_id}
        self.app.get("/v1/metric/" + metric_id,
                     status=200)
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)
        self.app.get("/v1/resource/" + self.resource_type + "/"
                     + self.attributes['id'],
                     status=200)
        self.app.delete("/v1/resource/" + self.resource_type + "/"
                        + self.attributes['id'],
                        status=204)
        self.app.get("/v1/resource/" + self.resource_type + "/"
                     + self.attributes['id'],
                     status=404)
        self.app.get("/v1/metric/" + metric_id,
                     status=404)
        # Test that storage deleted it
        self.assertRaises(storage.MetricDoesNotExist,
                          self.storage.get_measures,
                          storage.Metric(metric_id, None))

    def test_delete_resource_unauthorized(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)
        with self.app.use_another_user():
            self.app.delete("/v1/resource/" + self.resource_type + "/"
                            + self.attributes['id'],
                            status=403)

    def test_delete_resource_non_existent(self):
        result = self.app.delete("/v1/resource/" + self.resource_type + "/"
                                 + self.attributes['id'],
                                 status=404)
        self.assertIn(
            "Resource %s does not exist" % self.attributes['id'],
            result.text)

    def test_post_resource_invalid_uuid(self):
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params={"id": "foobar"},
                                    status=400)
        self.assertEqual("text/plain", result.content_type)
        self.assertIn(b"Invalid input: not a valid value "
                      b"for dictionary value @ data["
                      + repr(u'id').encode('ascii') + b"]",
                      result.body)

    def test_post_resource_with_metrics(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"})
        metric = json.loads(result.text)
        self.attributes['metrics'] = {"foo": metric['id']}
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params=self.attributes,
                                    status=201)
        resource = json.loads(result.text)
        self.assertEqual("http://localhost/v1/resource/"
                         + self.resource_type + "/"
                         + self.attributes['id'],
                         result.headers['Location'])
        self.resource['metrics'] = self.attributes['metrics']
        del resource['revision_start']
        del resource['revision_end']
        self.assertEqual(self.resource, resource)

    def test_post_resource_with_null_metrics(self):
        self.attributes['metrics'] = {"foo": {"archive_policy_name": "low"}}
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params=self.attributes,
                                    status=201)
        resource = json.loads(result.text)
        self.assertEqual("http://localhost/v1/resource/"
                         + self.resource_type + "/"
                         + self.attributes['id'],
                         result.headers['Location'])
        self.assertEqual(self.attributes['id'], resource["id"])
        metric_id = uuid.UUID(resource['metrics']['foo'])
        result = self.app.get("/v1/metric/" + str(metric_id) + "/measures",
                              status=200)

    def test_search_datetime(self):
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes,
                           status=201)
        result = self.app.get("/v1/resource/" + self.resource_type
                              + "/" + self.attributes['id'])
        result = json.loads(result.text)

        resources = self.app.post_json(
            "/v1/search/resource/" + self.resource_type,
            params={"and": [{"=": {"id": result['id']}},
                            {"=": {"ended_at": None}}]},
            status=200)
        resources = json.loads(resources.text)
        self.assertGreaterEqual(len(resources), 1)
        self.assertEqual(result, resources[0])

        resources = self.app.post_json(
            "/v1/search/resource/" + self.resource_type,
            headers={"Accept": "application/json; history=true"},
            params={"and": [
                {"=": {"id": result['id']}},
                {"or": [{">=": {"revision_end": '2014-01-03T02:02:02'}},
                        {"=": {"revision_end": None}}]}
            ]},
            status=200)
        resources = json.loads(resources.text)
        self.assertGreaterEqual(len(resources), 1)
        self.assertEqual(result, resources[0])

    def test_search_resources_by_user(self):
        u1 = str(uuid.uuid4())
        self.attributes['user_id'] = u1
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes)
        created_resource = json.loads(result.text)
        result = self.app.post_json("/v1/search/resource/generic",
                                    params={"eq": {"user_id": u1}},
                                    status=200)
        resources = json.loads(result.text)
        self.assertGreaterEqual(len(resources), 1)
        result = self.app.post_json(
            "/v1/search/resource/" + self.resource_type,
            params={"=": {"user_id": u1}},
            status=200)
        resources = json.loads(result.text)
        self.assertGreaterEqual(len(resources), 1)
        self.assertEqual(created_resource, resources[0])

    def test_search_resources_by_unknown_field(self):
        result = self.app.post_json(
            "/v1/search/resource/" + self.resource_type,
            params={"=": {"foobar": "baz"}},
            status=400)
        self.assertIn("Resource " + self.resource_type
                      + " has no foobar attribute",
                      result.text)

    def test_search_resources_started_after(self):
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
        result = self.app.post_json(
            "/v1/search/resource/generic",
            params={"≥": {"started_at": "2014-01-01"}},
            status=200)
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

        result = self.app.post_json(
            "/v1/search/resource/" + self.resource_type,
            params={">=": {"started_at": "2014-01-03"}})
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
                              status=400)
        self.assertIn(
            b"Unable to parse details value in query: "
            b"Unrecognized value 'awesome', acceptable values are",
            result.body)

    def test_list_resources_with_bad_details_in_accept(self):
        result = self.app.get("/v1/resource/generic",
                              headers={
                                  "Accept": "application/json; details=foo",
                              },
                              status=400)
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

    def test_search_resources_with_details(self):
        self._do_test_list_resources_with_detail(
            lambda: self.app.post("/v1/search/resource/generic?details=true"))

    def test_search_resources_with_details_via_accept(self):
        self._do_test_list_resources_with_detail(
            lambda: self.app.post(
                "/v1/search/resource/generic",
                headers={"Accept": "application/json; details=true"}))

    def test_get_res_named_metric_measure_aggregated_policies_invalid(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "low"})
        metric1 = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric1['id'],
                           params=[{"timestamp": '2013-01-01 12:00:01',
                                    "value": 16}])

        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name":
                                            "no_granularity_match"})
        metric2 = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric2['id'],
                           params=[{"timestamp": '2013-01-01 12:00:01',
                                    "value": 4}])

        # NOTE(sileht): because the database is never cleaned between each test
        # we must ensure that the query will not match resources from an other
        # test, to achieve this we set a different server_group on each test.
        server_group = str(uuid.uuid4())
        if self.resource_type == 'instance':
            self.attributes['server_group'] = server_group

        self.attributes['metrics'] = {'foo': metric1['id']}
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)

        self.attributes['id'] = str(uuid.uuid4())
        self.attributes['metrics'] = {'foo': metric2['id']}
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)

        result = self.app.post_json(
            "/v1/aggregation/resource/"
            + self.resource_type + "/metric/foo?aggregation=max",
            params={"and":
                    [{"=": {"server_group": server_group}},
                     {"=": {"display_name": "myinstance"}}]},
            status=400)
        if self.resource_type == 'instance':
            self.assertIn(b"One of the metric to aggregated doesn't have "
                          b"matching granularity",
                          result.body)

    def test_get_res_named_metric_measure_aggregation_nominal(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"})
        metric1 = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric1['id'],
                           params=[{"timestamp": '2013-01-01 12:00:01',
                                    "value": 8},
                                   {"timestamp": '2013-01-01 12:00:02',
                                    "value": 16}])

        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"})
        metric2 = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric2['id'],
                           params=[{"timestamp": '2013-01-01 12:00:01',
                                    "value": 0},
                                   {"timestamp": '2013-01-01 12:00:02',
                                    "value": 4}])

        # NOTE(sileht): because the database is never cleaned between each test
        # we must ensure that the query will not match resources from an other
        # test, to achieve this we set a different server_group on each test.
        server_group = str(uuid.uuid4())
        if self.resource_type == 'instance':
            self.attributes['server_group'] = server_group

        self.attributes['metrics'] = {'foo': metric1['id']}
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)

        self.attributes['id'] = str(uuid.uuid4())
        self.attributes['metrics'] = {'foo': metric2['id']}
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)

        result = self.app.post_json(
            "/v1/aggregation/resource/" + self.resource_type
            + "/metric/foo?aggregation=max",
            params={"and":
                    [{"=": {"server_group": server_group}},
                     {"=": {"display_name": "myinstance"}}]},
            expect_errors=True)

        if self.resource_type == 'instance':
            self.assertEqual(200, result.status_code, result.text)
            measures = json.loads(result.text)
            self.assertEqual([[u'2013-01-01T00:00:00.000000Z', 86400.0, 16.0],
                              [u'2013-01-01T12:00:00.000000Z', 3600.0, 16.0],
                              [u'2013-01-01T12:00:00.000000Z', 60.0, 16.0]],
                             measures)
        else:
            self.assertEqual(400, result.status_code)

        result = self.app.post_json(
            "/v1/aggregation/resource/"
            + self.resource_type + "/metric/foo?aggregation=min",
            params={"and":
                    [{"=": {"server_group": server_group}},
                     {"=": {"display_name": "myinstance"}}]},
            expect_errors=True)

        if self.resource_type == 'instance':
            self.assertEqual(200, result.status_code)
            measures = json.loads(result.text)
            self.assertEqual([['2013-01-01T00:00:00.000000Z', 86400.0, 0],
                              ['2013-01-01T12:00:00.000000Z', 3600.0, 0],
                              ['2013-01-01T12:00:00.000000Z', 60.0, 0]],
                             measures)
        else:
            self.assertEqual(400, result.status_code)

    def test_get_aggregated_measures_across_entities_no_match(self):
        result = self.app.post_json(
            "/v1/aggregation/resource/"
            + self.resource_type + "/metric/foo?aggregation=min",
            params={"and":
                    [{"=": {"server_group": "notexistentyet"}},
                     {"=": {"display_name": "myinstance"}}]},
            expect_errors=True)

        if self.resource_type == 'instance':
            self.assertEqual(200, result.status_code)
            measures = json.loads(result.text)
            self.assertEqual([], measures)
        else:
            self.assertEqual(400, result.status_code)

    def test_get_aggregated_measures_across_entities(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"})
        metric1 = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric1['id'],
                           params=[{"timestamp": '2013-01-01 12:00:01',
                                    "value": 8},
                                   {"timestamp": '2013-01-01 12:00:02',
                                    "value": 16}])

        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"})
        metric2 = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric2['id'],
                           params=[{"timestamp": '2013-01-01 12:00:01',
                                    "value": 0},
                                   {"timestamp": '2013-01-01 12:00:02',
                                    "value": 4}])
        # Check with one metric
        result = self.app.get("/v1/aggregation/metric"
                              "?aggregation=mean&metric=%s" % (metric2['id']))
        measures = json.loads(result.text)
        self.assertEqual([[u'2013-01-01T00:00:00.000000Z', 86400.0, 2.0],
                          [u'2013-01-01T12:00:00.000000Z', 3600.0, 2.0],
                          [u'2013-01-01T12:00:00.000000Z', 60.0, 2.0]],
                         measures)

        # Check with two metrics
        result = self.app.get("/v1/aggregation/metric"
                              "?aggregation=mean&metric=%s&metric=%s" %
                              (metric1['id'], metric2['id']))
        measures = json.loads(result.text)
        self.assertEqual([[u'2013-01-01T00:00:00.000000Z', 86400.0, 7.0],
                          [u'2013-01-01T12:00:00.000000Z', 3600.0, 7.0],
                          [u'2013-01-01T12:00:00.000000Z', 60.0, 7.0]],
                         measures)


class GenericResourceTest(RestTest):
    def test_list_resources_tied_to_user(self):
        resource_id = str(uuid.uuid4())
        self.app.post_json(
            "/v1/resource/generic",
            params={
                "id": resource_id,
                "started_at": "2014-01-01 02:02:02",
                "user_id": str(uuid.uuid4()),
                "project_id": str(uuid.uuid4()),
            })

        with self.app.use_another_user():
            result = self.app.get("/v1/resource/generic")
            resources = json.loads(result.text)
            for resource in resources:
                if resource['id'] == resource_id:
                    self.fail("Resource found")

    def test_get_resources_metric_tied_to_user(self):
        resource_id = str(uuid.uuid4())
        self.app.post_json(
            "/v1/resource/generic",
            params={
                "id": resource_id,
                "started_at": "2014-01-01 02:02:02",
                "user_id": FakeMemcache.USER_ID_2,
                "project_id": FakeMemcache.PROJECT_ID_2,
                "metrics": {"foobar": {"archive_policy_name": "low"}},
            })

        # This user created it, she can access it
        self.app.get(
            "/v1/resource/generic/%s/metric/foobar" % resource_id)

        with self.app.use_another_user():
            # This user "owns it", it should be able to access it
            self.app.get(
                "/v1/resource/generic/%s/metric/foobar" % resource_id)

    def test_search_resources_invalid_query(self):
        result = self.app.post_json(
            "/v1/search/resource/generic",
            params={"wrongoperator": {"user_id": "bar"}},
            status=400)
        self.assertIn(
            "Invalid input: extra keys not allowed @ data["
            + repr(u'wrongoperator') + "]",
            result.text)

    def test_search_resources_with_like(self):
        attributes = {
            "id": str(uuid.uuid4()),
            "started_at": "2014-01-03T02:02:02.000000",
            "host": "computenode42",
            "image_ref": "imageref!",
            "flavor_id": 123,
            "display_name": "myinstance",
        }
        result = self.app.post_json(
            "/v1/resource/instance",
            params=attributes)
        created_resource = json.loads(result.text)

        result = self.app.post_json(
            "/v1/search/resource/instance",
            params={"like": {"host": "computenode%"}},
            status=200)

        resources = json.loads(result.text)
        self.assertIn(created_resource, resources)

        result = self.app.post_json(
            "/v1/search/resource/instance",
            params={"like": {"host": str(uuid.uuid4())}},
            status=200)
        resources = json.loads(result.text)
        self.assertEqual([], resources)


ResourceTest.generate_scenarios()
