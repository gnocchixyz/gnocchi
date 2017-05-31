# -*- encoding: utf-8 -*-
#
# Copyright © 2016 Red Hat, Inc.
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
import base64
import calendar
import contextlib
import datetime
from email import utils as email_utils
import hashlib
import json
import uuid

import iso8601
from keystonemiddleware import fixture as ksm_fixture
import mock
import six
from stevedore import extension
import testscenarios
from testtools import testcase
import webtest

from gnocchi import archive_policy
from gnocchi import rest
from gnocchi.rest import app
from gnocchi.tests import base as tests_base
from gnocchi.tests import utils as tests_utils
from gnocchi import utils


load_tests = testscenarios.load_tests_apply_scenarios


class TestingApp(webtest.TestApp):
    VALID_TOKEN_ADMIN = str(uuid.uuid4())
    USER_ID_ADMIN = str(uuid.uuid4())
    PROJECT_ID_ADMIN = str(uuid.uuid4())

    VALID_TOKEN = str(uuid.uuid4())
    USER_ID = str(uuid.uuid4())
    PROJECT_ID = str(uuid.uuid4())

    VALID_TOKEN_2 = str(uuid.uuid4())
    USER_ID_2 = str(uuid.uuid4())
    PROJECT_ID_2 = str(uuid.uuid4())

    INVALID_TOKEN = str(uuid.uuid4())

    def __init__(self, *args, **kwargs):
        self.auth_mode = kwargs.pop('auth_mode')
        self.storage = kwargs.pop('storage')
        self.indexer = kwargs.pop('indexer')
        super(TestingApp, self).__init__(*args, **kwargs)
        # Setup Keystone auth_token fake cache
        self.token = self.VALID_TOKEN
        # Setup default user for basic auth
        self.user = self.USER_ID.encode('ascii')

    @contextlib.contextmanager
    def use_admin_user(self):
        if self.auth_mode == "keystone":
            old_token = self.token
            self.token = self.VALID_TOKEN_ADMIN
            try:
                yield
            finally:
                self.token = old_token
        elif self.auth_mode == "basic":
            old_user = self.user
            self.user = b"admin"
            try:
                yield
            finally:
                self.user = old_user
        elif self.auth_mode == "remoteuser":
            old_user = self.user
            self.user = b"admin"
            try:
                yield
            finally:
                self.user = old_user
        elif self.auth_mode == "noauth":
            raise testcase.TestSkipped("auth mode is noauth")
        else:
            raise RuntimeError("Unknown auth_mode")

    @contextlib.contextmanager
    def use_another_user(self):
        if self.auth_mode != "keystone":
            raise testcase.TestSkipped("Auth mode is not Keystone")
        old_token = self.token
        self.token = self.VALID_TOKEN_2
        try:
            yield
        finally:
            self.token = old_token

    @contextlib.contextmanager
    def use_invalid_token(self):
        if self.auth_mode != "keystone":
            raise testcase.TestSkipped("Auth mode is not Keystone")
        old_token = self.token
        self.token = self.INVALID_TOKEN
        try:
            yield
        finally:
            self.token = old_token

    def do_request(self, req, *args, **kwargs):
        if self.auth_mode in "keystone":
            if self.token is not None:
                req.headers['X-Auth-Token'] = self.token
        elif self.auth_mode == "basic":
            req.headers['Authorization'] = (
                b"basic " + base64.b64encode(self.user + b":")
            )
        elif self.auth_mode == "remoteuser":
            req.remote_user = self.user
        elif self.auth_mode == "noauth":
            req.headers['X-User-Id'] = self.USER_ID
            req.headers['X-Project-Id'] = self.PROJECT_ID
        response = super(TestingApp, self).do_request(req, *args, **kwargs)
        metrics = tests_utils.list_all_incoming_metrics(self.storage.incoming)
        self.storage.process_background_tasks(self.indexer, metrics, sync=True)
        return response


class RestTest(tests_base.TestCase, testscenarios.TestWithScenarios):

    scenarios = [
        ('basic', dict(auth_mode="basic")),
        ('keystone', dict(auth_mode="keystone")),
        ('noauth', dict(auth_mode="noauth")),
        ('remoteuser', dict(auth_mode="remoteuser")),
    ]

    def setUp(self):
        super(RestTest, self).setUp()

        if self.auth_mode == "keystone":
            self.auth_token_fixture = self.useFixture(
                ksm_fixture.AuthTokenFixture())
            self.auth_token_fixture.add_token_data(
                is_v2=True,
                token_id=TestingApp.VALID_TOKEN_ADMIN,
                user_id=TestingApp.USER_ID_ADMIN,
                user_name='adminusername',
                project_id=TestingApp.PROJECT_ID_ADMIN,
                role_list=['admin'])
            self.auth_token_fixture.add_token_data(
                is_v2=True,
                token_id=TestingApp.VALID_TOKEN,
                user_id=TestingApp.USER_ID,
                user_name='myusername',
                project_id=TestingApp.PROJECT_ID,
                role_list=["member"])
            self.auth_token_fixture.add_token_data(
                is_v2=True,
                token_id=TestingApp.VALID_TOKEN_2,
                user_id=TestingApp.USER_ID_2,
                user_name='myusername2',
                project_id=TestingApp.PROJECT_ID_2,
                role_list=["member"])

        self.conf.set_override("auth_mode", self.auth_mode, group="api")

        self.app = TestingApp(app.load_app(conf=self.conf,
                                           indexer=self.index,
                                           storage=self.storage,
                                           not_implemented_middleware=False),
                              storage=self.storage,
                              indexer=self.index,
                              auth_mode=self.auth_mode)

    # NOTE(jd) Used at least by docs
    @staticmethod
    def runTest():
        pass


class RootTest(RestTest):
    def test_deserialize_force_json(self):
        with self.app.use_admin_user():
            self.app.post(
                "/v1/archive_policy",
                params="foo",
                status=415)

    def test_capabilities(self):
        custom_agg = extension.Extension('test_aggregation', None, None, None)
        mgr = extension.ExtensionManager.make_test_instance(
            [custom_agg], 'gnocchi.aggregates')
        aggregation_methods = set(
            archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS)

        with mock.patch.object(extension, 'ExtensionManager',
                               return_value=mgr):
            result = self.app.get("/v1/capabilities").json
            self.assertEqual(
                sorted(aggregation_methods),
                sorted(result['aggregation_methods']))
            self.assertEqual(
                ['test_aggregation'],
                result['dynamic_aggregation_methods'])

    def test_status(self):
        with self.app.use_admin_user():
            r = self.app.get("/v1/status")
        status = json.loads(r.text)
        self.assertIsInstance(status['storage']['measures_to_process'], dict)
        self.assertIsInstance(status['storage']['summary']['metrics'], int)
        self.assertIsInstance(status['storage']['summary']['measures'], int)


class ArchivePolicyTest(RestTest):
    """Test the ArchivePolicies REST API.

    See also gnocchi/tests/gabbi/gabbits/archive.yaml
    """

    # TODO(chdent): The tests left here involve inspecting the
    # aggregation methods which gabbi can't currently handle because
    # the ordering of the results is not predictable.

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

    def test_get_metric_with_another_user_linked_resource(self):
        result = self.app.post_json(
            "/v1/resource/generic",
            params={
                "id": str(uuid.uuid4()),
                "started_at": "2014-01-01 02:02:02",
                "user_id": TestingApp.USER_ID_2,
                "project_id": TestingApp.PROJECT_ID_2,
                "metrics": {"foobar": {"archive_policy_name": "low"}},
            })
        resource = json.loads(result.text)
        metric_id = resource["metrics"]["foobar"]
        with self.app.use_another_user():
            self.app.get("/v1/metric/%s" % metric_id)

    def test_get_metric_with_another_user(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"},
                                    status=201)
        self.assertEqual("application/json", result.content_type)

        with self.app.use_another_user():
            self.app.get(result.headers['Location'], status=403)

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

    def test_delete_metric_another_user(self):
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "medium"})
        metric = json.loads(result.text)
        with self.app.use_another_user():
            self.app.delete("/v1/metric/" + metric['id'], status=403)

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
            status=202)
        self.app.post_json(
            "/v1/metric/%s/measures" % metric['id'],
            params=[{"timestamp": '2013-01-01 23:29:23',
                     "value": 1234.2}],
            status=202)
        self.app.post_json(
            "/v1/metric/%s/measures" % metric['id'],
            params=[{"timestamp": '2013-01-01 23:28:23',
                     "value": 1234.2}],
            status=202)
        # This one is too old and should not be taken into account
        self.app.post_json(
            "/v1/metric/%s/measures" % metric['id'],
            params=[{"timestamp": '2012-01-01 23:27:23',
                     "value": 1234.2}],
            status=202)

        ret = self.app.get("/v1/metric/%s/measures" % metric['id'])
        result = json.loads(ret.text)
        self.assertEqual(
            [[u'2013-01-01T23:28:00+00:00', 60.0, 1234.2],
             [u'2013-01-01T23:29:00+00:00', 60.0, 1234.2],
             [u'2013-01-01T23:30:00+00:00', 60.0, 1234.2]],
            result)

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

    @mock.patch.object(utils, 'utcnow')
    def test_get_measure_start_relative(self, utcnow):
        """Make sure the timestamps can be relative to now."""
        utcnow.return_value = datetime.datetime(2014, 1, 1, 10, 23)
        result = self.app.post_json("/v1/metric",
                                    params={"archive_policy_name": "high"})
        metric = json.loads(result.text)
        self.app.post_json("/v1/metric/%s/measures" % metric['id'],
                           params=[{"timestamp": utils.utcnow().isoformat(),
                                    "value": 1234.2}])
        ret = self.app.get(
            "/v1/metric/%s/measures?start=-10 minutes"
            % metric['id'],
            status=200)
        result = json.loads(ret.text)
        now = utils.datetime_utc(2014, 1, 1, 10, 23)
        self.assertEqual([
            ['2014-01-01T10:00:00+00:00', 3600.0, 1234.2],
            [(now
              - datetime.timedelta(
                  seconds=now.second,
                  microseconds=now.microsecond)).isoformat(),
             60.0, 1234.2],
            [(now
              - datetime.timedelta(
                  microseconds=now.microsecond)).isoformat(),
             1.0, 1234.2]], result)

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
            [[u'2013-01-01T12:00:00+00:00', 3600.0, 845.1],
             [u'2013-01-01T12:00:00+00:00', 60.0, 845.1],
             [u'2013-01-01T12:00:00+00:00', 1.0, 1234.2]],
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
        self.assertEqual([[u'2013-01-01T00:00:00+00:00', 86400.0, 12345.2],
                          [u'2013-01-01T12:00:00+00:00', 3600.0, 12345.2],
                          [u'2013-01-01T12:00:00+00:00', 60.0, 12345.2]],
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
        expected = [[u'2013-01-01T12:00:00+00:00', 120.0, 32.25]]
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
        mgr = self.index.get_resource_type_schema()
        resource_type = str(uuid.uuid4())
        self.index.create_resource_type(
            mgr.resource_type_from_dict(resource_type, {
                "server_group": {"type": "string",
                                 "min_length": 1,
                                 "max_length": 40,
                                 "required": True}
            }, 'creating'))

        attributes = {
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
        self.app.post_json("/v1/resource/" + resource_type,
                           params=attributes)

        attributes['id'] = str(uuid.uuid4())
        attributes['metrics'] = {'bar': metric2['id']}
        self.app.post_json("/v1/resource/" + resource_type,
                           params=attributes)

        result = self.app.post_json(
            "/v1/aggregation/resource/%s/metric/foo?aggregation=max"
            % resource_type,
            params={"=": {"server_group": attributes['server_group']}})

        measures = json.loads(result.text)
        self.assertEqual([[u'2013-01-01T00:00:00+00:00', 86400.0, 16.0],
                          [u'2013-01-01T12:00:00+00:00', 3600.0, 16.0],
                          [u'2013-01-01T12:00:00+00:00', 60.0, 16.0]],
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
            {metric1: [[u'2013-01-01T12:00:00+00:00', 1.0, 1234.2]],
             metric2: []},
            result)


class ResourceTest(RestTest):
    def setUp(self):
        super(ResourceTest, self).setUp()
        self.attributes = {
            "id": str(uuid.uuid4()),
            "started_at": "2014-01-03T02:02:02+00:00",
            "user_id": str(uuid.uuid4()),
            "project_id": str(uuid.uuid4()),
            "name": "my-name",
        }
        self.patchable_attributes = {
            "ended_at": "2014-01-03T02:02:02+00:00",
            "name": "new-name",
        }
        self.resource = self.attributes.copy()
        # Set original_resource_id
        self.resource['original_resource_id'] = self.resource['id']
        self.resource['created_by_user_id'] = TestingApp.USER_ID
        if self.auth_mode in ("keystone", "noauth"):
            self.resource['created_by_project_id'] = TestingApp.PROJECT_ID
            self.resource['creator'] = (
                TestingApp.USER_ID + ":" + TestingApp.PROJECT_ID
            )
        elif self.auth_mode in ["basic", "remoteuser"]:
            self.resource['created_by_project_id'] = ""
            self.resource['creator'] = TestingApp.USER_ID
        self.resource['ended_at'] = None
        self.resource['metrics'] = {}
        if 'user_id' not in self.resource:
            self.resource['user_id'] = None
        if 'project_id' not in self.resource:
            self.resource['project_id'] = None

        mgr = self.index.get_resource_type_schema()
        self.resource_type = str(uuid.uuid4())
        self.index.create_resource_type(
            mgr.resource_type_from_dict(self.resource_type, {
                "name": {"type": "string",
                         "min_length": 1,
                         "max_length": 40,
                         "required": True}
            }, "creating"))
        self.resource['type'] = self.resource_type

    @mock.patch.object(utils, 'utcnow')
    def test_post_resource(self, utcnow):
        utcnow.return_value = utils.datetime_utc(2014, 1, 1, 10, 23)
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
                         "2014-01-01T10:23:00+00:00")
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
            iso8601.parse_date(dt).timetuple()), usegmt=True)

    def _check_etag(self, response, resource):
        lastmodified = self._strtime_to_httpdate(resource['revision_start'])
        etag = hashlib.sha1()
        etag.update(resource['id'].encode('utf-8'))
        etag.update(resource['revision_start'].encode('utf8'))
        self.assertEqual(response.headers['Last-Modified'], lastmodified)
        self.assertEqual(response.headers['ETag'], '"%s"' % etag.hexdigest())

    @mock.patch.object(utils, 'utcnow')
    def test_get_resource(self, utcnow):
        utcnow.return_value = utils.datetime_utc(2014, 1, 1, 10, 23)
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
                         "2014-01-01T10:23:00+00:00")
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

    def test_list_resource_metrics_unauthorized(self):
        self.attributes['metrics'] = {'foo': {'archive_policy_name': "high"}}
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes)
        with self.app.use_another_user():
            self.app.get(
                "/v1/resource/" + self.resource_type
                + "/" + self.attributes['id'] + "/metric",
                status=403)

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
                           params=metrics, status=200)
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
                           params=metrics, status=200)
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

    @mock.patch.object(utils, 'utcnow')
    def test_patch_resource_metrics(self, utcnow):
        utcnow.return_value = utils.datetime_utc(2014, 1, 1, 10, 23)
        result = self.app.post_json("/v1/resource/" + self.resource_type,
                                    params=self.attributes,
                                    status=201)
        r = json.loads(result.text)

        utcnow.return_value = utils.datetime_utc(2014, 1, 2, 6, 49)
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
        self.assertEqual(result['revision_start'], "2014-01-01T10:23:00+00:00")
        self.assertEqual(r['revision_start'], "2014-01-01T10:23:00+00:00")

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

    @mock.patch.object(utils, 'utcnow')
    def test_patch_resource_attributes(self, utcnow):
        utcnow.return_value = utils.datetime_utc(2014, 1, 1, 10, 23)
        self.app.post_json("/v1/resource/" + self.resource_type,
                           params=self.attributes,
                           status=201)
        utcnow.return_value = utils.datetime_utc(2014, 1, 2, 6, 48)
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
                         "2014-01-02T06:48:00+00:00")
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
                         "2014-01-02T06:48:00+00:00")
        self.assertEqual(h['revision_start'],
                         "2014-01-01T10:23:00+00:00")

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

    def test_search_resource_by_original_resource_id(self):
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes)
        created_resource = json.loads(result.text)
        original_id = created_resource['original_resource_id']
        result = self.app.post_json(
            "/v1/search/resource/" + self.resource_type,
            params={"eq": {"original_resource_id": original_id}},
            status=200)
        resources = json.loads(result.text)
        self.assertGreaterEqual(len(resources), 1)
        self.assertEqual(created_resource, resources[0])

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

    def test_search_resources_with_another_project_id(self):
        u1 = str(uuid.uuid4())
        result = self.app.post_json(
            "/v1/resource/generic",
            params={
                "id": str(uuid.uuid4()),
                "started_at": "2014-01-01 02:02:02",
                "user_id": u1,
                "project_id": TestingApp.PROJECT_ID_2,
            })
        g = json.loads(result.text)

        with self.app.use_another_user():
            result = self.app.post_json(
                "/v1/resource/generic",
                params={
                    "id": str(uuid.uuid4()),
                    "started_at": "2014-01-01 03:03:03",
                    "user_id": u1,
                    "project_id": str(uuid.uuid4()),
                })
            j = json.loads(result.text)
            g_found = False
            j_found = False

            result = self.app.post_json(
                "/v1/search/resource/generic",
                params={"=": {"user_id": u1}},
                status=200)
            resources = json.loads(result.text)
            self.assertGreaterEqual(len(resources), 2)
            for r in resources:
                if r['id'] == str(g['id']):
                    self.assertEqual(g, r)
                    g_found = True
                elif r['id'] == str(j['id']):
                    self.assertEqual(j, r)
                    j_found = True
                if g_found and j_found:
                    break
            else:
                self.fail("Some resources were not found")

    def test_search_resources_by_unknown_field(self):
        result = self.app.post_json(
            "/v1/search/resource/" + self.resource_type,
            params={"=": {"foobar": "baz"}},
            status=400)
        self.assertIn("Resource type " + self.resource_type
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
            b"Unable to parse `details': invalid truth value",
            result.body)

    def test_list_resources_with_bad_details_in_accept(self):
        result = self.app.get("/v1/resource/generic",
                              headers={
                                  "Accept": "application/json; details=foo",
                              },
                              status=400)
        self.assertIn(
            b"Unable to parse `Accept header': invalid truth value",
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

    def test_list_resources_with_another_project_id(self):
        result = self.app.post_json(
            "/v1/resource/generic",
            params={
                "id": str(uuid.uuid4()),
                "started_at": "2014-01-01 02:02:02",
                "user_id": TestingApp.USER_ID_2,
                "project_id": TestingApp.PROJECT_ID_2,
            })
        g = json.loads(result.text)

        with self.app.use_another_user():
            result = self.app.post_json(
                "/v1/resource/generic",
                params={
                    "id": str(uuid.uuid4()),
                    "started_at": "2014-01-01 03:03:03",
                    "user_id": str(uuid.uuid4()),
                    "project_id": str(uuid.uuid4()),
                })
            j = json.loads(result.text)

            g_found = False
            j_found = False

            result = self.app.get("/v1/resource/generic")
            self.assertEqual(200, result.status_code)
            resources = json.loads(result.text)
            self.assertGreaterEqual(len(resources), 2)
            for r in resources:
                if r['id'] == str(g['id']):
                    self.assertEqual(g, r)
                    g_found = True
                elif r['id'] == str(j['id']):
                    self.assertEqual(j, r)
                    j_found = True
                if g_found and j_found:
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
        # test, to achieve this we set a different name on each test.
        name = str(uuid.uuid4())
        self.attributes['name'] = name

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
            params={"=": {"name": name}},
            status=400)
        self.assertIn(b"One of the metrics being aggregated doesn't have "
                      b"matching granularity",
                      result.body)

    def test_get_res_named_metric_measure_aggregation_nooverlap(self):
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

        # NOTE(sileht): because the database is never cleaned between each test
        # we must ensure that the query will not match resources from an other
        # test, to achieve this we set a different name on each test.
        name = str(uuid.uuid4())
        self.attributes['name'] = name

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
            params={"=": {"name": name}},
            expect_errors=True)

        self.assertEqual(400, result.status_code, result.text)
        self.assertIn("No overlap", result.text)

        result = self.app.post_json(
            "/v1/aggregation/resource/"
            + self.resource_type + "/metric/foo?aggregation=min"
            + "&needed_overlap=0",
            params={"=": {"name": name}},
            expect_errors=True)

        self.assertEqual(200, result.status_code, result.text)
        measures = json.loads(result.text)
        self.assertEqual([['2013-01-01T00:00:00+00:00', 86400.0, 8.0],
                          ['2013-01-01T12:00:00+00:00', 3600.0, 8.0],
                          ['2013-01-01T12:00:00+00:00', 60.0, 8.0]],
                         measures)

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
        # test, to achieve this we set a different name on each test.
        name = str(uuid.uuid4())
        self.attributes['name'] = name

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
            params={"=": {"name": name}},
            expect_errors=True)

        self.assertEqual(200, result.status_code, result.text)
        measures = json.loads(result.text)
        self.assertEqual([[u'2013-01-01T00:00:00+00:00', 86400.0, 16.0],
                          [u'2013-01-01T12:00:00+00:00', 3600.0, 16.0],
                          [u'2013-01-01T12:00:00+00:00', 60.0, 16.0]],
                         measures)

        result = self.app.post_json(
            "/v1/aggregation/resource/"
            + self.resource_type + "/metric/foo?aggregation=min",
            params={"=": {"name": name}},
            expect_errors=True)

        self.assertEqual(200, result.status_code)
        measures = json.loads(result.text)
        self.assertEqual([['2013-01-01T00:00:00+00:00', 86400.0, 0],
                          ['2013-01-01T12:00:00+00:00', 3600.0, 0],
                          ['2013-01-01T12:00:00+00:00', 60.0, 0]],
                         measures)

    def test_get_aggregated_measures_across_entities_no_match(self):
        result = self.app.post_json(
            "/v1/aggregation/resource/"
            + self.resource_type + "/metric/foo?aggregation=min",
            params={"=": {"name": "none!"}},
            expect_errors=True)

        self.assertEqual(200, result.status_code)
        measures = json.loads(result.text)
        self.assertEqual([], measures)

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
        self.assertEqual([[u'2013-01-01T00:00:00+00:00', 86400.0, 2.0],
                          [u'2013-01-01T12:00:00+00:00', 3600.0, 2.0],
                          [u'2013-01-01T12:00:00+00:00', 60.0, 2.0]],
                         measures)

        # Check with two metrics
        result = self.app.get("/v1/aggregation/metric"
                              "?aggregation=mean&metric=%s&metric=%s" %
                              (metric1['id'], metric2['id']))
        measures = json.loads(result.text)
        self.assertEqual([[u'2013-01-01T00:00:00+00:00', 86400.0, 7.0],
                          [u'2013-01-01T12:00:00+00:00', 3600.0, 7.0],
                          [u'2013-01-01T12:00:00+00:00', 60.0, 7.0]],
                         measures)

    def test_search_resources_with_like(self):
        result = self.app.post_json(
            "/v1/resource/" + self.resource_type,
            params=self.attributes)
        created_resource = json.loads(result.text)

        result = self.app.post_json(
            "/v1/search/resource/" + self.resource_type,
            params={"like": {"name": "my%"}},
            status=200)

        resources = json.loads(result.text)
        self.assertIn(created_resource, resources)

        result = self.app.post_json(
            "/v1/search/resource/" + self.resource_type,
            params={"like": {"name": str(uuid.uuid4())}},
            status=200)
        resources = json.loads(result.text)
        self.assertEqual([], resources)


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
                "user_id": TestingApp.USER_ID_2,
                "project_id": TestingApp.PROJECT_ID_2,
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


class QueryStringSearchAttrFilterTest(tests_base.TestCase):
    def _do_test(self, expr, expected):
        req = rest.QueryStringSearchAttrFilter.parse(expr)
        self.assertEqual(expected, req)

    def test_search_query_builder(self):
        self._do_test('foo=7EED6CC3-EDC8-48C9-8EF6-8A36B9ACC91C',
                      {"=": {"foo": "7EED6CC3-EDC8-48C9-8EF6-8A36B9ACC91C"}})
        self._do_test('foo=7EED6CC3EDC848C98EF68A36B9ACC91C',
                      {"=": {"foo": "7EED6CC3EDC848C98EF68A36B9ACC91C"}})
        self._do_test('foo=bar', {"=": {"foo": "bar"}})
        self._do_test('foo!=1', {"!=": {"foo": 1.0}})
        self._do_test('foo=True', {"=": {"foo": True}})
        self._do_test('foo=null', {"=": {"foo": None}})
        self._do_test('foo="null"', {"=": {"foo": "null"}})
        self._do_test('foo in ["null", "foo"]',
                      {"in": {"foo": ["null", "foo"]}})
        self._do_test(u'foo="quote" and bar≠1',
                      {"and": [{u"≠": {"bar": 1}},
                               {"=": {"foo": "quote"}}]})
        self._do_test('foo="quote" or bar like "%%foo"',
                      {"or": [{"like": {"bar": "%%foo"}},
                              {"=": {"foo": "quote"}}]})

        self._do_test('not (foo="quote" or bar like "%%foo" or foo="what!" '
                      'or bar="who?")',
                      {"not": {"or": [
                          {"=": {"bar": "who?"}},
                          {"=": {"foo": "what!"}},
                          {"like": {"bar": "%%foo"}},
                          {"=": {"foo": "quote"}},
                      ]}})

        self._do_test('(foo="quote" or bar like "%%foo" or not foo="what!" '
                      'or bar="who?") and cat="meme"',
                      {"and": [
                          {"=": {"cat": "meme"}},
                          {"or": [
                              {"=": {"bar": "who?"}},
                              {"not": {"=": {"foo": "what!"}}},
                              {"like": {"bar": "%%foo"}},
                              {"=": {"foo": "quote"}},
                          ]}
                      ]})

        self._do_test('foo="quote" or bar like "%%foo" or foo="what!" '
                      'or bar="who?" and cat="meme"',
                      {"or": [
                          {"and": [
                              {"=": {"cat": "meme"}},
                              {"=": {"bar": "who?"}},
                          ]},
                          {"=": {"foo": "what!"}},
                          {"like": {"bar": "%%foo"}},
                          {"=": {"foo": "quote"}},
                      ]})

        self._do_test('foo="quote" or bar like "%%foo" and foo="what!" '
                      'or bar="who?" or cat="meme"',
                      {"or": [
                          {"=": {"cat": "meme"}},
                          {"=": {"bar": "who?"}},
                          {"and": [
                              {"=": {"foo": "what!"}},
                              {"like": {"bar": "%%foo"}},
                          ]},
                          {"=": {"foo": "quote"}},
                      ]})
