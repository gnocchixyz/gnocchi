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
import uuid

import pecan.testing
import testscenarios

from gnocchi.openstack.common import jsonutils
from gnocchi.rest import app
from gnocchi import tests


load_tests = testscenarios.load_tests_apply_scenarios


class RestTest(tests.TestCase):
    def setUp(self):
        super(RestTest, self).setUp()
        c = {}
        c.update(app.PECAN_CONFIG)
        c['conf'] = self.conf
        c['indexer'] = self.index
        c['storage'] = self.storage
        self.app = pecan.testing.load_test_app(c)

    def test_root(self):
        result = self.app.get("/")
        self.assertEqual("Nom nom nom.", result.body)
        self.assertEqual("text/plain", result.content_type)
        self.assertEqual(200, result.status_code)

    def test_post_entity(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archives": [(5, 60),
                                                         (60, 60)]})
        self.assertEqual("application/json", result.content_type)
        self.assertEqual(201, result.status_code)
        entity = jsonutils.loads(result.body)
        self.assertEqual("http://localhost/v1/entity/" + entity['id'],
                         result.headers['Location'])
        self.assertEqual(entity['archives'], [[5, 60], [60, 60]])

    def test_delete_entity(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archives": [(5, 60),
                                                         (60, 60)]})
        entity = jsonutils.loads(result.body)
        result = self.app.delete("/v1/entity/" + entity['id'])
        self.assertEqual(result.status_code, 204)

    def test_delete_entity_non_existent(self):
        e1 = str(uuid.uuid4())
        result = self.app.delete("/v1/entity/" + e1,
                                 expect_errors=True)
        self.assertEqual(result.status_code, 400)
        self.assertIn(
            u"Entity %s does not exist" % e1,
            result.body)

    def test_post_entity_bad_archives(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archives": [(5, 60, 30),
                                                         (60, 60)]},
                                    expect_errors=True)
        self.assertEqual("text/plain", result.content_type)
        self.assertEqual(result.status_code, 400)
        self.assertIn(
            u"Invalid input: invalid list value @ data[u'archives'][0]",
            result.body)

    def test_add_measure(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archives": [(5, 60),
                                                         (60, 60)]})
        entity = jsonutils.loads(result.body)
        result = self.app.post_json(
            "/v1/entity/%s/measures" % entity['id'],
            params=[{"timestamp": '2013-01-01 23:23:23',
                     "value": 1234.2}])
        self.assertEqual(result.status_code, 204)

    def test_add_measure_no_such_entity(self):
        e1 = str(uuid.uuid4())
        result = self.app.post_json(
            "/v1/entity/%s/measures" % e1,
            params=[{"timestamp": '2013-01-01 23:23:23',
                     "value": 1234.2}],
            expect_errors=True)
        self.assertEqual(result.status_code, 400)
        self.assertIn(
            u"Entity %s does not exist" % e1,
            result.body)

    def test_get_measure(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archives": [(1, 10)]})
        entity = jsonutils.loads(result.body)
        self.app.post_json("/v1/entity/%s/measures" % entity['id'],
                           params=[{"timestamp": '2013-01-01 23:23:23',
                                    "value": 1234.2}])
        ret = self.app.get("/v1/entity/%s/measures" % entity['id'])
        self.assertEqual(ret.status_code, 200)
        result = jsonutils.loads(ret.body)
        self.assertEqual({'2013-01-01T23:23:23.000000': 1234.2},
                         result)

    def test_get_measure_start(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archives": [(1, 10)]})
        entity = jsonutils.loads(result.body)
        self.app.post_json("/v1/entity/%s/measures" % entity['id'],
                           params=[{"timestamp": '2013-01-01 23:23:23',
                                    "value": 1234.2}])
        ret = self.app.get(
            "/v1/entity/%s/measures?start='2013-01-01 23:23:20"
            % entity['id'])
        self.assertEqual(ret.status_code, 200)
        result = jsonutils.loads(ret.body)
        self.assertEqual({'2013-01-01T23:23:23.000000': 1234.2},
                         result)

    def test_get_measure_stop(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archives": [(1, 10)]})
        entity = jsonutils.loads(result.body)
        self.app.post_json("/v1/entity/%s/measures" % entity['id'],
                           params=[{"timestamp": '2013-01-01 12:00:00',
                                    "value": 1234.2},
                                   {"timestamp": '2013-01-01 12:00:02',
                                    "value": 456}])
        ret = self.app.get("/v1/entity/%s/measures"
                           "?stop=2013-01-01 12:00:00" % entity['id'])
        self.assertEqual(ret.status_code, 200)
        result = jsonutils.loads(ret.body)
        self.assertEqual({'2013-01-01T12:00:00.000000': 1234.2},
                         result)

    def test_get_measure_aggregation(self):
        result = self.app.post_json("/v1/entity",
                                    params={"archives": [(5, 10)]})
        entity = jsonutils.loads(result.body)
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
        result = jsonutils.loads(ret.body)
        self.assertEqual({'2013-01-01T12:00:00.000000': 12345.2},
                         result)
