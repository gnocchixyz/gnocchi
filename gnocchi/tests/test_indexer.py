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
import operator
import uuid

import mock

from gnocchi import archive_policy
from gnocchi import indexer
from gnocchi.tests import base as tests_base
from gnocchi import utils


class MockException(Exception):
    pass


class TestIndexer(tests_base.TestCase):
    def test_get_driver(self):
        driver = indexer.get_driver(self.conf)
        self.assertIsInstance(driver, indexer.IndexerDriver)


class TestIndexerDriver(tests_base.TestCase):

    def test_create_archive_policy_already_exists(self):
        # NOTE(jd) This archive policy
        # is created by gnocchi.tests on setUp() :)
        self.assertRaises(indexer.ArchivePolicyAlreadyExists,
                          self.index.create_archive_policy,
                          archive_policy.ArchivePolicy("high", 0, {}))

    def test_get_archive_policy(self):
        ap = self.index.get_archive_policy("low")
        self.assertEqual({
            'back_window': 0,
            'aggregation_methods':
            set(self.conf.archive_policy.default_aggregation_methods),
            'definition': [
                {u'granularity': 300, u'points': 12, u'timespan': 3600},
                {u'granularity': 3600, u'points': 24, u'timespan': 86400},
                {u'granularity': 86400, u'points': 30, u'timespan': 2592000}],
            'name': u'low'}, dict(ap))

    def test_update_archive_policy(self):
        self.assertRaises(indexer.UnsupportedArchivePolicyChange,
                          self.index.update_archive_policy, "low",
                          [archive_policy.ArchivePolicyItem(granularity=300,
                                                            points=10)])
        self.assertRaises(indexer.UnsupportedArchivePolicyChange,
                          self.index.update_archive_policy, "low",
                          [archive_policy.ArchivePolicyItem(granularity=300,
                                                            points=12),
                           archive_policy.ArchivePolicyItem(granularity=3600,
                                                            points=12),
                           archive_policy.ArchivePolicyItem(granularity=5,
                                                            points=6)])
        apname = str(uuid.uuid4())
        self.index.create_archive_policy(archive_policy.ArchivePolicy(
            apname, 0, [(12, 300), (24, 3600), (30, 86400)]))
        ap = self.index.update_archive_policy(
            apname, [archive_policy.ArchivePolicyItem(granularity=300,
                                                      points=6),
                     archive_policy.ArchivePolicyItem(granularity=3600,
                                                      points=24),
                     archive_policy.ArchivePolicyItem(granularity=86400,
                                                      points=30)])
        self.assertEqual({
            'back_window': 0,
            'aggregation_methods':
            set(self.conf.archive_policy.default_aggregation_methods),
            'definition': [
                {u'granularity': 300, u'points': 6, u'timespan': 1800},
                {u'granularity': 3600, u'points': 24, u'timespan': 86400},
                {u'granularity': 86400, u'points': 30, u'timespan': 2592000}],
            'name': apname}, dict(ap))
        ap = self.index.update_archive_policy(
            apname, [archive_policy.ArchivePolicyItem(granularity=300,
                                                      points=12),
                     archive_policy.ArchivePolicyItem(granularity=3600,
                                                      points=24),
                     archive_policy.ArchivePolicyItem(granularity=86400,
                                                      points=30)])
        self.assertEqual({
            'back_window': 0,
            'aggregation_methods':
            set(self.conf.archive_policy.default_aggregation_methods),
            'definition': [
                {u'granularity': 300, u'points': 12, u'timespan': 3600},
                {u'granularity': 3600, u'points': 24, u'timespan': 86400},
                {u'granularity': 86400, u'points': 30, u'timespan': 2592000}],
            'name': apname}, dict(ap))

    def test_delete_archive_policy(self):
        name = str(uuid.uuid4())
        self.index.create_archive_policy(
            archive_policy.ArchivePolicy(name, 0, {}))
        self.index.delete_archive_policy(name)
        self.assertRaises(indexer.NoSuchArchivePolicy,
                          self.index.delete_archive_policy,
                          name)
        self.assertRaises(indexer.NoSuchArchivePolicy,
                          self.index.delete_archive_policy,
                          str(uuid.uuid4()))
        metric_id = uuid.uuid4()
        self.index.create_metric(metric_id, str(uuid.uuid4()),
                                 str(uuid.uuid4()), "low")
        self.assertRaises(indexer.ArchivePolicyInUse,
                          self.index.delete_archive_policy,
                          "low")
        self.index.delete_metric(metric_id)

    def test_list_ap_rules_ordered(self):
        name = str(uuid.uuid4())
        self.index.create_archive_policy(
            archive_policy.ArchivePolicy(name, 0, {}))
        self.index.create_archive_policy_rule('rule1', 'abc.*', name)
        self.index.create_archive_policy_rule('rule2', 'abc.xyz.*', name)
        self.index.create_archive_policy_rule('rule3', 'abc.xyz', name)
        rules = self.index.list_archive_policy_rules()
        # NOTE(jd) The test is not isolated, there might be more than 3 rules
        found = 0
        for r in rules:
            if r['metric_pattern'] == 'abc.xyz.*':
                found = 1
            if found == 1 and r['metric_pattern'] == 'abc.xyz':
                found = 2
            if found == 2 and r['metric_pattern'] == 'abc.*':
                break
        else:
            self.fail("Metric patterns are not ordered")

        # Ensure we can't delete the archive policy
        self.assertRaises(indexer.ArchivePolicyInUse,
                          self.index.delete_archive_policy, name)

    def test_create_metric(self):
        r1 = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        m = self.index.create_metric(r1, user, project, "low")
        self.assertEqual(r1, m.id)
        self.assertEqual(m.created_by_user_id, user)
        self.assertEqual(m.created_by_project_id, project)
        self.assertIsNone(m.name)
        self.assertIsNone(m.unit)
        self.assertIsNone(m.resource_id)
        m2 = self.index.list_metrics(id=r1)
        self.assertEqual([m], m2)

    def test_expunge_metric(self):
        r1 = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        m = self.index.create_metric(r1, user, project, "low")
        self.index.delete_metric(m.id)
        try:
            self.index.expunge_metric(m.id)
        except indexer.NoSuchMetric:
            # It's possible another test process expunged the metric just
            # before us; in that case, we're good, we'll just check that the
            # next call actually really raises NoSuchMetric anyway
            pass
        self.assertRaises(indexer.NoSuchMetric,
                          self.index.delete_metric,
                          m.id)
        self.assertRaises(indexer.NoSuchMetric,
                          self.index.expunge_metric,
                          m.id)

    def test_create_resource(self):
        r1 = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        rc = self.index.create_resource('generic', r1, user, project)
        self.assertIsNotNone(rc.started_at)
        self.assertIsNotNone(rc.revision_start)
        self.assertEqual({"id": r1,
                          "revision_start": rc.revision_start,
                          "revision_end": None,
                          "created_by_user_id": user,
                          "created_by_project_id": project,
                          "user_id": None,
                          "project_id": None,
                          "started_at": rc.started_at,
                          "ended_at": None,
                          "original_resource_id": None,
                          "type": "generic",
                          "metrics": {}},
                         rc.jsonify())
        rg = self.index.get_resource('generic', r1, with_metrics=True)
        self.assertEqual(rc, rg)
        self.assertEqual(rc.metrics, rg.metrics)

    def test_create_non_existent_metric(self):
        e = uuid.uuid4()
        try:
            self.index.create_resource(
                'generic', uuid.uuid4(), str(uuid.uuid4()), str(uuid.uuid4()),
                metrics={"foo": e})
        except indexer.NoSuchMetric as ex:
            self.assertEqual(e, ex.metric)
        else:
            self.fail("Exception not raised")

    def test_create_resource_already_exists(self):
        r1 = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        self.index.create_resource('generic', r1, user, project)
        self.assertRaises(indexer.ResourceAlreadyExists,
                          self.index.create_resource,
                          'generic', r1, user, project)

    def test_create_resource_with_new_metrics(self):
        r1 = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        rc = self.index.create_resource(
            'generic', r1, user, project,
            metrics={"foobar": {"archive_policy_name": "low"}})
        self.assertEqual(1, len(rc.metrics))
        m = self.index.list_metrics(id=rc.metrics[0].id)
        self.assertEqual(m[0], rc.metrics[0])

    def test_delete_resource(self):
        r1 = uuid.uuid4()
        self.index.create_resource('generic', r1, str(uuid.uuid4()),
                                   str(uuid.uuid4()))
        self.index.delete_resource(r1)
        self.assertRaises(indexer.NoSuchResource,
                          self.index.delete_resource,
                          r1)

    def test_delete_resource_with_metrics(self):
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        e1 = uuid.uuid4()
        e2 = uuid.uuid4()
        self.index.create_metric(e1, user, project,
                                 archive_policy_name="low")
        self.index.create_metric(e2, user, project,
                                 archive_policy_name="low")
        r1 = uuid.uuid4()
        self.index.create_resource('generic', r1, user, project,
                                   metrics={'foo': e1, 'bar': e2})
        self.index.delete_resource(r1)
        self.assertRaises(indexer.NoSuchResource,
                          self.index.delete_resource,
                          r1)
        metrics = self.index.list_metrics(ids=[e1, e2])
        self.assertEqual([], metrics)

    def test_delete_resource_non_existent(self):
        r1 = uuid.uuid4()
        self.assertRaises(indexer.NoSuchResource,
                          self.index.delete_resource,
                          r1)

    def test_create_resource_with_start_timestamp(self):
        r1 = uuid.uuid4()
        ts = utils.datetime_utc(2014, 1, 1, 23, 34, 23, 1234)
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        rc = self.index.create_resource(
            'generic',
            r1, user, project,
            started_at=ts)
        self.assertEqual({"id": r1,
                          "revision_start": rc.revision_start,
                          "revision_end": None,
                          "created_by_user_id": user,
                          "created_by_project_id": project,
                          "user_id": None,
                          "project_id": None,
                          "started_at": ts,
                          "ended_at": None,
                          "original_resource_id": None,
                          "type": "generic",
                          "metrics": {}}, rc.jsonify())
        r = self.index.get_resource('generic', r1, with_metrics=True)
        self.assertEqual(rc, r)

    def test_create_resource_with_metrics(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        e2 = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        self.index.create_metric(e1,
                                 user, project,
                                 archive_policy_name="low")
        self.index.create_metric(e2,
                                 user, project,
                                 archive_policy_name="low")
        rc = self.index.create_resource('generic', r1, user, project,
                                        metrics={'foo': e1, 'bar': e2})
        self.assertIsNotNone(rc.started_at)
        self.assertIsNotNone(rc.revision_start)
        self.assertEqual({"id": r1,
                          "revision_start": rc.revision_start,
                          "revision_end": None,
                          "created_by_user_id": user,
                          "created_by_project_id": project,
                          "user_id": None,
                          "project_id": None,
                          "started_at": rc.started_at,
                          "ended_at": None,
                          "original_resource_id": None,
                          "type": "generic",
                          "metrics": {'foo': str(e1), 'bar': str(e2)}},
                         rc.jsonify())
        r = self.index.get_resource('generic', r1, with_metrics=True)
        self.assertIsNotNone(r.started_at)
        self.assertEqual({"id": r1,
                          "revision_start": r.revision_start,
                          "revision_end": None,
                          "created_by_user_id": user,
                          "created_by_project_id": project,
                          "type": "generic",
                          "started_at": rc.started_at,
                          "ended_at": None,
                          "user_id": None,
                          "project_id": None,
                          "original_resource_id": None,
                          "metrics": {'foo': str(e1), 'bar': str(e2)}},
                         r.jsonify())

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
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        self.index.create_resource('generic', r1, user, project)
        self.index.update_resource(
            'generic',
            r1,
            ended_at=utils.datetime_utc(2043, 1, 1, 2, 3, 4))
        r = self.index.get_resource('generic', r1, with_metrics=True)
        self.assertIsNotNone(r.started_at)
        self.assertIsNone(r.user_id)
        self.assertIsNone(r.project_id)
        self.assertIsNone(r.revision_end)
        self.assertIsNotNone(r.revision_start)
        self.assertEqual(r1, r.id)
        self.assertEqual(user, r.created_by_user_id)
        self.assertEqual(project, r.created_by_project_id)
        self.assertEqual(utils.datetime_utc(2043, 1, 1, 2, 3, 4), r.ended_at)
        self.assertEqual("generic", r.type)
        self.assertEqual(0, len(r.metrics))
        self.index.update_resource(
            'generic',
            r1,
            ended_at=None)
        r = self.index.get_resource('generic', r1, with_metrics=True)
        self.assertIsNotNone(r.started_at)
        self.assertIsNotNone(r.revision_start)
        self.assertEqual({"id": r1,
                          "revision_start": r.revision_start,
                          "revision_end": None,
                          "ended_at": None,
                          "created_by_user_id": user,
                          "created_by_project_id": project,
                          "user_id": None,
                          "project_id": None,
                          "type": "generic",
                          "started_at": r.started_at,
                          "original_resource_id": None,
                          "metrics": {}}, r.jsonify())

    def test_update_resource_metrics(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        e2 = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        self.index.create_metric(e1, user, project,
                                 archive_policy_name="low")
        self.index.create_resource('generic', r1, user, project,
                                   metrics={'foo': e1})
        self.index.create_metric(e2, user, project,
                                 archive_policy_name="low")
        rc = self.index.update_resource('generic', r1, metrics={'bar': e2})
        r = self.index.get_resource('generic', r1, with_metrics=True)
        self.assertEqual(rc, r)

    def test_update_resource_metrics_append(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        e2 = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        self.index.create_metric(e1, user, project,
                                 archive_policy_name="low")
        self.index.create_metric(e2, user, project,
                                 archive_policy_name="low")
        self.index.create_resource('generic', r1, user, project,
                                   metrics={'foo': e1})
        rc = self.index.update_resource('generic', r1, metrics={'bar': e2},
                                        append_metrics=True)
        r = self.index.get_resource('generic', r1, with_metrics=True)
        self.assertEqual(rc, r)
        metric_names = [m.name for m in rc.metrics]
        self.assertIn('foo', metric_names)
        self.assertIn('bar', metric_names)

    def test_update_resource_metrics_append_fail(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        e2 = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        self.index.create_metric(e1, user, project,
                                 archive_policy_name="low")
        self.index.create_metric(e2, user, project,
                                 archive_policy_name="low")
        self.index.create_resource('generic', r1, user, project,
                                   metrics={'foo': e1})

        self.assertRaises(indexer.NamedMetricAlreadyExists,
                          self.index.update_resource,
                          'generic', r1, metrics={'foo': e2},
                          append_metrics=True)
        r = self.index.get_resource('generic', r1, with_metrics=True)
        self.assertEqual(e1, r.metrics[0].id)

    def test_update_resource_attribute(self):
        mgr = self.index.get_resource_type_schema()
        resource_type = str(uuid.uuid4())
        rtype = mgr.resource_type_from_dict(resource_type, {
            "col1": {"type": "string", "required": True,
                     "min_length": 2, "max_length": 15}
        }, 'creating')
        r1 = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        # Create
        self.index.create_resource_type(rtype)

        rc = self.index.create_resource(resource_type, r1, user, project,
                                        col1="foo")
        rc = self.index.update_resource(resource_type, r1, col1="foo")
        r = self.index.get_resource(resource_type, r1, with_metrics=True)
        self.assertEqual(rc, r)

    def test_update_resource_no_change(self):
        mgr = self.index.get_resource_type_schema()
        resource_type = str(uuid.uuid4())
        rtype = mgr.resource_type_from_dict(resource_type, {
            "col1": {"type": "string", "required": True,
                     "min_length": 2, "max_length": 15}
        }, 'creating')
        self.index.create_resource_type(rtype)
        r1 = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        rc = self.index.create_resource(resource_type, r1, user, project,
                                        col1="foo")
        updated = self.index.update_resource(resource_type, r1, col1="foo",
                                             create_revision=False)
        r = self.index.list_resources(resource_type,
                                      {"=": {"id": r1}},
                                      history=True)
        self.assertEqual(1, len(r))
        self.assertEqual(dict(rc), dict(r[0]))
        self.assertEqual(dict(updated), dict(r[0]))

    def test_update_resource_ended_at_fail(self):
        r1 = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        self.index.create_resource('generic', r1, user, project)
        self.assertRaises(
            indexer.ResourceValueError,
            self.index.update_resource,
            'generic', r1,
            ended_at=utils.datetime_utc(2010, 1, 1, 1, 1, 1))

    def test_update_resource_unknown_attribute(self):
        mgr = self.index.get_resource_type_schema()
        resource_type = str(uuid.uuid4())
        rtype = mgr.resource_type_from_dict(resource_type, {
            "col1": {"type": "string", "required": False,
                     "min_length": 1, "max_length": 2},
        }, 'creating')
        self.index.create_resource_type(rtype)
        r1 = uuid.uuid4()
        self.index.create_resource(resource_type, r1,
                                   str(uuid.uuid4()), str(uuid.uuid4()))
        self.assertRaises(indexer.ResourceAttributeError,
                          self.index.update_resource,
                          resource_type,
                          r1, foo="bar")

    def test_update_non_existent_metric(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        self.index.create_resource('generic', r1, str(uuid.uuid4()),
                                   str(uuid.uuid4()))
        self.assertRaises(indexer.NoSuchMetric,
                          self.index.update_resource,
                          'generic',
                          r1, metrics={'bar': e1})

    def test_update_non_existent_resource(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        self.index.create_metric(e1, str(uuid.uuid4()), str(uuid.uuid4()),
                                 archive_policy_name="low")
        self.assertRaises(indexer.NoSuchResource,
                          self.index.update_resource,
                          'generic',
                          r1, metrics={'bar': e1})

    def test_create_resource_with_non_existent_metrics(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        self.assertRaises(indexer.NoSuchMetric,
                          self.index.create_resource,
                          'generic',
                          r1, str(uuid.uuid4()), str(uuid.uuid4()),
                          metrics={'foo': e1})

    def test_delete_metric_on_resource(self):
        r1 = uuid.uuid4()
        e1 = uuid.uuid4()
        e2 = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        self.index.create_metric(e1, user, project,
                                 archive_policy_name="low")
        self.index.create_metric(e2, user, project,
                                 archive_policy_name="low")
        rc = self.index.create_resource('generic', r1, user, project,
                                        metrics={'foo': e1, 'bar': e2})
        self.index.delete_metric(e1)
        self.assertRaises(indexer.NoSuchMetric, self.index.delete_metric, e1)
        r = self.index.get_resource('generic', r1, with_metrics=True)
        self.assertIsNotNone(r.started_at)
        self.assertIsNotNone(r.revision_start)
        self.assertEqual({"id": r1,
                          "started_at": r.started_at,
                          "revision_start": rc.revision_start,
                          "revision_end": None,
                          "ended_at": None,
                          "created_by_user_id": user,
                          "created_by_project_id": project,
                          "user_id": None,
                          "project_id": None,
                          "original_resource_id": None,
                          "type": "generic",
                          "metrics": {'bar': str(e2)}}, r.jsonify())

    def test_delete_resource_custom(self):
        mgr = self.index.get_resource_type_schema()
        resource_type = str(uuid.uuid4())
        self.index.create_resource_type(
            mgr.resource_type_from_dict(resource_type, {
                "flavor_id": {"type": "string",
                              "min_length": 1,
                              "max_length": 20,
                              "required": True}
            }, 'creating'))
        r1 = uuid.uuid4()
        created = self.index.create_resource(resource_type, r1,
                                             str(uuid.uuid4()),
                                             str(uuid.uuid4()),
                                             flavor_id="foo")
        got = self.index.get_resource(resource_type, r1, with_metrics=True)
        self.assertEqual(created, got)
        self.index.delete_resource(r1)
        got = self.index.get_resource(resource_type, r1)
        self.assertIsNone(got)

    def test_list_resources_by_unknown_field(self):
        self.assertRaises(indexer.ResourceAttributeError,
                          self.index.list_resources,
                          'generic',
                          attribute_filter={"=": {"fern": "bar"}})

    def test_list_resources_by_user(self):
        r1 = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        g = self.index.create_resource('generic', r1, user, project,
                                       user, project)
        resources = self.index.list_resources(
            'generic',
            attribute_filter={"=": {"user_id": user}})
        self.assertEqual(1, len(resources))
        self.assertEqual(g, resources[0])
        resources = self.index.list_resources(
            'generic',
            attribute_filter={"=": {"user_id": 'bad-user'}})
        self.assertEqual(0, len(resources))

    def test_list_resources_by_created_by_user(self):
        r1 = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        g = self.index.create_resource('generic', r1, user, project)
        resources = self.index.list_resources(
            'generic',
            attribute_filter={"=": {"created_by_user_id": user}})
        self.assertEqual(1, len(resources))
        self.assertEqual(g, resources[0])
        resources = self.index.list_resources(
            'generic',
            attribute_filter={"=": {"created_by_user_id": 'bad-user'}})
        self.assertEqual(0, len(resources))

    def test_list_resources_by_user_with_details(self):
        r1 = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        g = self.index.create_resource('generic', r1, user, project,
                                       user, project)
        mgr = self.index.get_resource_type_schema()
        resource_type = str(uuid.uuid4())
        self.index.create_resource_type(
            mgr.resource_type_from_dict(resource_type, {}, 'creating'))
        r2 = uuid.uuid4()
        i = self.index.create_resource(resource_type, r2,
                                       user, project,
                                       user, project)
        resources = self.index.list_resources(
            'generic',
            attribute_filter={"=": {"user_id": user}},
            details=True,
        )
        self.assertEqual(2, len(resources))
        self.assertIn(g, resources)
        self.assertIn(i, resources)

    def test_list_resources_by_project(self):
        r1 = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        g = self.index.create_resource('generic', r1, user, project,
                                       user, project)
        resources = self.index.list_resources(
            'generic',
            attribute_filter={"=": {"project_id": project}})
        self.assertEqual(1, len(resources))
        self.assertEqual(g, resources[0])
        resources = self.index.list_resources(
            'generic',
            attribute_filter={"=": {"project_id": 'bad-project'}})
        self.assertEqual(0, len(resources))

    def test_list_resources_by_duration(self):
        r1 = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        g = self.index.create_resource(
            'generic', r1, user, project,
            user_id=user, project_id=project,
            started_at=utils.datetime_utc(2010, 1, 1, 12, 0),
            ended_at=utils.datetime_utc(2010, 1, 1, 13, 0))
        resources = self.index.list_resources(
            'generic',
            attribute_filter={"and": [
                {"=": {"project_id": project}},
                {">": {"lifespan": 1800}},
            ]})
        self.assertEqual(1, len(resources))
        self.assertEqual(g, resources[0])
        resources = self.index.list_resources(
            'generic',
            attribute_filter={"and": [
                {"=": {"project_id": project}},
                {">": {"lifespan": 7200}},
            ]})
        self.assertEqual(0, len(resources))

    def test_list_resources(self):
        # NOTE(jd) So this test is a bit fuzzy right now as we uses the same
        # database for all tests and the tests are running concurrently, but
        # for now it'll be better than nothing.
        r1 = uuid.uuid4()
        g = self.index.create_resource('generic', r1,
                                       str(uuid.uuid4()), str(uuid.uuid4()))
        mgr = self.index.get_resource_type_schema()
        resource_type = str(uuid.uuid4())
        self.index.create_resource_type(
            mgr.resource_type_from_dict(resource_type, {}, 'creating'))
        r2 = uuid.uuid4()
        i = self.index.create_resource(resource_type, r2,
                                       str(uuid.uuid4()), str(uuid.uuid4()))
        resources = self.index.list_resources('generic')
        self.assertGreaterEqual(len(resources), 2)
        g_found = False
        i_found = False
        for r in resources:
            if r.id == r1:
                self.assertEqual(g, r)
                g_found = True
            elif r.id == r2:
                i_found = True
            if i_found and g_found:
                break
        else:
            self.fail("Some resources were not found")

        resources = self.index.list_resources(resource_type)
        self.assertGreaterEqual(len(resources), 1)
        for r in resources:
            if r.id == r2:
                self.assertEqual(i, r)
                break
        else:
            self.fail("Some resources were not found")

    def test_list_resource_weird_uuid(self):
        r = self.index.list_resources(
            'generic', attribute_filter={"=": {"id": "f00bar"}})
        self.assertEqual(0, len(r))
        self.assertRaises(
            indexer.QueryValueError,
            self.index.list_resources,
            'generic',
            attribute_filter={"=": {"id": "f00bar" * 50}})

    def test_list_resource_attribute_type_numeric(self):
        """Test that we can pass an integer to filter on a string type."""
        mgr = self.index.get_resource_type_schema()
        resource_type = str(uuid.uuid4())
        self.index.create_resource_type(
            mgr.resource_type_from_dict(resource_type, {
                "flavor_id": {"type": "string",
                              "min_length": 1,
                              "max_length": 20,
                              "required": False},
            }, 'creating'))
        r = self.index.list_resources(
            resource_type, attribute_filter={"=": {"flavor_id": 1.0}})
        self.assertEqual(0, len(r))

    def test_list_resource_weird_date(self):
        self.assertRaises(
            indexer.QueryValueError,
            self.index.list_resources,
            'generic',
            attribute_filter={"=": {"started_at": "f00bar"}})

    def test_list_resources_without_history(self):
        e = uuid.uuid4()
        rid = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        new_user = str(uuid.uuid4())
        new_project = str(uuid.uuid4())

        self.index.create_metric(e, user, project,
                                 archive_policy_name="low")

        self.index.create_resource('generic', rid, user, project,
                                   user, project,
                                   metrics={'foo': e})
        r2 = self.index.update_resource('generic', rid, user_id=new_user,
                                        project_id=new_project,
                                        append_metrics=True).jsonify()

        self.assertEqual({'foo': str(e)}, r2['metrics'])
        self.assertEqual(new_user, r2['user_id'])
        self.assertEqual(new_project, r2['project_id'])
        resources = self.index.list_resources('generic', history=False,
                                              details=True)
        self.assertGreaterEqual(len(resources), 1)
        expected_resources = [r.jsonify() for r in resources
                              if r.id == rid]
        self.assertIn(r2, expected_resources)

    def test_list_resources_with_history(self):
        e1 = uuid.uuid4()
        e2 = uuid.uuid4()
        rid = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        new_user = str(uuid.uuid4())
        new_project = str(uuid.uuid4())

        self.index.create_metric(e1, user, project,
                                 archive_policy_name="low")
        self.index.create_metric(e2, user, project,
                                 archive_policy_name="low")
        self.index.create_metric(uuid.uuid4(), user, project,
                                 archive_policy_name="low")

        r1 = self.index.create_resource('generic', rid, user, project,
                                        user, project,
                                        metrics={'foo': e1, 'bar': e2}
                                        ).jsonify()
        r2 = self.index.update_resource('generic', rid, user_id=new_user,
                                        project_id=new_project,
                                        append_metrics=True).jsonify()

        r1['revision_end'] = r2['revision_start']
        r2['revision_end'] = None
        self.assertEqual({'foo': str(e1),
                          'bar': str(e2)}, r2['metrics'])
        self.assertEqual(new_user, r2['user_id'])
        self.assertEqual(new_project, r2['project_id'])
        resources = self.index.list_resources('generic', history=True,
                                              details=False,
                                              attribute_filter={
                                                  "=": {"id": rid}})
        self.assertGreaterEqual(len(resources), 2)
        resources = sorted(
            [r.jsonify() for r in resources],
            key=operator.itemgetter("revision_start"))
        self.assertEqual([r1, r2], resources)

    def test_list_resources_custom_with_history(self):
        e1 = uuid.uuid4()
        e2 = uuid.uuid4()
        rid = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        new_user = str(uuid.uuid4())
        new_project = str(uuid.uuid4())

        mgr = self.index.get_resource_type_schema()
        resource_type = str(uuid.uuid4())
        self.index.create_resource_type(
            mgr.resource_type_from_dict(resource_type, {
                "col1": {"type": "string", "required": True,
                         "min_length": 2, "max_length": 15}
            }, 'creating'))

        self.index.create_metric(e1, user, project,
                                 archive_policy_name="low")
        self.index.create_metric(e2, user, project,
                                 archive_policy_name="low")
        self.index.create_metric(uuid.uuid4(), user, project,
                                 archive_policy_name="low")

        r1 = self.index.create_resource(resource_type, rid, user, project,
                                        user, project,
                                        col1="foo",
                                        metrics={'foo': e1, 'bar': e2}
                                        ).jsonify()
        r2 = self.index.update_resource(resource_type, rid, user_id=new_user,
                                        project_id=new_project,
                                        col1="bar",
                                        append_metrics=True).jsonify()

        r1['revision_end'] = r2['revision_start']
        r2['revision_end'] = None
        self.assertEqual({'foo': str(e1),
                          'bar': str(e2)}, r2['metrics'])
        self.assertEqual(new_user, r2['user_id'])
        self.assertEqual(new_project, r2['project_id'])
        self.assertEqual('bar', r2['col1'])
        resources = self.index.list_resources(resource_type, history=True,
                                              details=False,
                                              attribute_filter={
                                                  "=": {"id": rid}})
        self.assertGreaterEqual(len(resources), 2)
        resources = sorted(
            [r.jsonify() for r in resources],
            key=operator.itemgetter("revision_start"))
        self.assertEqual([r1, r2], resources)

    def test_list_resources_started_after_ended_before(self):
        # NOTE(jd) So this test is a bit fuzzy right now as we uses the same
        # database for all tests and the tests are running concurrently, but
        # for now it'll be better than nothing.
        r1 = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        g = self.index.create_resource(
            'generic', r1, user, project,
            started_at=utils.datetime_utc(2000, 1, 1, 23, 23, 23),
            ended_at=utils.datetime_utc(2000, 1, 3, 23, 23, 23))
        r2 = uuid.uuid4()
        mgr = self.index.get_resource_type_schema()
        resource_type = str(uuid.uuid4())
        self.index.create_resource_type(
            mgr.resource_type_from_dict(resource_type, {}, 'creating'))
        i = self.index.create_resource(
            resource_type, r2, user, project,
            started_at=utils.datetime_utc(2000, 1, 1, 23, 23, 23),
            ended_at=utils.datetime_utc(2000, 1, 4, 23, 23, 23))
        resources = self.index.list_resources(
            'generic',
            attribute_filter={
                "and":
                [{">=": {"started_at":
                         utils.datetime_utc(2000, 1, 1, 23, 23, 23)}},
                 {"<": {"ended_at":
                        utils.datetime_utc(2000, 1, 5, 23, 23, 23)}}]})
        self.assertGreaterEqual(len(resources), 2)
        g_found = False
        i_found = False
        for r in resources:
            if r.id == r1:
                self.assertEqual(g, r)
                g_found = True
            elif r.id == r2:
                i_found = True
            if i_found and g_found:
                break
        else:
            self.fail("Some resources were not found")

        resources = self.index.list_resources(
            resource_type,
            attribute_filter={
                ">=": {
                    "started_at": datetime.datetime(2000, 1, 1, 23, 23, 23)
                },
            })
        self.assertGreaterEqual(len(resources), 1)
        for r in resources:
            if r.id == r2:
                self.assertEqual(i, r)
                break
        else:
            self.fail("Some resources were not found")

        resources = self.index.list_resources(
            'generic',
            attribute_filter={
                "<": {
                    "ended_at": datetime.datetime(1999, 1, 1, 23, 23, 23)
                },
            })
        self.assertEqual(0, len(resources))

    def test_deletes_resources(self):
        r1 = uuid.uuid4()
        r2 = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        metrics = {'foo': {'archive_policy_name': 'medium'}}
        g1 = self.index.create_resource('generic', r1, user, project,
                                        user, project, metrics=metrics)
        g2 = self.index.create_resource('generic', r2, user, project,
                                        user, project, metrics=metrics)

        metrics = self.index.list_metrics(ids=[g1['metrics'][0]['id'],
                                               g2['metrics'][0]['id']])
        self.assertEqual(2, len(metrics))
        for m in metrics:
            self.assertEqual('active', m['status'])

        deleted = self.index.delete_resources(
            'generic',
            attribute_filter={"=": {"user_id": user}})
        self.assertEqual(2, deleted)

        resources = self.index.list_resources(
            'generic',
            attribute_filter={"=": {"user_id": user}})
        self.assertEqual(0, len(resources))

        metrics = self.index.list_metrics(ids=[g1['metrics'][0]['id'],
                                               g2['metrics'][0]['id']],
                                          status='delete')
        self.assertEqual(2, len(metrics))
        for m in metrics:
            self.assertEqual('delete', m['status'])

    def test_get_metric(self):
        e1 = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        self.index.create_metric(e1,
                                 user, project,
                                 archive_policy_name="low")

        metric = self.index.list_metrics(id=e1)
        self.assertEqual(1, len(metric))
        metric = metric[0]
        self.assertEqual(e1, metric.id)
        self.assertEqual(metric.created_by_user_id, user)
        self.assertEqual(metric.created_by_project_id, project)
        self.assertIsNone(metric.name)
        self.assertIsNone(metric.resource_id)

    def test_get_metric_with_details(self):
        e1 = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        self.index.create_metric(e1,
                                 user, project,
                                 archive_policy_name="low")

        metric = self.index.list_metrics(id=e1)
        self.assertEqual(1, len(metric))
        metric = metric[0]
        self.assertEqual(e1, metric.id)
        self.assertEqual(metric.created_by_user_id, user)
        self.assertEqual(metric.created_by_project_id, project)
        self.assertIsNone(metric.name)
        self.assertIsNone(metric.resource_id)
        self.assertEqual(self.archive_policies['low'], metric.archive_policy)

    def test_get_metric_with_bad_uuid(self):
        e1 = uuid.uuid4()
        self.assertEqual([], self.index.list_metrics(id=e1))

    def test_get_metric_empty_list_uuids(self):
        self.assertEqual([], self.index.list_metrics(ids=[]))

    def test_list_metrics(self):
        e1 = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        self.index.create_metric(e1,
                                 user, project,
                                 archive_policy_name="low")
        e2 = uuid.uuid4()
        self.index.create_metric(e2,
                                 user, project,
                                 archive_policy_name="low")
        metrics = self.index.list_metrics()
        id_list = [m.id for m in metrics]
        self.assertIn(e1, id_list)
        # Test ordering
        if e1 < e2:
            self.assertLess(id_list.index(e1), id_list.index(e2))
        else:
            self.assertLess(id_list.index(e2), id_list.index(e1))

    def test_list_metrics_delete_status(self):
        e1 = uuid.uuid4()
        user = str(uuid.uuid4())
        project = str(uuid.uuid4())
        self.index.create_metric(e1,
                                 user, project,
                                 archive_policy_name="low")
        self.index.delete_metric(e1)
        metrics = self.index.list_metrics()
        self.assertNotIn(e1, [m.id for m in metrics])

    def test_resource_type_crud(self):
        mgr = self.index.get_resource_type_schema()
        rtype = mgr.resource_type_from_dict("indexer_test", {
            "col1": {"type": "string", "required": True,
                     "min_length": 2, "max_length": 15}
        }, "creating")

        # Create
        self.index.create_resource_type(rtype)
        self.assertRaises(indexer.ResourceTypeAlreadyExists,
                          self.index.create_resource_type,
                          rtype)

        # Get
        rtype = self.index.get_resource_type("indexer_test")
        self.assertEqual("indexer_test", rtype.name)
        self.assertEqual(1, len(rtype.attributes))
        self.assertEqual("col1", rtype.attributes[0].name)
        self.assertEqual("string", rtype.attributes[0].typename)
        self.assertEqual(15, rtype.attributes[0].max_length)
        self.assertEqual(2, rtype.attributes[0].min_length)
        self.assertEqual("active", rtype.state)

        # List
        rtypes = self.index.list_resource_types()
        for rtype in rtypes:
            if rtype.name == "indexer_test":
                break
        else:
            self.fail("indexer_test not found")

        # Test resource itself
        rid = uuid.uuid4()
        self.index.create_resource("indexer_test", rid,
                                   str(uuid.uuid4()),
                                   str(uuid.uuid4()),
                                   col1="col1_value")
        r = self.index.get_resource("indexer_test", rid)
        self.assertEqual("indexer_test", r.type)
        self.assertEqual("col1_value", r.col1)

        # Deletion
        self.assertRaises(indexer.ResourceTypeInUse,
                          self.index.delete_resource_type,
                          "indexer_test")
        self.index.delete_resource(rid)
        self.index.delete_resource_type("indexer_test")

        # Ensure it's deleted
        self.assertRaises(indexer.NoSuchResourceType,
                          self.index.get_resource_type,
                          "indexer_test")

        self.assertRaises(indexer.NoSuchResourceType,
                          self.index.delete_resource_type,
                          "indexer_test")

    def _get_rt_state(self, name):
        return self.index.get_resource_type(name).state

    def test_resource_type_unexpected_creation_error(self):
        mgr = self.index.get_resource_type_schema()
        rtype = mgr.resource_type_from_dict("indexer_test_fail", {
            "col1": {"type": "string", "required": True,
                     "min_length": 2, "max_length": 15}
        }, "creating")

        states = {'before': None,
                  'after': None}

        def map_and_create_mock(rt, conn):
            states['before'] = self._get_rt_state("indexer_test_fail")
            raise MockException("boom!")

        with mock.patch.object(self.index._RESOURCE_TYPE_MANAGER,
                               "map_and_create_tables",
                               side_effect=map_and_create_mock):
            self.assertRaises(MockException,
                              self.index.create_resource_type,
                              rtype)
            states['after'] = self._get_rt_state('indexer_test_fail')

        self.assertEqual([('after', 'creation_error'),
                          ('before', 'creating')],
                         sorted(states.items()))

    def test_resource_type_unexpected_deleting_error(self):
        mgr = self.index.get_resource_type_schema()
        rtype = mgr.resource_type_from_dict("indexer_test_fail2", {
            "col1": {"type": "string", "required": True,
                     "min_length": 2, "max_length": 15}
        }, "creating")
        self.index.create_resource_type(rtype)

        states = {'before': None,
                  'after': None}

        def map_and_create_mock(rt, conn):
            states['before'] = self._get_rt_state("indexer_test_fail2")
            raise MockException("boom!")

        with mock.patch.object(self.index._RESOURCE_TYPE_MANAGER,
                               "unmap_and_delete_tables",
                               side_effect=map_and_create_mock):
            self.assertRaises(MockException,
                              self.index.delete_resource_type,
                              rtype.name)
            states['after'] = self._get_rt_state('indexer_test_fail2')

        self.assertEqual([('after', 'deletion_error'),
                          ('before', 'deleting')],
                         sorted(states.items()))

        # We can cleanup the mess !
        self.index.delete_resource_type("indexer_test_fail2")

        # Ensure it's deleted
        self.assertRaises(indexer.NoSuchResourceType,
                          self.index.get_resource_type,
                          "indexer_test_fail2")

        self.assertRaises(indexer.NoSuchResourceType,
                          self.index.delete_resource_type,
                          "indexer_test_fail2")
