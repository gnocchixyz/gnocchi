
# -*- encoding: utf-8 -*-
#
# Copyright © 2016-2017 Red Hat, Inc.
# Copyright © 2015 eNovance
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

import ujson
import uuid


import mock
import numpy

from gnocchi import amqp1d
from gnocchi import indexer
from gnocchi.tests import base as tests_base
from gnocchi import utils


def datetime64(*args):
    return numpy.datetime64(datetime.datetime(*args))


class TestAmqp1d(tests_base.TestCase):

    AMQP1D_USER_ID = str(uuid.uuid4())
    AMQP1D_PROJECT_ID = str(uuid.uuid4())
    AMQP1D_ARCHIVE_POLICY_NAME = "medium"

    def setUp(self):
        super(TestAmqp1d, self).setUp()
        self.conf.set_override("resource_name",
                               "collectd_amqp1d", "amqp1d")
        self.conf.set_override("creator",
                               self.AMQP1D_USER_ID, "amqp1d")
        self.conf.set_override("archive_policy_name",
                               self.AMQP1D_ARCHIVE_POLICY_NAME, "amqp1d")

        self.stats = amqp1d.CollectdStats(self.conf)

        # Replace storage/indexer with correct ones that have been upgraded
        self.stats.incoming = self.incoming
        self.stats.indexer = self.index
        self.server = amqp1d.AMQP1Server(self.conf, self.stats)

    def test_flush_empty(self):
        self.server.stats.flush()

    @mock.patch.object(utils, 'utcnow')
    def _test_gauge(self, metrics, utcnow):
        utcnow.return_value = utils.datetime_utc(2017, 1, 10, 13, 58, 36)
        self.server.process_collectd_message(metrics[0])
        self.stats.flush()

        metric_in_json = ujson.loads(metrics[0])
        metric_name = amqp1d.CollectdStats.serialize_identifier(
            0, metric_in_json[0]
        )
        host = metric_in_json[0]["host"]

        resources = self.stats.indexer.list_resources(
            self.conf.amqp1d.resource_name,
            attribute_filter={"=": {"host": host}}
        )

        self.assertIsNotNone(resources)
        resource = self.stats.indexer.get_resource(
            self.conf.amqp1d.resource_name,
            resources[0].id, with_metrics=True
        )
        self.assertIsNotNone(resource)
        metric = resource.get_metric(metric_name)
        self.assertIsNotNone(metric)

        self.storage.process_new_measures(
            self.stats.indexer,
            self.stats.incoming,
            [str(metric.id)], sync=True
        )

        measures = self.storage.get_measures(metric)
        self.assertEqual([
            (datetime64(2017, 1, 10), numpy.timedelta64(1, 'D'), 129),
            (datetime64(2017, 1, 10, 13), numpy.timedelta64(1, 'h'), 129),
            (datetime64(2017, 1, 10, 13, 58), numpy.timedelta64(1, 'm'), 129)
        ], measures)

    @mock.patch.object(utils, 'utcnow')
    def _test_counters(self, metrics, utcnow):
        """"

        this creates two metrics names for derived types (collectd counter).

        """

        utcnow.return_value = utils.datetime_utc(2017, 1, 10, 13, 58, 36)
        self.server.process_collectd_message(metrics[0])
        self.stats.flush()
        metric_in_json = ujson.loads(metrics[0])
        metric_names = []
        for metric in metric_in_json:
            for index, value in enumerate(metric["values"]):
                metric_names.append(amqp1d.CollectdStats.serialize_identifier(
                    index, metric))

        host = metric_in_json[0]["host"]

        resources = self.stats.indexer.list_resources(
            self.conf.amqp1d.resource_name,
            attribute_filter={"=": {"host": host}}
        )

        self.assertIsNotNone(resources)
        resource = self.stats.indexer.get_resource(
            self.conf.amqp1d.resource_name,
            resources[0].id,
            with_metrics=True
        )
        self.assertIsNotNone(resource)
        for metric_name in metric_names:
            metric = resource.get_metric(metric_name)
            self.assertIsNotNone(metric)
            self.storage.process_new_measures(
                self.stats.indexer, self.stats.incoming,
                [str(metric.id)], sync=True)
            measures = self.storage.get_measures(metric)
            self.assertEqual([
                (datetime64(2017, 1, 10), numpy.timedelta64(1, 'D'), 0),
                (datetime64(2017, 1, 10, 13), numpy.timedelta64(1, 'h'), 0),
                (datetime64(2017, 1, 10, 13, 58), numpy.timedelta64(1, 'm'), 0)
            ], measures)

    def test_gauge(self):
        metric_gauge = []
        metric_gauge.append("""[{\"values\":[129],\"dstypes\":
                            [\"gauge\"],\"dsnames\":[\"value\"],
                            \"time\":1506712460.824,
                             \"dsnames\":[\"value\"],
                             \"time\":1506712460.824,
                             \"interval\":10.000,\"host\":
                             \"www.gnocchi.test.com\",
                             \"plugin\":\"memory\",
                             \"plugin_instance\":\"\",
                             \"type\":\"memory\",
                             \"type_instance\":\"free\"}]""")

        self._test_gauge(metric_gauge)

    def test_counters(self):
        metric_counters = []
        metric_counters.append("""[{\"values\":[0,0],\"dstypes\":
                                [\"derive\",\"derive\"],
                                \"dsnames\":[\"rx\",\"tx\"],
                                \"time\":1506712460.824,
                                \"interval\":10.000,\"host\":
                                \"www.gnocchi.test.com\",
                                \"plugin\":\"interface\",
                                \"plugin_instance\":\"ens2f1\",
                                \"type\":\"if_errors\",
                                \"type_instance\": \"\"}]""")
        self._test_counters(metric_counters)


class TestAmqp1dArchivePolicyRule(TestAmqp1d):
    AMQP1D_ARCHIVE_POLICY_NAME = ""

    def setUp(self):
        super(TestAmqp1dArchivePolicyRule, self).setUp()
        try:
            self.stats.indexer.create_archive_policy_rule(
                "amqp1d", "*", "medium")
        except indexer.ArchivePolicyRuleAlreadyExists:
            # Created by another test run
            pass
