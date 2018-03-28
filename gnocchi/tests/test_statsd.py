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
import uuid

import mock
import numpy

from gnocchi import indexer
from gnocchi import statsd
from gnocchi.tests import base as tests_base
from gnocchi import utils


def datetime64(*args):
    return numpy.datetime64(datetime.datetime(*args))


class TestStatsd(tests_base.TestCase):

    STATSD_USER_ID = str(uuid.uuid4())
    STATSD_PROJECT_ID = str(uuid.uuid4())
    STATSD_ARCHIVE_POLICY_NAME = "medium"

    def setUp(self):
        super(TestStatsd, self).setUp()

        self.conf.set_override("resource_id",
                               str(uuid.uuid4()), "statsd")
        self.conf.set_override("creator",
                               self.STATSD_USER_ID, "statsd")
        self.conf.set_override("archive_policy_name",
                               self.STATSD_ARCHIVE_POLICY_NAME, "statsd")
        ap = self.ARCHIVE_POLICIES["medium"]
        self.aggregations = ap.get_aggregations_for_method("mean")

        self.stats = statsd.Stats(self.conf)
        # Replace storage/indexer with correct ones that have been upgraded
        self.stats.incoming = self.incoming
        self.stats.indexer = self.index
        self.server = statsd.StatsdServer(self.stats)

    def test_flush_empty(self):
        self.server.stats.flush()

    @mock.patch.object(utils, 'utcnow')
    def _test_gauge_or_ms(self, metric_type, utcnow):
        metric_name = "test_gauge_or_ms"
        metric_key = metric_name + "|" + metric_type
        utcnow.return_value = utils.datetime_utc(2015, 1, 7, 13, 58, 36)
        self.server.datagram_received(
            ("%s:1|%s" % (metric_name, metric_type)).encode('ascii'),
            ("127.0.0.1", 12345))
        self.stats.flush()

        r = self.stats.indexer.get_resource('generic',
                                            self.conf.statsd.resource_id,
                                            with_metrics=True)

        metric = r.get_metric(metric_key)

        self.trigger_processing([metric])

        measures = self.storage.get_measures(metric, self.aggregations)
        self.assertEqual({"mean": [
            (datetime64(2015, 1, 7), numpy.timedelta64(1, 'D'), 1.0),
            (datetime64(2015, 1, 7, 13), numpy.timedelta64(1, 'h'), 1.0),
            (datetime64(2015, 1, 7, 13, 58), numpy.timedelta64(1, 'm'), 1.0)
        ]}, measures)

        utcnow.return_value = utils.datetime_utc(2015, 1, 7, 13, 59, 37)
        # This one is going to be ignored
        self.server.datagram_received(
            ("%s:45|%s" % (metric_name, metric_type)).encode('ascii'),
            ("127.0.0.1", 12345))
        self.server.datagram_received(
            ("%s:2|%s" % (metric_name, metric_type)).encode('ascii'),
            ("127.0.0.1", 12345))
        self.stats.flush()

        self.trigger_processing([metric])

        measures = self.storage.get_measures(metric, self.aggregations)
        self.assertEqual({"mean": [
            (datetime64(2015, 1, 7), numpy.timedelta64(1, 'D'), 1.5),
            (datetime64(2015, 1, 7, 13), numpy.timedelta64(1, 'h'), 1.5),
            (datetime64(2015, 1, 7, 13, 58), numpy.timedelta64(1, 'm'), 1.0),
            (datetime64(2015, 1, 7, 13, 59), numpy.timedelta64(1, 'm'), 2.0)
        ]}, measures)

    def test_gauge(self):
        self._test_gauge_or_ms("g")

    def test_ms(self):
        self._test_gauge_or_ms("ms")

    @mock.patch.object(utils, 'utcnow')
    def test_counter(self, utcnow):
        metric_name = "test_counter"
        metric_key = metric_name + "|c"
        utcnow.return_value = utils.datetime_utc(2015, 1, 7, 13, 58, 36)
        self.server.datagram_received(
            ("%s:1|c" % metric_name).encode('ascii'),
            ("127.0.0.1", 12345))
        self.stats.flush()

        r = self.stats.indexer.get_resource('generic',
                                            self.conf.statsd.resource_id,
                                            with_metrics=True)
        metric = r.get_metric(metric_key)
        self.assertIsNotNone(metric)

        self.trigger_processing([metric])

        measures = self.storage.get_measures(metric, self.aggregations)
        self.assertEqual({"mean": [
            (datetime64(2015, 1, 7), numpy.timedelta64(1, 'D'), 1.0),
            (datetime64(2015, 1, 7, 13), numpy.timedelta64(1, 'h'), 1.0),
            (datetime64(2015, 1, 7, 13, 58), numpy.timedelta64(1, 'm'), 1.0)
        ]}, measures)

        utcnow.return_value = utils.datetime_utc(2015, 1, 7, 13, 59, 37)
        self.server.datagram_received(
            ("%s:45|c" % metric_name).encode('ascii'),
            ("127.0.0.1", 12345))
        self.server.datagram_received(
            ("%s:2|c|@0.2" % metric_name).encode('ascii'),
            ("127.0.0.1", 12345))
        self.stats.flush()

        self.trigger_processing([metric])

        measures = self.storage.get_measures(metric, self.aggregations)
        self.assertEqual({"mean": [
            (datetime64(2015, 1, 7), numpy.timedelta64(1, 'D'), 28),
            (datetime64(2015, 1, 7, 13), numpy.timedelta64(1, 'h'), 28),
            (datetime64(2015, 1, 7, 13, 58), numpy.timedelta64(1, 'm'), 1.0),
            (datetime64(2015, 1, 7, 13, 59), numpy.timedelta64(1, 'm'), 55.0)
        ]}, measures)


class TestStatsdArchivePolicyRule(TestStatsd):
    STATSD_ARCHIVE_POLICY_NAME = ""

    def setUp(self):
        super(TestStatsdArchivePolicyRule, self).setUp()
        try:
            self.stats.indexer.create_archive_policy_rule(
                "statsd", "*", "medium")
        except indexer.ArchivePolicyRuleAlreadyExists:
            # Created by another test run
            pass
