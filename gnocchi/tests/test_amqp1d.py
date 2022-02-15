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

import numpy
from unittest import mock

from gnocchi import amqp1d
from gnocchi.tests import base as tests_base
from gnocchi.tests.test_utils import get_measures_list
from gnocchi import utils


def datetime64(*args):
    return numpy.datetime64(datetime.datetime(*args))


class TestAmqp1d(tests_base.TestCase):

    AMQP1D_USER_ID = str(uuid.uuid4())
    AMQP1D_PROJECT_ID = str(uuid.uuid4())

    def setUp(self):
        super(TestAmqp1d, self).setUp()
        self.conf.set_override("resource_type",
                               "collectd_amqp1d", "amqp1d")
        self.conf.set_override("creator",
                               self.AMQP1D_USER_ID, "amqp1d")

        self.index.create_archive_policy_rule("rule-amqp", "*", "medium")

        self.server = amqp1d.AMQP1Server(self.conf)
        self.server.processor.incoming = self.incoming
        self.server.processor.indexer = self.index

    @mock.patch.object(utils, 'utcnow')
    def test_amqp1d(self, utcnow):
        utcnow.return_value = utils.datetime_utc(2017, 1, 10, 13, 58, 36)

        metrics = json.dumps([
            {u'dstypes': [u'gauge'], u'plugin': u'memory', u'dsnames':
             [u'value'], u'interval': 10.0, u'host': u'www.gnocchi.test.com',
             u'values': [9], u'time': 1506712460.824, u'plugin_instance':
             u'', u'type_instance': u'free', u'type': u'memory'},
            {u'dstypes': [u'derive', u'derive'], u'plugin': u'interface',
             u'dsnames': [u'rx', u'tx'], u'interval': 10.0, u'host':
             u'www.gnocchi.test.com', u'values': [2, 5], u'time':
             1506712460.824, u'plugin_instance': u'ens2f1', u'type_instance':
             u'', u'type': u'if_errors'}
        ])

        self.server.on_message(mock.Mock(message=mock.Mock(body=metrics)))
        self.server.processor.flush()

        resources = self.index.list_resources(
            self.conf.amqp1d.resource_type,
            attribute_filter={"=": {"host": "www.gnocchi.test.com"}}
        )
        self.assertEqual(1, len(resources))
        self.assertEqual("www.gnocchi.test.com",
                         resources[0].host)

        metrics = self.index.list_metrics(attribute_filter={
            '=': {"resource_id": resources[0].id}
        })
        self.assertEqual(3, len(metrics))

        self.trigger_processing(metrics)

        expected_measures = {
            "memory@memory-free": [
                (datetime64(2017, 1, 10, 13, 58), numpy.timedelta64(1, 'm'), 9)
            ],
            "interface-ens2f1@if_errors-rx": [
                (datetime64(2017, 1, 10, 13, 58), numpy.timedelta64(1, 'm'), 2)
            ],
            "interface-ens2f1@if_errors-tx": [
                (datetime64(2017, 1, 10, 13, 58), numpy.timedelta64(1, 'm'), 5)
            ]
        }
        for metric in metrics:
            aggregation = metric.archive_policy.get_aggregation(
                "mean", numpy.timedelta64(1, 'm'))
            results = self.storage.get_aggregated_measures(
                {metric: [aggregation]})[metric]
            measures = get_measures_list(results)
            self.assertEqual(expected_measures[metric.name],
                             measures["mean"])
