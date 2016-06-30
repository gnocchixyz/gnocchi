# -*- encoding: utf-8 -*-
#
# Copyright © 2015 Red Hat, Inc.
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
import itertools
import uuid

import mock
import msgpack
import six

from gnocchi import carbonara
from gnocchi import storage
from gnocchi.storage import _carbonara
from gnocchi.tests import base as tests_base
from gnocchi import utils


def _serialize_v2(self):
    d = {'values': dict((timestamp.value, float(v))
                        for timestamp, v
                        in six.iteritems(self.ts.dropna()))}
    return msgpack.dumps(d)


class TestCarbonaraMigration(tests_base.TestCase):
    def setUp(self):
        super(TestCarbonaraMigration, self).setUp()
        if not isinstance(self.storage, _carbonara.CarbonaraBasedStorage):
            self.skipTest("This driver is not based on Carbonara")

        self.metric = storage.Metric(uuid.uuid4(),
                                     self.archive_policies['low'])

        self.storage._create_metric(self.metric)

        # serialise in old format
        with mock.patch('gnocchi.carbonara.AggregatedTimeSerie.serialize',
                        autospec=True) as f:
            f.side_effect = _serialize_v2

            for d, agg in itertools.product(
                    self.metric.archive_policy.definition, ['mean', 'max']):
                ts = carbonara.AggregatedTimeSerie(
                    sampling=d.granularity, aggregation_method=agg,
                    max_size=d.points)

                ts.update(carbonara.TimeSerie.from_data(
                    [datetime.datetime(2014, 1, 1, 12, 0, 0),
                     datetime.datetime(2014, 1, 1, 12, 0, 4),
                     datetime.datetime(2014, 1, 1, 12, 0, 9)],
                    [4, 5, 6]))

                for key, split in ts.split():
                    self.storage._store_metric_measures(
                        self.metric, key, agg, d.granularity,
                        split.serialize(), version=None)

    def upgrade(self):
        with mock.patch.object(self.index, 'list_metrics') as f:
            f.return_value = [self.metric]
            self.storage.upgrade(self.index)

    def test_get_measures(self):
        with mock.patch.object(
                self.storage, '_get_measures_and_unserialize',
                side_effect=self.storage._get_measures_and_unserialize_v2):
            self.assertEqual([
                (utils.datetime_utc(2014, 1, 1), 86400, 5),
                (utils.datetime_utc(2014, 1, 1, 12), 3600, 5),
                (utils.datetime_utc(2014, 1, 1, 12), 300, 5)
            ], self.storage.get_measures(self.metric))

            self.assertEqual([
                (utils.datetime_utc(2014, 1, 1), 86400, 6),
                (utils.datetime_utc(2014, 1, 1, 12), 3600, 6),
                (utils.datetime_utc(2014, 1, 1, 12), 300, 6)
            ], self.storage.get_measures(self.metric, aggregation='max'))

        self.upgrade()

        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1), 86400, 5),
            (utils.datetime_utc(2014, 1, 1, 12), 3600, 5),
            (utils.datetime_utc(2014, 1, 1, 12), 300, 5)
        ], self.storage.get_measures(self.metric))

        self.assertEqual([
            (utils.datetime_utc(2014, 1, 1), 86400, 6),
            (utils.datetime_utc(2014, 1, 1, 12), 3600, 6),
            (utils.datetime_utc(2014, 1, 1, 12), 300, 6)
        ], self.storage.get_measures(self.metric, aggregation='max'))

        with mock.patch.object(
                self.storage, '_get_measures_and_unserialize',
                side_effect=self.storage._get_measures_and_unserialize_v2):
            self.assertRaises(
                storage.AggregationDoesNotExist,
                self.storage.get_measures, self.metric)

            self.assertRaises(
                storage.AggregationDoesNotExist,
                self.storage.get_measures, self.metric, aggregation='max')

    def test_delete_metric_not_upgraded(self):
        # Make sure that we delete everything (e.g. objects + container)
        # correctly even if the metric has not been upgraded.
        self.storage.delete_metric(self.metric)
        self.assertEqual([], self.storage.get_measures(self.metric))
