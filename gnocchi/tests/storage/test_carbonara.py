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
import uuid

import mock
import pandas
import six

from gnocchi import carbonara
from gnocchi import storage
from gnocchi.storage import _carbonara
from gnocchi.tests import base as tests_base
from gnocchi import utils


def _to_dict_v1_3(self):
    d = {'values': dict((timestamp.value, float(v))
                        for timestamp, v
                        in six.iteritems(self.ts.dropna()))}
    sampling = pandas.tseries.offsets.Nano(self.sampling * 10e8)
    d.update({
        'aggregation_method': self.aggregation_method,
        'max_size': self.max_size,
        'sampling': six.text_type(sampling.n) + sampling.rule_code})
    return d


class TestCarbonaraMigration(tests_base.TestCase):
    def setUp(self):
        super(TestCarbonaraMigration, self).setUp()
        if not isinstance(self.storage, _carbonara.CarbonaraBasedStorage):
            self.skipTest("This driver is not based on Carbonara")

        self.metric = storage.Metric(uuid.uuid4(),
                                     self.archive_policies['low'])

        archive = carbonara.TimeSerieArchive.from_definitions(
            [(v.granularity, v.points)
             for v in self.metric.archive_policy.definition]
        )

        archive_max = carbonara.TimeSerieArchive.from_definitions(
            [(v.granularity, v.points)
             for v in self.metric.archive_policy.definition],
            aggregation_method='max',
        )

        for a in (archive, archive_max):
            a.update(carbonara.TimeSerie.from_data(
                [datetime.datetime(2014, 1, 1, 12, 0, 0),
                 datetime.datetime(2014, 1, 1, 12, 0, 4),
                 datetime.datetime(2014, 1, 1, 12, 0, 9)],
                [4, 5, 6]))

        self.storage._create_metric(self.metric)

        # serialise in old format
        with mock.patch('gnocchi.carbonara.AggregatedTimeSerie.to_dict',
                        autospec=True) as f:
            f.side_effect = _to_dict_v1_3

            self.storage._store_metric_archive(
                self.metric,
                archive.agg_timeseries[0].aggregation_method,
                archive.serialize())

            self.storage._store_metric_archive(
                self.metric,
                archive_max.agg_timeseries[0].aggregation_method,
                archive_max.serialize())

    def upgrade(self):
        with mock.patch.object(self.index, 'list_metrics') as f:
            f.return_value = [self.metric]
            self.storage.upgrade(self.index)

    def test_get_measures(self):
        # This is to make gordc safer
        self.assertIsNotNone(self.storage._get_metric_archive(
            self.metric, "mean"))

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

        self.assertRaises(
            storage.AggregationDoesNotExist,
            self.storage._get_metric_archive,
            self.metric, "mean")

    def test_delete_metric_not_upgraded(self):
        # Make sure that we delete everything (e.g. objects + container)
        # correctly even if the metric has not been upgraded.
        self.storage.delete_metric(self.metric)
        self.assertEqual([], self.storage.get_measures(self.metric))
