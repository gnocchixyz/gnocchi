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
import uuid

import numpy
from unittest import mock

from gnocchi import archive_policy
from gnocchi import carbonara
from gnocchi import incoming
from gnocchi import indexer
from gnocchi import storage
from gnocchi.storage import ceph
from gnocchi.storage import file
from gnocchi.storage import redis
from gnocchi.storage import s3
from gnocchi.storage import swift
from gnocchi.tests import base as tests_base
from gnocchi.tests.test_utils import get_measures_list


def datetime64(*args):
    return numpy.datetime64(datetime.datetime(*args))


class TestStorageDriver(tests_base.TestCase):
    def setUp(self):
        super(TestStorageDriver, self).setUp()
        # A lot of tests wants a metric, create one
        self.metric, __ = self._create_metric()

    def test_driver_str(self):
        driver = storage.get_driver(self.conf)

        if isinstance(driver, file.FileStorage):
            s = driver.basepath
        elif isinstance(driver, ceph.CephStorage):
            s = driver.rados.get_fsid()
        elif isinstance(driver, redis.RedisStorage):
            s = driver._client
        elif isinstance(driver, s3.S3Storage):
            s = driver._bucket_name
        elif isinstance(driver, swift.SwiftStorage):
            s = driver._container_prefix

        self.assertEqual(str(driver), "%s: %s" % (
                         driver.__class__.__name__, s))

    def test_get_driver(self):
        driver = storage.get_driver(self.conf)
        self.assertIsInstance(driver, storage.StorageDriver)

    def test_file_driver_subdir_len(self):
        driver = storage.get_driver(self.conf)
        if not isinstance(driver, file.FileStorage):
            self.skipTest("not file driver")

        # Check the default
        self.assertEqual(2, driver.SUBDIR_LEN)

        metric = mock.Mock(id=uuid.UUID("12345678901234567890123456789012"))
        expected = (driver.basepath + "/12/34/56/78/90/12/34/56/78/90/12/34/56"
                    "/78/90/12/12345678-9012-3456-7890-123456789012")
        self.assertEqual(expected, driver._build_metric_dir(metric))

        driver._file_subdir_len = 16
        expected = (driver.basepath + "/1234567890123456/7890123456"
                    "789012/12345678-9012-3456-7890-123456789012")
        self.assertEqual(expected, driver._build_metric_dir(metric))

        driver._file_subdir_len = 15
        expected = (driver.basepath + "/123456789012345/67890123456"
                    "7890/12/12345678-9012-3456-7890-123456789012")
        self.assertEqual(expected, driver._build_metric_dir(metric))

    def test_corrupted_split(self):
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2014, 1, 1, 12, 0, 1), 69),
        ])
        self.trigger_processing()

        aggregation = self.metric.archive_policy.get_aggregation(
            "mean", numpy.timedelta64(5, 'm'))

        with mock.patch('gnocchi.carbonara.AggregatedTimeSerie.unserialize',
                        side_effect=carbonara.InvalidData()):
            results = self.storage._get_splits_and_unserialize({
                self.metric: {
                    aggregation: [
                        carbonara.SplitKey(
                            numpy.datetime64(1387800000, 's'),
                            numpy.timedelta64(5, 'm'))
                    ],
                },
            })[self.metric][aggregation]
            self.assertEqual(1, len(results))
            self.assertIsInstance(results[0], carbonara.AggregatedTimeSerie)
            # Assert it's an empty one since corrupted
            self.assertEqual(0, len(results[0]))
            self.assertEqual(results[0].aggregation, aggregation)

    def test_get_splits_and_unserialize(self):
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2014, 1, 1, 12, 0, 1), 69),
        ])
        self.trigger_processing()

        aggregation = self.metric.archive_policy.get_aggregation(
            "mean", numpy.timedelta64(5, 'm'))

        results = self.storage._get_splits_and_unserialize({
            self.metric: {
                aggregation: [
                    carbonara.SplitKey(
                        numpy.datetime64(1387800000, 's'),
                        numpy.timedelta64(5, 'm')),
                ],
            },
        })[self.metric][aggregation]
        self.assertEqual(1, len(results))
        self.assertIsInstance(results[0], carbonara.AggregatedTimeSerie)
        # Assert it's not empty one since corrupted
        self.assertGreater(len(results[0]), 0)
        self.assertEqual(results[0].aggregation, aggregation)

    def test_corrupted_data(self):
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2014, 1, 1, 12, 0, 1), 69),
        ])
        self.trigger_processing()

        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2014, 1, 1, 13, 0, 1), 1),
        ])

        with mock.patch('gnocchi.carbonara.AggregatedTimeSerie.unserialize',
                        side_effect=carbonara.InvalidData()):
            with mock.patch('gnocchi.carbonara.BoundTimeSerie.unserialize',
                            side_effect=carbonara.InvalidData()):
                self.trigger_processing()

        m = self.storage.get_aggregated_measures(
            {self.metric:
                self.metric.archive_policy.get_aggregations_for_method(
                    'mean')},)[self.metric]
        m = get_measures_list(m)['mean']
        self.assertIn((datetime64(2014, 1, 1),
                       numpy.timedelta64(1, 'D'), 1), m)
        self.assertIn((datetime64(2014, 1, 1, 13),
                       numpy.timedelta64(1, 'h'), 1), m)
        self.assertIn((datetime64(2014, 1, 1, 13),
                       numpy.timedelta64(5, 'm'), 1), m)

    def test_aborted_initial_processing(self):
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2014, 1, 1, 12, 0, 1), 5),
        ])
        with mock.patch.object(self.storage, '_store_unaggregated_timeseries',
                               side_effect=Exception):
            try:
                self.trigger_processing()
            except Exception:
                pass

        with mock.patch('gnocchi.storage.LOG') as LOG:
            self.trigger_processing()
            self.assertFalse(LOG.error.called)

        aggregations = (
            self.metric.archive_policy.get_aggregations_for_method("mean")
        )

        m = self.storage.get_aggregated_measures(
            {self.metric: aggregations})[self.metric]
        m = get_measures_list(m)['mean']
        self.assertIn((datetime64(2014, 1, 1),
                       numpy.timedelta64(1, 'D'), 5.0), m)
        self.assertIn((datetime64(2014, 1, 1, 12),
                       numpy.timedelta64(1, 'h'), 5.0), m)
        self.assertIn((datetime64(2014, 1, 1, 12),
                       numpy.timedelta64(5, 'm'), 5.0), m)

    def test_delete_nonempty_metric(self):
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2014, 1, 1, 12, 0, 1), 69),
        ])
        self.trigger_processing()
        self.storage._delete_metric(self.metric)
        self.trigger_processing()

        aggregations = (
            self.metric.archive_policy.get_aggregations_for_method("mean")
        )

        self.assertRaises(storage.MetricDoesNotExist,
                          self.storage.get_aggregated_measures,
                          {self.metric: aggregations})
        self.assertEqual(
            {self.metric: None},
            self.storage._get_or_create_unaggregated_timeseries(
                [self.metric]))

    def test_measures_reporting_format(self):
        report = self.incoming.measures_report(True)
        self.assertIsInstance(report, dict)
        self.assertIn('summary', report)
        self.assertIn('metrics', report['summary'])
        self.assertIn('measures', report['summary'])
        self.assertIn('details', report)
        self.assertIsInstance(report['details'], dict)
        report = self.incoming.measures_report(False)
        self.assertIsInstance(report, dict)
        self.assertIn('summary', report)
        self.assertIn('metrics', report['summary'])
        self.assertIn('measures', report['summary'])
        self.assertNotIn('details', report)

    def test_measures_reporting(self):
        m2, __ = self._create_metric('medium')
        for i in range(60):
            self.incoming.add_measures(self.metric.id, [
                incoming.Measure(datetime64(2014, 1, 1, 12, 0, i), 69),
            ])
            self.incoming.add_measures(m2.id, [
                incoming.Measure(datetime64(2014, 1, 1, 12, 0, i), 69),
            ])
        report = self.incoming.measures_report(True)
        self.assertIsInstance(report, dict)
        self.assertEqual(2, report['summary']['metrics'])
        self.assertEqual(120, report['summary']['measures'])
        self.assertIn('details', report)
        self.assertIsInstance(report['details'], dict)
        report = self.incoming.measures_report(False)
        self.assertIsInstance(report, dict)
        self.assertEqual(2, report['summary']['metrics'])
        self.assertEqual(120, report['summary']['measures'])

    def test_get_aggregated_measures(self):
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2014, 1, 1, 12, i, j), 100)
            for i in range(0, 60) for j in range(0, 60)])
        self.trigger_processing([self.metric])

        aggregations = self.metric.archive_policy.aggregations

        measures = self.storage.get_aggregated_measures(
            {self.metric: aggregations})
        self.assertEqual(1, len(measures))
        self.assertIn(self.metric, measures)
        measures = measures[self.metric]
        self.assertEqual(len(aggregations), len(measures))
        self.assertGreater(len(measures[aggregations[0]]), 0)
        for agg in aggregations:
            self.assertEqual(agg, measures[agg].aggregation)

    def test_get_aggregated_measures_multiple(self):
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2014, 1, 1, 12, i, j), 100)
            for i in range(0, 60) for j in range(0, 60)])
        m2, __ = self._create_metric('medium')
        self.incoming.add_measures(m2.id, [
            incoming.Measure(datetime64(2014, 1, 1, 12, i, j), 100)
            for i in range(0, 60) for j in range(0, 60)])
        self.trigger_processing([self.metric, m2])

        aggregations = self.metric.archive_policy.aggregations

        measures = self.storage.get_aggregated_measures(
            {self.metric: aggregations,
             m2: m2.archive_policy.aggregations})

        self.assertEqual({self.metric, m2}, set(measures.keys()))
        self.assertEqual(len(aggregations), len(measures[self.metric]))
        self.assertGreater(len(measures[self.metric][aggregations[0]]), 0)
        for agg in aggregations:
            self.assertEqual(agg, measures[self.metric][agg].aggregation)
        self.assertEqual(len(m2.archive_policy.aggregations),
                         len(measures[m2]))
        self.assertGreater(
            len(measures[m2][m2.archive_policy.aggregations[0]]), 0)
        for agg in m2.archive_policy.aggregations:
            self.assertEqual(agg, measures[m2][agg].aggregation)

    def test_add_measures_big(self):
        m, __ = self._create_metric('high')
        self.incoming.add_measures(m.id, [
            incoming.Measure(datetime64(2014, 1, 1, 12, i, j), 100)
            for i in range(0, 60) for j in range(0, 60)])
        self.trigger_processing([m])

        aggregations = (
            m.archive_policy.get_aggregations_for_method("mean")
        )

        self.assertEqual(3661, len(
            get_measures_list(self.storage.get_aggregated_measures(
                {m: aggregations})[m])['mean']))

    @mock.patch('gnocchi.carbonara.SplitKey.POINTS_PER_SPLIT', 48)
    def test_add_measures_update_subset_split(self):
        m, m_sql = self._create_metric('medium')
        measures = [
            incoming.Measure(datetime64(2014, 1, 6, i, j, 0), 100)
            for i in range(2) for j in range(0, 60, 2)]
        self.incoming.add_measures(m.id, measures)
        self.trigger_processing([m])

        # add measure to end, in same aggregate time as last point.
        self.incoming.add_measures(m.id, [
            incoming.Measure(datetime64(2014, 1, 6, 1, 58, 1), 100)])

        with mock.patch.object(self.storage, '_store_metric_splits') as c:
            # should only resample last aggregate
            self.trigger_processing([m])
        count = 0
        for call in c.mock_calls:
            # policy is 60 points and split is 48. should only update 2nd half
            args = call[1]
            for metric, key_agg_data_offset in args[0].items():
                if metric.id == m_sql.id:
                    for key, aggregation, data, offset in key_agg_data_offset:
                        if (key.sampling == numpy.timedelta64(1, 'm')
                           and aggregation.method == "mean"):
                            count += 1
        self.assertEqual(1, count)

    def test_add_measures_update_subset(self):
        m, m_sql = self._create_metric('medium')
        measures = [
            incoming.Measure(datetime64(2014, 1, 6, i, j, 0), 100)
            for i in range(2) for j in range(0, 60, 2)]
        self.incoming.add_measures(m.id, measures)
        self.trigger_processing([m])

        # add measure to end, in same aggregate time as last point.
        new_point = datetime64(2014, 1, 6, 1, 58, 1)
        self.incoming.add_measures(m.id, [incoming.Measure(new_point, 100)])

        with mock.patch.object(self.incoming, 'add_measures') as c:
            self.trigger_processing([m])
        for __, args, __ in c.mock_calls:
            self.assertEqual(
                list(args[3])[0][0], carbonara.round_timestamp(
                    new_point, args[1].granularity * 10e8))

    def test_delete_old_measures(self):
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2014, 1, 1, 12, 0, 1), 69),
            incoming.Measure(datetime64(2014, 1, 1, 12, 7, 31), 42),
            incoming.Measure(datetime64(2014, 1, 1, 12, 9, 31), 4),
            incoming.Measure(datetime64(2014, 1, 1, 12, 12, 45), 44),
        ])
        self.trigger_processing()

        aggregations = (
            self.metric.archive_policy.get_aggregations_for_method("mean")
        )

        self.assertEqual({"mean": [
            (datetime64(2014, 1, 1), numpy.timedelta64(1, 'D'), 39.75),
            (datetime64(2014, 1, 1, 12), numpy.timedelta64(1, 'h'), 39.75),
            (datetime64(2014, 1, 1, 12), numpy.timedelta64(5, 'm'), 69.0),
            (datetime64(2014, 1, 1, 12, 5), numpy.timedelta64(5, 'm'), 23.0),
            (datetime64(2014, 1, 1, 12, 10), numpy.timedelta64(5, 'm'), 44.0),
        ]}, get_measures_list(self.storage.get_aggregated_measures(
            {self.metric: aggregations})[self.metric]))

        # One year later…
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2015, 1, 1, 12, 0, 1), 69),
        ])
        self.trigger_processing()

        self.assertEqual({"mean": [
            (datetime64(2015, 1, 1), numpy.timedelta64(1, 'D'), 69),
            (datetime64(2015, 1, 1, 12), numpy.timedelta64(1, 'h'), 69),
            (datetime64(2015, 1, 1, 12), numpy.timedelta64(5, 'm'), 69),
        ]}, get_measures_list(self.storage.get_aggregated_measures(
            {self.metric: aggregations})[self.metric]))

        agg = self.metric.archive_policy.get_aggregation(
            "mean", numpy.timedelta64(1, 'D'))
        self.assertEqual({
            self.metric: {
                agg: {carbonara.SplitKey(numpy.datetime64(1244160000, 's'),
                                         numpy.timedelta64(1, 'D'))},
            },
        }, self.storage._list_split_keys({self.metric: [agg]}))
        agg = self.metric.archive_policy.get_aggregation(
            "mean", numpy.timedelta64(1, 'h'))
        self.assertEqual({
            self.metric: {
                agg: {carbonara.SplitKey(numpy.datetime64(1412640000, 's'),
                                         numpy.timedelta64(1, 'h'))},
            },
        }, self.storage._list_split_keys({self.metric: [agg]}))
        agg = self.metric.archive_policy.get_aggregation(
            "mean", numpy.timedelta64(5, 'm'))
        self.assertEqual({
            self.metric: {
                agg: {carbonara.SplitKey(numpy.datetime64(1419120000, 's'),
                                         numpy.timedelta64(5, 'm'))},
            }
        }, self.storage._list_split_keys({self.metric: [agg]}))

    def test_get_measures_return(self):
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2016, 1, 1, 12, 0, 1), 69),
            incoming.Measure(datetime64(2016, 1, 2, 13, 7, 31), 42),
            incoming.Measure(datetime64(2016, 1, 4, 14, 9, 31), 4),
            incoming.Measure(datetime64(2016, 1, 6, 15, 12, 45), 44),
        ])
        self.trigger_processing()

        aggregation = self.metric.archive_policy.get_aggregation(
            "mean", numpy.timedelta64(5, 'm'))

        data = self.storage._get_splits({
            self.metric: {
                aggregation: [
                    carbonara.SplitKey(
                        numpy.datetime64(1451520000, 's'),
                        numpy.timedelta64(5, 'm'),
                    )]}})
        self.assertEqual(1, len(data))
        data = data[self.metric]
        self.assertEqual(1, len(data))
        data = data[aggregation]
        self.assertEqual(1, len(data))
        self.assertIsInstance(data[0], bytes)
        self.assertGreater(len(data[0]), 0)
        existing = data[0]

        # Now retrieve an existing and a non-existing key
        data = self.storage._get_splits({
            self.metric: {
                aggregation: [
                    carbonara.SplitKey(
                        numpy.datetime64(1451520000, 's'),
                        numpy.timedelta64(5, 'm'),
                    ),
                    carbonara.SplitKey(
                        numpy.datetime64(1451520010, 's'),
                        numpy.timedelta64(5, 'm'),
                    ),
                ]}})
        self.assertEqual(1, len(data))
        data = data[self.metric]
        self.assertEqual(1, len(data))
        data = data[aggregation]
        self.assertEqual(2, len(data))
        self.assertIsInstance(data[0], bytes)
        self.assertGreater(len(data[0]), 0)
        self.assertEqual(existing, data[0])
        self.assertIsNone(data[1])

        # Now retrieve a non-existing and an existing key
        data = self.storage._get_splits({
            self.metric: {
                aggregation: [
                    carbonara.SplitKey(
                        numpy.datetime64(155152000, 's'),
                        numpy.timedelta64(5, 'm'),
                    ),
                    carbonara.SplitKey(
                        numpy.datetime64(1451520000, 's'),
                        numpy.timedelta64(5, 'm'),
                    )
                ]}})
        self.assertEqual(1, len(data))
        data = data[self.metric]
        self.assertEqual(1, len(data))
        data = data[aggregation]
        self.assertEqual(2, len(data))
        self.assertIsInstance(data[1], bytes)
        self.assertGreater(len(data[1]), 0)
        self.assertEqual(existing, data[1])
        self.assertIsNone(data[0])

        m2, _ = self._create_metric()
        # Now retrieve a non-existing (= no aggregated measures) metric
        data = self.storage._get_splits({
            m2: {
                aggregation: [
                    carbonara.SplitKey(
                        numpy.datetime64(1451520010, 's'),
                        numpy.timedelta64(5, 'm'),
                    ),
                    carbonara.SplitKey(
                        numpy.datetime64(1451520000, 's'),
                        numpy.timedelta64(5, 'm'),
                    )
                ]}})
        self.assertEqual({m2: {aggregation: [None, None]}}, data)

    def test_rewrite_measures(self):
        # Create an archive policy that spans on several splits. Each split
        # being 3600 points, let's go for 36k points so we have 10 splits.
        apname = str(uuid.uuid4())
        ap = archive_policy.ArchivePolicy(apname, 0, [(36000, 60)])
        self.index.create_archive_policy(ap)
        self.metric = indexer.Metric(uuid.uuid4(), ap)
        self.index.create_metric(self.metric.id, str(uuid.uuid4()),
                                 apname)

        # First store some points scattered across different splits
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2016, 1, 1, 12, 0, 1), 69),
            incoming.Measure(datetime64(2016, 1, 2, 13, 7, 31), 42),
            incoming.Measure(datetime64(2016, 1, 4, 14, 9, 31), 4),
            incoming.Measure(datetime64(2016, 1, 6, 15, 12, 45), 44),
        ])
        self.trigger_processing()

        agg = self.metric.archive_policy.get_aggregation(
            "mean", numpy.timedelta64(1, 'm'))
        self.assertEqual({
            self.metric: {
                agg: {
                    carbonara.SplitKey(numpy.datetime64(1451520000, 's'),
                                       numpy.timedelta64(1, 'm')),
                    carbonara.SplitKey(numpy.datetime64(1451736000, 's'),
                                       numpy.timedelta64(1, 'm')),
                    carbonara.SplitKey(numpy.datetime64(1451952000, 's'),
                                       numpy.timedelta64(1, 'm')),
                },
            }
        }, self.storage._list_split_keys({self.metric: [agg]}))

        if self.storage.WRITE_FULL:
            assertCompressedIfWriteFull = self.assertTrue
        else:
            assertCompressedIfWriteFull = self.assertFalse

        aggregation = self.metric.archive_policy.get_aggregation(
            "mean", numpy.timedelta64(1, 'm'))

        data = self.storage._get_splits({
            self.metric: {
                aggregation: [carbonara.SplitKey(
                    numpy.datetime64(1451520000, 's'),
                    numpy.timedelta64(1, 'm'),
                )]}})[self.metric][aggregation][0]
        self.assertTrue(carbonara.AggregatedTimeSerie.is_compressed(data))
        data = self.storage._get_splits({
            self.metric: {
                aggregation: [carbonara.SplitKey(
                    numpy.datetime64(1451736000, 's'),
                    numpy.timedelta64(60, 's'),
                )]}})[self.metric][aggregation][0]
        self.assertTrue(carbonara.AggregatedTimeSerie.is_compressed(data))
        data = self.storage._get_splits({
            self.metric: {
                aggregation: [carbonara.SplitKey(
                    numpy.datetime64(1451952000, 's'),
                    numpy.timedelta64(60, 's'),
                )]}})[self.metric][aggregation][0]
        assertCompressedIfWriteFull(
            carbonara.AggregatedTimeSerie.is_compressed(data))

        self.assertEqual({"mean": [
            (datetime64(2016, 1, 1, 12), numpy.timedelta64(1, 'm'), 69),
            (datetime64(2016, 1, 2, 13, 7), numpy.timedelta64(1, 'm'), 42),
            (datetime64(2016, 1, 4, 14, 9), numpy.timedelta64(1, 'm'), 4),
            (datetime64(2016, 1, 6, 15, 12), numpy.timedelta64(1, 'm'), 44),
        ]}, get_measures_list(self.storage.get_aggregated_measures(
            {self.metric: [aggregation]})[self.metric]))

        # Now store brand new points that should force a rewrite of one of the
        # split (keep in mind the back window size in one hour here). We move
        # the BoundTimeSerie processing timeserie far away from its current
        # range.
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2016, 1, 10, 16, 18, 45), 45),
            incoming.Measure(datetime64(2016, 1, 10, 17, 12, 45), 46),
        ])
        self.trigger_processing()

        agg = self.metric.archive_policy.get_aggregation(
            "mean", numpy.timedelta64(1, 'm'))
        self.assertEqual({
            self.metric: {
                agg: {
                    carbonara.SplitKey(numpy.datetime64(1452384000, 's'),
                                       numpy.timedelta64(1, 'm')),
                    carbonara.SplitKey(numpy.datetime64(1451736000, 's'),
                                       numpy.timedelta64(1, 'm')),
                    carbonara.SplitKey(numpy.datetime64(1451520000, 's'),
                                       numpy.timedelta64(1, 'm')),
                    carbonara.SplitKey(numpy.datetime64(1451952000, 's'),
                                       numpy.timedelta64(1, 'm')),
                },
            },
        }, self.storage._list_split_keys({self.metric: [agg]}))
        data = self.storage._get_splits({
            self.metric: {
                aggregation: [carbonara.SplitKey(
                    numpy.datetime64(1451520000, 's'),
                    numpy.timedelta64(60, 's'),
                )]}})[self.metric][aggregation][0]
        self.assertTrue(carbonara.AggregatedTimeSerie.is_compressed(data))
        data = self.storage._get_splits({
            self.metric: {
                aggregation: [carbonara.SplitKey(
                    numpy.datetime64(1451736000, 's'),
                    numpy.timedelta64(60, 's'),
                )]}})[self.metric][aggregation][0]
        self.assertTrue(carbonara.AggregatedTimeSerie.is_compressed(data))
        data = self.storage._get_splits({
            self.metric: {
                aggregation: [carbonara.SplitKey(
                    numpy.datetime64(1451952000, 's'),
                    numpy.timedelta64(1, 'm'),
                )]}})[self.metric][aggregation][0]
        # Now this one is compressed because it has been rewritten!
        self.assertTrue(carbonara.AggregatedTimeSerie.is_compressed(data))
        data = self.storage._get_splits({
            self.metric: {
                aggregation: [
                    carbonara.SplitKey(
                        numpy.datetime64(1452384000, 's'),
                        numpy.timedelta64(60, 's'),
                    )]}})[self.metric][aggregation][0]
        assertCompressedIfWriteFull(
            carbonara.AggregatedTimeSerie.is_compressed(data))

        self.assertEqual({"mean": [
            (datetime64(2016, 1, 1, 12), numpy.timedelta64(1, 'm'), 69),
            (datetime64(2016, 1, 2, 13, 7), numpy.timedelta64(1, 'm'), 42),
            (datetime64(2016, 1, 4, 14, 9), numpy.timedelta64(1, 'm'), 4),
            (datetime64(2016, 1, 6, 15, 12), numpy.timedelta64(1, 'm'), 44),
            (datetime64(2016, 1, 10, 16, 18), numpy.timedelta64(1, 'm'), 45),
            (datetime64(2016, 1, 10, 17, 12), numpy.timedelta64(1, 'm'), 46),
        ]}, get_measures_list(self.storage.get_aggregated_measures(
            {self.metric: [aggregation]})[self.metric]))

    def test_rewrite_measures_multiple_granularities(self):
        apname = str(uuid.uuid4())
        # Create an archive policy with two different granularities
        ap = archive_policy.ArchivePolicy(apname, 0, [(36000, 60), (36000, 1)])
        self.index.create_archive_policy(ap)
        self.metric = indexer.Metric(uuid.uuid4(), ap)
        self.index.create_metric(self.metric.id, str(uuid.uuid4()),
                                 apname)

        # First store some points
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2016, 1, 6, 18, 15, 46), 43),
            incoming.Measure(datetime64(2016, 1, 6, 18, 15, 47), 43),
            incoming.Measure(datetime64(2016, 1, 6, 18, 15, 48), 43),
        ])
        self.trigger_processing()

        # Add some more points, mocking out WRITE_FULL attribute of the current
        # driver, so that rewrite happens
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2016, 1, 7, 18, 15, 49), 43),
            incoming.Measure(datetime64(2016, 1, 7, 18, 15, 50), 43),
            incoming.Measure(datetime64(2016, 1, 7, 18, 18, 46), 43),
        ])
        driver = storage.get_driver(self.conf)
        with mock.patch.object(driver.__class__, 'WRITE_FULL', False):
            self.trigger_processing()

    def test_rewrite_measures_oldest_mutable_timestamp_eq_next_key(self):
        """See LP#1655422"""
        # Create an archive policy that spans on several splits. Each split
        # being 3600 points, let's go for 36k points so we have 10 splits.
        apname = str(uuid.uuid4())
        ap = archive_policy.ArchivePolicy(apname, 0, [(36000, 60)])
        self.index.create_archive_policy(ap)
        self.metric = indexer.Metric(uuid.uuid4(), ap)
        self.index.create_metric(self.metric.id, str(uuid.uuid4()),
                                 apname)

        # First store some points scattered across different splits
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2016, 1, 1, 12, 0, 1), 69),
            incoming.Measure(datetime64(2016, 1, 2, 13, 7, 31), 42),
            incoming.Measure(datetime64(2016, 1, 4, 14, 9, 31), 4),
            incoming.Measure(datetime64(2016, 1, 6, 15, 12, 45), 44),
        ])
        self.trigger_processing()

        agg = self.metric.archive_policy.get_aggregation(
            "mean", numpy.timedelta64(1, 'm'))
        self.assertEqual({
            self.metric: {
                agg: {
                    carbonara.SplitKey(numpy.datetime64(1451520000, 's'),
                                       numpy.timedelta64(1, 'm')),
                    carbonara.SplitKey(numpy.datetime64(1451736000, 's'),
                                       numpy.timedelta64(1, 'm')),
                    carbonara.SplitKey(numpy.datetime64(1451952000, 's'),
                                       numpy.timedelta64(1, 'm')),
                },
            },
        }, self.storage._list_split_keys({self.metric: [agg]}))

        if self.storage.WRITE_FULL:
            assertCompressedIfWriteFull = self.assertTrue
        else:
            assertCompressedIfWriteFull = self.assertFalse

        aggregation = self.metric.archive_policy.get_aggregation(
            "mean", numpy.timedelta64(1, 'm'))

        data = self.storage._get_splits(
            {self.metric: {
                aggregation: [carbonara.SplitKey(
                    numpy.datetime64(1451520000, 's'),
                    numpy.timedelta64(1, 'm'),
                )]}})[self.metric][aggregation][0]
        self.assertTrue(carbonara.AggregatedTimeSerie.is_compressed(data))
        data = self.storage._get_splits(
            {self.metric: {
                aggregation: [carbonara.SplitKey(
                    numpy.datetime64(1451736000, 's'),
                    numpy.timedelta64(1, 'm'),
                )]}})[self.metric][aggregation][0]
        self.assertTrue(carbonara.AggregatedTimeSerie.is_compressed(data))
        data = self.storage._get_splits(
            {self.metric: {aggregation: [carbonara.SplitKey(
                numpy.datetime64(1451952000, 's'),
                numpy.timedelta64(1, 'm')
            )]}})[self.metric][aggregation][0]
        assertCompressedIfWriteFull(
            carbonara.AggregatedTimeSerie.is_compressed(data))

        self.assertEqual({"mean": [
            (datetime64(2016, 1, 1, 12), numpy.timedelta64(1, 'm'), 69),
            (datetime64(2016, 1, 2, 13, 7), numpy.timedelta64(1, 'm'), 42),
            (datetime64(2016, 1, 4, 14, 9), numpy.timedelta64(1, 'm'), 4),
            (datetime64(2016, 1, 6, 15, 12), numpy.timedelta64(1, 'm'), 44),
        ]}, get_measures_list(self.storage.get_aggregated_measures(
            {self.metric: [aggregation]})[self.metric]))

        # Now store brand new points that should force a rewrite of one of the
        # split (keep in mind the back window size is one hour here). We move
        # the BoundTimeSerie processing timeserie far away from its current
        # range.

        # Here we test a special case where the oldest_mutable_timestamp will
        # be 2016-01-10T00:00:00 = 1452384000.0, our new split key.
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2016, 1, 10, 0, 12), 45),
        ])
        self.trigger_processing()

        agg = self.metric.archive_policy.get_aggregation(
            "mean", numpy.timedelta64(1, 'm'))
        self.assertEqual({
            self.metric: {
                agg: {
                    carbonara.SplitKey(numpy.datetime64('2016-01-10T00:00:00'),
                                       numpy.timedelta64(1, 'm')),
                    carbonara.SplitKey(numpy.datetime64('2016-01-02T12:00:00'),
                                       numpy.timedelta64(1, 'm')),
                    carbonara.SplitKey(numpy.datetime64('2015-12-31T00:00:00'),
                                       numpy.timedelta64(1, 'm')),
                    carbonara.SplitKey(numpy.datetime64('2016-01-05T00:00:00'),
                                       numpy.timedelta64(1, 'm')),
                },
            },
        }, self.storage._list_split_keys({self.metric: [agg]}))
        data = self.storage._get_splits({
            self.metric: {
                agg: [carbonara.SplitKey(
                    numpy.datetime64(1451520000, 's'),
                    numpy.timedelta64(1, 'm'),
                )]}})[self.metric][agg][0]
        self.assertTrue(carbonara.AggregatedTimeSerie.is_compressed(data))
        data = self.storage._get_splits({
            self.metric: {
                agg: [carbonara.SplitKey(
                    numpy.datetime64(1451736000, 's'),
                    numpy.timedelta64(1, 'm'),
                )]}})[self.metric][agg][0]
        self.assertTrue(carbonara.AggregatedTimeSerie.is_compressed(data))
        data = self.storage._get_splits({
            self.metric: {
                agg: [carbonara.SplitKey(
                    numpy.datetime64(1451952000, 's'),
                    numpy.timedelta64(60, 's')
                )]}})[self.metric][agg][0]
        # Now this one is compressed because it has been rewritten!
        self.assertTrue(carbonara.AggregatedTimeSerie.is_compressed(data))
        data = self.storage._get_splits({
            self.metric: {
                agg: [carbonara.SplitKey(
                    numpy.datetime64(1452384000, 's'),
                    numpy.timedelta64(1, 'm'),
                )]}})[self.metric][agg][0]
        assertCompressedIfWriteFull(
            carbonara.AggregatedTimeSerie.is_compressed(data))

        self.assertEqual({"mean": [
            (datetime64(2016, 1, 1, 12), numpy.timedelta64(1, 'm'), 69),
            (datetime64(2016, 1, 2, 13, 7), numpy.timedelta64(1, 'm'), 42),
            (datetime64(2016, 1, 4, 14, 9), numpy.timedelta64(1, 'm'), 4),
            (datetime64(2016, 1, 6, 15, 12), numpy.timedelta64(1, 'm'), 44),
            (datetime64(2016, 1, 10, 0, 12), numpy.timedelta64(1, 'm'), 45),
        ]}, get_measures_list(self.storage.get_aggregated_measures(
            {self.metric: [aggregation]})[self.metric]))

    def test_rewrite_measures_corruption_missing_file(self):
        # Create an archive policy that spans on several splits. Each split
        # being 3600 points, let's go for 36k points so we have 10 splits.
        apname = str(uuid.uuid4())
        ap = archive_policy.ArchivePolicy(apname, 0, [(36000, 60)])
        self.index.create_archive_policy(ap)
        self.metric = indexer.Metric(uuid.uuid4(), ap)
        self.index.create_metric(self.metric.id, str(uuid.uuid4()),
                                 apname)

        # First store some points scattered across different splits
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2016, 1, 1, 12, 0, 1), 69),
            incoming.Measure(datetime64(2016, 1, 2, 13, 7, 31), 42),
            incoming.Measure(datetime64(2016, 1, 4, 14, 9, 31), 4),
            incoming.Measure(datetime64(2016, 1, 6, 15, 12, 45), 44),
        ])
        self.trigger_processing()

        agg = self.metric.archive_policy.get_aggregation(
            "mean", numpy.timedelta64(1, 'm'))
        self.assertEqual({
            self.metric: {
                agg: {
                    carbonara.SplitKey(numpy.datetime64('2015-12-31T00:00:00'),
                                       numpy.timedelta64(1, 'm')),
                    carbonara.SplitKey(numpy.datetime64('2016-01-02T12:00:00'),
                                       numpy.timedelta64(1, 'm')),
                    carbonara.SplitKey(numpy.datetime64('2016-01-05T00:00:00'),
                                       numpy.timedelta64(1, 'm')),
                },
            },
        }, self.storage._list_split_keys({self.metric: [agg]}))
        if self.storage.WRITE_FULL:
            assertCompressedIfWriteFull = self.assertTrue
        else:
            assertCompressedIfWriteFull = self.assertFalse

        aggregation = self.metric.archive_policy.get_aggregation(
            "mean", numpy.timedelta64(1, 'm'))

        data = self.storage._get_splits({
            self.metric: {
                aggregation:
                [carbonara.SplitKey(
                    numpy.datetime64(1451520000, 's'),
                    numpy.timedelta64(1, 'm'),
                )]}})[self.metric][aggregation][0]
        self.assertTrue(carbonara.AggregatedTimeSerie.is_compressed(data))
        data = self.storage._get_splits({
            self.metric: {
                aggregation: [carbonara.SplitKey(
                    numpy.datetime64(1451736000, 's'),
                    numpy.timedelta64(1, 'm')
                )]}})[self.metric][aggregation][0]
        self.assertTrue(carbonara.AggregatedTimeSerie.is_compressed(data))
        data = self.storage._get_splits({
            self.metric: {
                aggregation: [carbonara.SplitKey(
                    numpy.datetime64(1451952000, 's'),
                    numpy.timedelta64(1, 'm'),
                )]}})[self.metric][aggregation][0]
        assertCompressedIfWriteFull(
            carbonara.AggregatedTimeSerie.is_compressed(data))

        self.assertEqual({"mean": [
            (datetime64(2016, 1, 1, 12),
             numpy.timedelta64(1, 'm'), 69),
            (datetime64(2016, 1, 2, 13, 7),
             numpy.timedelta64(1, 'm'), 42),
            (datetime64(2016, 1, 4, 14, 9),
             numpy.timedelta64(1, 'm'), 4),
            (datetime64(2016, 1, 6, 15, 12),
             numpy.timedelta64(1, 'm'), 44),
        ]}, get_measures_list(self.storage.get_aggregated_measures(
            {self.metric: [aggregation]})[self.metric]))

        # Test what happens if we delete the latest split and then need to
        # compress it!
        self.storage._delete_metric_splits(
            {self.metric: [(carbonara.SplitKey(
                numpy.datetime64(1451952000, 's'),
                numpy.timedelta64(1, 'm'),
            ), aggregation)]})

        # Now store brand new points that should force a rewrite of one of the
        # split (keep in mind the back window size in one hour here). We move
        # the BoundTimeSerie processing timeserie far away from its current
        # range.
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2016, 1, 10, 16, 18, 45), 45),
            incoming.Measure(datetime64(2016, 1, 10, 17, 12, 45), 46),
        ])
        self.trigger_processing()

    def test_rewrite_measures_corruption_bad_data(self):
        # Create an archive policy that spans on several splits. Each split
        # being 3600 points, let's go for 36k points so we have 10 splits.
        apname = str(uuid.uuid4())
        ap = archive_policy.ArchivePolicy(apname, 0, [(36000, 60)])
        self.index.create_archive_policy(ap)
        self.metric = indexer.Metric(uuid.uuid4(), ap)
        self.index.create_metric(self.metric.id, str(uuid.uuid4()),
                                 apname)

        # First store some points scattered across different splits
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2016, 1, 1, 12, 0, 1), 69),
            incoming.Measure(datetime64(2016, 1, 2, 13, 7, 31), 42),
            incoming.Measure(datetime64(2016, 1, 4, 14, 9, 31), 4),
            incoming.Measure(datetime64(2016, 1, 6, 15, 12, 45), 44),
        ])
        self.trigger_processing()

        agg = self.metric.archive_policy.get_aggregation(
            "mean", numpy.timedelta64(1, 'm'))
        self.assertEqual({
            self.metric: {
                agg: {
                    carbonara.SplitKey(numpy.datetime64(1451520000, 's'),
                                       numpy.timedelta64(1, 'm')),
                    carbonara.SplitKey(numpy.datetime64(1451736000, 's'),
                                       numpy.timedelta64(1, 'm')),
                    carbonara.SplitKey(numpy.datetime64(1451952000, 's'),
                                       numpy.timedelta64(1, 'm')),
                },
            },
        }, self.storage._list_split_keys({self.metric: [agg]}))

        if self.storage.WRITE_FULL:
            assertCompressedIfWriteFull = self.assertTrue
        else:
            assertCompressedIfWriteFull = self.assertFalse

        aggregation = self.metric.archive_policy.get_aggregation(
            "mean", numpy.timedelta64(1, 'm'))

        data = self.storage._get_splits({
            self.metric: {
                aggregation: [carbonara.SplitKey(
                    numpy.datetime64(1451520000, 's'),
                    numpy.timedelta64(60, 's'),
                )]}})[self.metric][aggregation][0]
        self.assertTrue(carbonara.AggregatedTimeSerie.is_compressed(data))
        data = self.storage._get_splits({
            self.metric: {
                aggregation: [carbonara.SplitKey(
                    numpy.datetime64(1451736000, 's'),
                    numpy.timedelta64(1, 'm'),
                )]}})[self.metric][aggregation][0]
        self.assertTrue(carbonara.AggregatedTimeSerie.is_compressed(data))
        data = self.storage._get_splits({
            self.metric: {
                aggregation: [carbonara.SplitKey(
                    numpy.datetime64(1451952000, 's'),
                    numpy.timedelta64(1, 'm'),
                )]}})[self.metric][aggregation][0]
        assertCompressedIfWriteFull(
            carbonara.AggregatedTimeSerie.is_compressed(data))

        self.assertEqual({"mean": [
            (datetime64(2016, 1, 1, 12), numpy.timedelta64(1, 'm'), 69),
            (datetime64(2016, 1, 2, 13, 7), numpy.timedelta64(1, 'm'), 42),
            (datetime64(2016, 1, 4, 14, 9), numpy.timedelta64(1, 'm'), 4),
            (datetime64(2016, 1, 6, 15, 12), numpy.timedelta64(1, 'm'), 44),
        ]}, get_measures_list(self.storage.get_aggregated_measures(
            {self.metric: [aggregation]})[self.metric]))

        # Test what happens if we write garbage
        self.storage._store_metric_splits({
            self.metric: [
                (carbonara.SplitKey(
                    numpy.datetime64(1451952000, 's'),
                    numpy.timedelta64(1, 'm')),
                 aggregation, b"oh really?", None),
            ]})

        # Now store brand new points that should force a rewrite of one of the
        # split (keep in mind the back window size in one hour here). We move
        # the BoundTimeSerie processing timeserie far away from its current
        # range.
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2016, 1, 10, 16, 18, 45), 45),
            incoming.Measure(datetime64(2016, 1, 10, 17, 12, 45), 46),
        ])
        self.trigger_processing()

    def test_updated_measures(self):
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2014, 1, 1, 12, 0, 1), 69),
            incoming.Measure(datetime64(2014, 1, 1, 12, 7, 31), 42),
        ])
        self.trigger_processing()

        aggregations = (
            self.metric.archive_policy.get_aggregations_for_method("mean")
        )

        self.assertEqual({"mean": [
            (datetime64(2014, 1, 1), numpy.timedelta64(1, 'D'), 55.5),
            (datetime64(2014, 1, 1, 12), numpy.timedelta64(1, 'h'), 55.5),
            (datetime64(2014, 1, 1, 12), numpy.timedelta64(5, 'm'), 69),
            (datetime64(2014, 1, 1, 12, 5), numpy.timedelta64(5, 'm'), 42.0),
        ]}, get_measures_list(self.storage.get_aggregated_measures(
            {self.metric: aggregations})[self.metric]))

        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2014, 1, 1, 12, 9, 31), 4),
            incoming.Measure(datetime64(2014, 1, 1, 12, 12, 45), 44),
        ])
        self.trigger_processing()

        self.assertEqual({"mean": [
            (datetime64(2014, 1, 1), numpy.timedelta64(1, 'D'), 39.75),
            (datetime64(2014, 1, 1, 12), numpy.timedelta64(1, 'h'), 39.75),
            (datetime64(2014, 1, 1, 12), numpy.timedelta64(5, 'm'), 69.0),
            (datetime64(2014, 1, 1, 12, 5), numpy.timedelta64(5, 'm'), 23.0),
            (datetime64(2014, 1, 1, 12, 10), numpy.timedelta64(5, 'm'), 44.0),
        ]}, get_measures_list(self.storage.get_aggregated_measures(
            {self.metric: aggregations})[self.metric]))

        aggregations = (
            self.metric.archive_policy.get_aggregations_for_method("max")
        )

        self.assertEqual({"max": [
            (datetime64(2014, 1, 1), numpy.timedelta64(1, 'D'), 69),
            (datetime64(2014, 1, 1, 12), numpy.timedelta64(1, 'h'), 69.0),
            (datetime64(2014, 1, 1, 12), numpy.timedelta64(5, 'm'), 69.0),
            (datetime64(2014, 1, 1, 12, 5), numpy.timedelta64(5, 'm'), 42.0),
            (datetime64(2014, 1, 1, 12, 10), numpy.timedelta64(5, 'm'), 44.0),
        ]}, get_measures_list(self.storage.get_aggregated_measures(
            {self.metric: aggregations})[self.metric]))

        aggregations = (
            self.metric.archive_policy.get_aggregations_for_method("min")
        )

        self.assertEqual({"min": [
            (datetime64(2014, 1, 1), numpy.timedelta64(1, 'D'), 4),
            (datetime64(2014, 1, 1, 12), numpy.timedelta64(1, 'h'), 4),
            (datetime64(2014, 1, 1, 12), numpy.timedelta64(5, 'm'), 69.0),
            (datetime64(2014, 1, 1, 12, 5), numpy.timedelta64(5, 'm'), 4.0),
            (datetime64(2014, 1, 1, 12, 10), numpy.timedelta64(5, 'm'), 44.0),
        ]}, get_measures_list(self.storage.get_aggregated_measures(
            {self.metric: aggregations})[self.metric]))

    def test_add_and_get_splits(self):
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2014, 1, 1, 12, 0, 1), 69),
            incoming.Measure(datetime64(2014, 1, 1, 12, 7, 31), 42),
            incoming.Measure(datetime64(2014, 1, 1, 12, 9, 31), 4),
            incoming.Measure(datetime64(2014, 1, 1, 12, 12, 45), 44),
        ])
        self.trigger_processing()

        aggregations = (
            self.metric.archive_policy.get_aggregations_for_method("mean")
        )

        self.assertEqual({"mean": [
            (datetime64(2014, 1, 1), numpy.timedelta64(1, 'D'), 39.75),
            (datetime64(2014, 1, 1, 12), numpy.timedelta64(1, 'h'), 39.75),
            (datetime64(2014, 1, 1, 12), numpy.timedelta64(5, 'm'), 69.0),
            (datetime64(2014, 1, 1, 12, 5), numpy.timedelta64(5, 'm'), 23.0),
            (datetime64(2014, 1, 1, 12, 10), numpy.timedelta64(5, 'm'), 44.0),
        ]}, get_measures_list(self.storage.get_aggregated_measures(
            {self.metric: aggregations})[self.metric]))

        self.assertEqual({"mean": [
            (datetime64(2014, 1, 1), numpy.timedelta64(1, 'D'), 39.75),
            (datetime64(2014, 1, 1, 12), numpy.timedelta64(1, 'h'), 39.75),
            (datetime64(2014, 1, 1, 12, 10), numpy.timedelta64(5, 'm'), 44.0),
        ]}, get_measures_list(self.storage.get_aggregated_measures(
            {self.metric: aggregations},
            from_timestamp=datetime64(2014, 1, 1, 12, 10, 0))[self.metric]))

        self.assertEqual({"mean": [
            (datetime64(2014, 1, 1), numpy.timedelta64(1, 'D'), 39.75),
            (datetime64(2014, 1, 1, 12), numpy.timedelta64(1, 'h'), 39.75),
            (datetime64(2014, 1, 1, 12), numpy.timedelta64(5, 'm'), 69.0),
            (datetime64(2014, 1, 1, 12, 5), numpy.timedelta64(5, 'm'), 23.0),
        ]}, get_measures_list(self.storage.get_aggregated_measures(
            {self.metric: aggregations},
            to_timestamp=datetime64(2014, 1, 1, 12, 6, 0))[self.metric]))

        self.assertEqual({"mean": [
            (datetime64(2014, 1, 1), numpy.timedelta64(1, 'D'), 39.75),
            (datetime64(2014, 1, 1, 12), numpy.timedelta64(1, 'h'), 39.75),
            (datetime64(2014, 1, 1, 12, 10), numpy.timedelta64(5, 'm'), 44.0),
        ]}, get_measures_list(self.storage.get_aggregated_measures(
            {self.metric: aggregations},
            to_timestamp=datetime64(2014, 1, 1, 12, 10, 10),
            from_timestamp=datetime64(2014, 1, 1, 12, 10, 10))[self.metric]))

        self.assertEqual({"mean": [
            (datetime64(2014, 1, 1), numpy.timedelta64(1, 'D'), 39.75),
            (datetime64(2014, 1, 1, 12), numpy.timedelta64(1, 'h'), 39.75),
            (datetime64(2014, 1, 1, 12), numpy.timedelta64(5, 'm'), 69.0),
        ]}, get_measures_list(self.storage.get_aggregated_measures(
            {self.metric: aggregations},
            from_timestamp=datetime64(2014, 1, 1, 12, 0, 0),
            to_timestamp=datetime64(2014, 1, 1, 12, 0, 2))[self.metric]))

        self.assertEqual({"mean": [
            (datetime64(2014, 1, 1), numpy.timedelta64(1, 'D'), 39.75),
            (datetime64(2014, 1, 1, 12), numpy.timedelta64(1, 'h'), 39.75),
            (datetime64(2014, 1, 1, 12), numpy.timedelta64(5, 'm'), 69.0),
        ]}, get_measures_list(self.storage.get_aggregated_measures(
            {self.metric: aggregations},
            from_timestamp=datetime64(2014, 1, 1, 12),
            to_timestamp=datetime64(2014, 1, 1, 12, 0, 2))[self.metric]))

        aggregation_1h = (
            self.metric.archive_policy.get_aggregation(
                "mean", numpy.timedelta64(1, 'h'))
        )

        self.assertEqual({"mean": [
            (datetime64(2014, 1, 1, 12), numpy.timedelta64(1, 'h'), 39.75),
        ]}, get_measures_list(self.storage.get_aggregated_measures(
            {self.metric: [aggregation_1h]},
            from_timestamp=datetime64(2014, 1, 1, 12, 0, 0),
            to_timestamp=datetime64(2014, 1, 1, 12, 0, 2))[self.metric]))

        aggregation_5m = (
            self.metric.archive_policy.get_aggregation(
                "mean", numpy.timedelta64(5, 'm'))
        )

        self.assertEqual({"mean": [
            (datetime64(2014, 1, 1, 12), numpy.timedelta64(5, 'm'), 69.0),
        ]}, get_measures_list(self.storage.get_aggregated_measures(
            {self.metric: [aggregation_5m]},
            from_timestamp=datetime64(2014, 1, 1, 12, 0, 0),
            to_timestamp=datetime64(2014, 1, 1, 12, 0, 2))[self.metric]))

        self.assertEqual({"mean": []},
                         get_measures_list(
                             self.storage.get_aggregated_measures(
                                 {self.metric:
                                     [carbonara.Aggregation(
                                         "mean", numpy.timedelta64(42, 's'),
                                      None)]})[self.metric]))

    def test_get_measure_unknown_aggregation(self):
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2014, 1, 1, 12, 0, 1), 69),
            incoming.Measure(datetime64(2014, 1, 1, 12, 7, 31), 42),
            incoming.Measure(datetime64(2014, 1, 1, 12, 9, 31), 4),
            incoming.Measure(datetime64(2014, 1, 1, 12, 12, 45), 44),
        ])

        aggregations = (
            self.metric.archive_policy.get_aggregations_for_method("last")
        )

        self.assertRaises(
            storage.MetricDoesNotExist,
            self.storage.get_aggregated_measures,
            {self.metric: aggregations})

    def test_resize_policy(self):
        name = str(uuid.uuid4())
        ap = archive_policy.ArchivePolicy(name, 0, [(3, 5)])
        self.index.create_archive_policy(ap)
        m = self.index.create_metric(uuid.uuid4(), str(uuid.uuid4()), name)
        m = self.index.list_metrics(attribute_filter={"=": {"id": m.id}})[0]
        self.incoming.add_measures(m.id, [
            incoming.Measure(datetime64(2014, 1, 1, 12, 0, 0), 1),
            incoming.Measure(datetime64(2014, 1, 1, 12, 0, 5), 1),
            incoming.Measure(datetime64(2014, 1, 1, 12, 0, 10), 1),
        ])
        self.trigger_processing([m])

        aggregation = m.archive_policy.get_aggregation(
            "mean", numpy.timedelta64(5, 's'))

        self.assertEqual({"mean": [
            (datetime64(2014, 1, 1, 12, 0, 0), numpy.timedelta64(5, 's'), 1),
            (datetime64(2014, 1, 1, 12, 0, 5), numpy.timedelta64(5, 's'), 1),
            (datetime64(2014, 1, 1, 12, 0, 10), numpy.timedelta64(5, 's'), 1),
        ]}, get_measures_list(self.storage.get_aggregated_measures(
            {m: [aggregation]})[m]))
        # expand to more points
        self.index.update_archive_policy(
            name, [archive_policy.ArchivePolicyItem(granularity=5, points=6)])
        m = self.index.list_metrics(attribute_filter={"=": {"id": m.id}})[0]
        self.incoming.add_measures(m.id, [
            incoming.Measure(datetime64(2014, 1, 1, 12, 0, 15), 1),
        ])
        self.trigger_processing([m])
        self.assertEqual({"mean": [
            (datetime64(2014, 1, 1, 12, 0, 5), numpy.timedelta64(5, 's'), 1),
            (datetime64(2014, 1, 1, 12, 0, 10), numpy.timedelta64(5, 's'), 1),
            (datetime64(2014, 1, 1, 12, 0, 15), numpy.timedelta64(5, 's'), 1),
        ]}, get_measures_list(self.storage.get_aggregated_measures(
            {m: [aggregation]})[m]))
        # shrink timespan
        self.index.update_archive_policy(
            name, [archive_policy.ArchivePolicyItem(granularity=5, points=2)])
        m = self.index.list_metrics(attribute_filter={"=": {"id": m.id}})[0]
        aggregation = m.archive_policy.get_aggregation(
            "mean", numpy.timedelta64(5, 's'))
        self.assertEqual({"mean": [
            (datetime64(2014, 1, 1, 12, 0, 10), numpy.timedelta64(5, 's'), 1),
            (datetime64(2014, 1, 1, 12, 0, 15), numpy.timedelta64(5, 's'), 1),
        ]}, get_measures_list(self.storage.get_aggregated_measures(
            {m: [aggregation]})[m]))

    def test_resample_no_metric(self):
        """https://github.com/gnocchixyz/gnocchi/issues/69"""
        aggregation = self.metric.archive_policy.get_aggregation(
            "mean", numpy.timedelta64(300, 's'))
        self.assertRaises(storage.MetricDoesNotExist,
                          self.storage.get_aggregated_measures,
                          {self.metric:
                              [aggregation]},
                          datetime64(2014, 1, 1),
                          datetime64(2015, 1, 1),
                          resample=numpy.timedelta64(1, 'h'))

    def test_get_latest_timestmap_of_measures(self):
        measures = {"timestamps": [numpy.datetime64('1976-01-01T00:00:00'), numpy.datetime64('1970-02-01T00:00:00'),
                                   numpy.datetime64('1970-01-01T00:00:00'), numpy.datetime64('2030-01-01T00:00:00')]}

        expected_valued = numpy.datetime64('2030-01-01T00:00:00')
        expected_valued = datetime.datetime.utcfromtimestamp(
            (expected_valued - numpy.datetime64('1970-01-01T00:00:00')) / numpy.timedelta64(1, 's'))

        expected_valued = expected_valued.replace(tzinfo=datetime.timezone.utc)
        latest_timestmap_of_measures = self.storage.get_latest_timestmap_of_measures(measures)

        self.assertEqual(expected_valued, latest_timestmap_of_measures)

    def test_store_data_backend(self):

        with mock.patch.object(self.storage.statistics, 'time') as time_mock_method:
            with mock.patch.object(self.storage, '_delete_metric_splits') as delete_metric_splits_mock:
                with mock.patch.object(self.storage, '_update_metric_splits') as update_metric_splits_mock:
                    with mock.patch.object(
                            self.storage, '_store_unaggregated_timeseries') as store_unaggregated_timeseries_mock:

                        new_boundts_mock = {}
                        splits_to_delete_mock = {}
                        splits_to_update = {}

                        self.storage.store_data_backend(new_boundts_mock, splits_to_delete_mock, splits_to_update)

                        # We use any_order=True here to avoid errors with other calls that we can ignore.
                        time_mock_method.assert_has_calls([
                            mock.call("splits delete"),
                            mock.call("splits update"),
                            mock.call("raw measures store")], any_order=True)

                        delete_metric_splits_mock.assert_has_calls([mock.call(splits_to_delete_mock)])
                        update_metric_splits_mock.assert_has_calls([mock.call(splits_to_update)])
                        store_unaggregated_timeseries_mock.assert_has_calls([mock.call(new_boundts_mock)])

                        self.assertEqual(3, time_mock_method.call_count)
                        self.assertEqual(1, delete_metric_splits_mock.call_count)
                        self.assertEqual(1, update_metric_splits_mock.call_count)
                        self.assertEqual(1, store_unaggregated_timeseries_mock.call_count)

    def test_execute_metadata_updates_if_needed_needs_raw_data_truncation(self):
        measures = {"timestamps": [numpy.datetime64('1979-01-01T00:00:00'), numpy.datetime64('2030-01-01T00:00:00'),
                                   numpy.datetime64('1973-01-01T00:00:00')]}

        indexer_driver_mock = mock.Mock()
        metric_mock = mock.Mock()
        metric_mock.needs_raw_data_truncation = True
        metric_mock.resource_id = None

        resource_mock = mock.Mock()
        resource_mock.ended_at = datetime.datetime.fromisoformat('2023-11-04').replace(tzinfo=datetime.timezone.utc)

        indexer_driver_mock.get_resource.return_value = resource_mock

        with mock.patch('gnocchi.storage.LOG') as log_mock:
            self.storage.execute_metadata_updates_if_needed(indexer_driver_mock, measures, metric_mock)

            indexer_driver_mock.update_needs_raw_data_truncation.assert_has_calls([mock.call(metric_mock.id)])
            indexer_driver_mock.update_last_measure_timestamp.assert_has_calls([mock.call(metric_mock.id)])

            self.assertEqual(1, indexer_driver_mock.update_needs_raw_data_truncation.call_count)
            self.assertEqual(1, indexer_driver_mock.update_last_measure_timestamp.call_count)

            log_mock.debug.assert_has_calls([
                mock.call("Metric [%s] does not have a resource assigned to it.", metric_mock)])

            self.assertEqual(0, log_mock.info.call_count)
            self.assertEqual(1, log_mock.debug.call_count)

    def test_execute_metadata_updates_if_needed_no_need_for_raw_data_truncation(self):
        measures = {"timestamps": [numpy.datetime64('1979-01-01T00:00:00'), numpy.datetime64('2030-01-01T00:00:00'),
                                   numpy.datetime64('1973-01-01T00:00:00')]}

        indexer_driver_mock = mock.Mock()
        metric_mock = mock.Mock()
        metric_mock.needs_raw_data_truncation = False
        metric_mock.resource_id = None

        resource_mock = mock.Mock()
        resource_mock.ended_at = datetime.datetime.fromisoformat('2023-11-04').replace(tzinfo=datetime.timezone.utc)

        indexer_driver_mock.get_resource.return_value = resource_mock

        with mock.patch('gnocchi.storage.LOG') as log_mock:
            self.storage.execute_metadata_updates_if_needed(indexer_driver_mock, measures, metric_mock)
            indexer_driver_mock.update_last_measure_timestamp.assert_has_calls([mock.call(metric_mock.id)])

            self.assertEqual(1, indexer_driver_mock.update_last_measure_timestamp.call_count)
            self.assertEqual(0, indexer_driver_mock.update_needs_raw_data_truncation.call_count)

            log_mock.debug.assert_has_calls([
                mock.call("Metric [%s] does not have a resource assigned to it.", metric_mock)])

            self.assertEqual(0, log_mock.info.call_count)
            self.assertEqual(1, log_mock.debug.call_count)

    def test_execute_metadata_updates_if_needed_resource_recover(self):
        measures = {"timestamps": [numpy.datetime64('1979-01-01T00:00:00'), numpy.datetime64('2030-01-01T00:00:00'),
                                   numpy.datetime64('1973-01-01T00:00:00')]}

        indexer_driver_mock = mock.Mock()
        metric_mock = mock.Mock()
        metric_mock.needs_raw_data_truncation = True

        resource_id = 1
        metric_mock.resource_id = resource_id

        resource_mock = mock.Mock()
        resource_mock.ended_at = datetime.datetime.fromisoformat('2023-11-04').replace(tzinfo=datetime.timezone.utc)

        indexer_driver_mock.get_resource.return_value = resource_mock

        with mock.patch('gnocchi.storage.LOG') as log_mock:
            self.storage.execute_metadata_updates_if_needed(indexer_driver_mock, measures, metric_mock)

            indexer_driver_mock.update_needs_raw_data_truncation.assert_has_calls([mock.call(metric_mock.id)])
            indexer_driver_mock.update_last_measure_timestamp.assert_has_calls([mock.call(metric_mock.id)])

            indexer_driver_mock.update_resource.assert_has_calls([
                mock.call(resource_mock.type, resource_id, ended_at=None)])

            self.assertEqual(1, indexer_driver_mock.update_needs_raw_data_truncation.call_count)
            self.assertEqual(1, indexer_driver_mock.update_last_measure_timestamp.call_count)
            self.assertEqual(1, indexer_driver_mock.update_resource.call_count)

            log_mock.info.assert_has_calls([
                mock.call("Resource [%s] was marked with a timestamp for the 'ended_at' field. It received a "
                          "measurement for metric [%s]. Therefore, restoring it.", resource_mock, metric_mock.id)])

            log_mock.debug.assert_has_calls([
                mock.call("Checking if resource [%s] of metric [%s] with resource ID [%s] needs to be restored. The "
                          "measurement timestamps are [%s].", resource_mock, metric_mock.id, resource_id,
                          measures['timestamps'])])

            self.assertEqual(1, log_mock.info.call_count)
            self.assertEqual(1, log_mock.debug.call_count)

    def test_execute_metadata_updates_if_needed_resource_no_recover(self):
        measures = {"timestamps": [numpy.datetime64('1979-01-01T00:00:00'), numpy.datetime64('2022-01-01T00:00:00'),
                                   numpy.datetime64('1973-01-01T00:00:00')]}

        indexer_driver_mock = mock.Mock()
        metric_mock = mock.Mock()
        metric_mock.needs_raw_data_truncation = True

        resource_id = 1
        metric_mock.resource_id = resource_id

        resource_mock = mock.Mock()
        resource_mock.ended_at = datetime.datetime.fromisoformat('2023-11-04').replace(tzinfo=datetime.timezone.utc)

        indexer_driver_mock.get_resource.return_value = resource_mock

        latest_timestamp_in_measurements = datetime.datetime.fromisoformat('2022-01-01').replace(
            tzinfo=datetime.timezone.utc)

        with mock.patch('gnocchi.storage.LOG') as log_mock:
            self.storage.execute_metadata_updates_if_needed(indexer_driver_mock, measures, metric_mock)

            indexer_driver_mock.update_needs_raw_data_truncation.assert_has_calls([mock.call(metric_mock.id)])
            indexer_driver_mock.update_last_measure_timestamp.assert_has_calls([mock.call(metric_mock.id)])

            self.assertEqual(1, indexer_driver_mock.update_needs_raw_data_truncation.call_count)
            self.assertEqual(1, indexer_driver_mock.update_last_measure_timestamp.call_count)
            self.assertEqual(0, indexer_driver_mock.update_resource.call_count)

            log_mock.info.assert_has_calls([
                mock.call("Resource [%s] was marked with a timestamp for the 'ended_at' field. It received a "
                          "measurement for metric [%s]. However, we do not restore it as the latest timestamp "
                          "of the measurement is [%s].", resource_mock, metric_mock.id,
                          latest_timestamp_in_measurements)])

            log_mock.debug.assert_has_calls([
                mock.call("Checking if resource [%s] of metric [%s] with resource ID [%s] needs to be restored. The "
                          "measurement timestamps are [%s].", resource_mock, metric_mock.id, resource_id,
                          measures['timestamps'])])

            self.assertEqual(1, log_mock.info.call_count)
            self.assertEqual(1, log_mock.debug.call_count)

    def test_add_measures_to_metrics(self):
        raw_measures_mock = mock.Mock()

        with mock.patch.object(
                self.storage, 'get_raw_measures', return_value=raw_measures_mock) as get_raw_measures_mock:
            with mock.patch.object(self.storage, 'execute_data_processing') as execute_data_processing_mock:
                with mock.patch.object(self.storage,
                                       'execute_metadata_updates_if_needed') as execute_metadata_updates_if_needed_mock:
                    with mock.patch.object(self.storage, 'store_data_backend') as store_data_backend_mock:
                        measures_to_use = {"timestamps": [numpy.datetime64('1979-01-01T00:00:00'),
                                                          numpy.datetime64('2022-01-01T00:00:00'),
                                                          numpy.datetime64('1973-01-01T00:00:00')]}
                        with mock.patch("numpy.sort", return_value=measures_to_use) as numpy_sort_mock:
                            indexer_driver_mock = mock.Mock()
                            metrics_and_measures = {"metric1": measures_to_use}

                            self.storage.add_measures_to_metrics(metrics_and_measures, indexer_driver_mock)

                            self.assertEqual(1, numpy_sort_mock.call_count)
                            self.assertEqual(1, get_raw_measures_mock.call_count)
                            self.assertEqual(1, execute_data_processing_mock.call_count)
                            self.assertEqual(1, execute_metadata_updates_if_needed_mock.call_count)
                            self.assertEqual(1, store_data_backend_mock.call_count)

                            get_raw_measures_mock.assert_has_calls([mock.call(metrics_and_measures)])
                            store_data_backend_mock.assert_has_calls([mock.call([], {}, {})])

                            for metric, measures in metrics_and_measures.items():
                                numpy_sort_mock.assert_has_calls([mock.call(measures, order='timestamps')])
                                execute_data_processing_mock.assert_has_calls(
                                    [mock.call(measures, metric, [], raw_measures_mock, {}, {})])
                                execute_metadata_updates_if_needed_mock.assert_has_calls(
                                    [mock.call(indexer_driver_mock, measures_to_use, metric)])
