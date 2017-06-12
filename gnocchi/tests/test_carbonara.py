# -*- encoding: utf-8 -*-
#
# Copyright © 2014-2016 eNovance
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
import functools
import math

import fixtures
import iso8601
import pandas
import six

from gnocchi import carbonara
from gnocchi.tests import base


class TestBoundTimeSerie(base.BaseTestCase):
    def test_benchmark(self):
        self.useFixture(fixtures.Timeout(300, gentle=True))
        carbonara.BoundTimeSerie.benchmark()

    @staticmethod
    def test_base():
        carbonara.BoundTimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6])

    def test_block_size(self):
        ts = carbonara.BoundTimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6],
            block_size='5s')
        self.assertEqual(1, len(ts))
        ts.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 10), 3),
                       (datetime.datetime(2014, 1, 1, 12, 0, 11), 4)])
        self.assertEqual(2, len(ts))

    def test_block_size_back_window(self):
        ts = carbonara.BoundTimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6],
            block_size='5s',
            back_window=1)
        self.assertEqual(3, len(ts))
        ts.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 10), 3),
                       (datetime.datetime(2014, 1, 1, 12, 0, 11), 4)])
        self.assertEqual(3, len(ts))

    def test_block_size_unordered(self):
        ts = carbonara.BoundTimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 5),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [10, 5, 23],
            block_size='5s')
        self.assertEqual(2, len(ts))
        ts.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 11), 3),
                       (datetime.datetime(2014, 1, 1, 12, 0, 10), 4)])
        self.assertEqual(2, len(ts))

    def test_duplicate_timestamps(self):
        ts = carbonara.BoundTimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [10, 23])
        self.assertEqual(2, len(ts))
        self.assertEqual(10.0, ts[0])
        self.assertEqual(23.0, ts[1])

        ts.set_values([(datetime.datetime(2014, 1, 1, 13, 0, 10), 3),
                       (datetime.datetime(2014, 1, 1, 13, 0, 11), 9),
                       (datetime.datetime(2014, 1, 1, 13, 0, 11), 8),
                       (datetime.datetime(2014, 1, 1, 13, 0, 11), 7),
                       (datetime.datetime(2014, 1, 1, 13, 0, 11), 4)])
        self.assertEqual(4, len(ts))
        self.assertEqual(10.0, ts[0])
        self.assertEqual(23.0, ts[1])
        self.assertEqual(3.0, ts[2])
        self.assertEqual(4.0, ts[3])


class TestAggregatedTimeSerie(base.BaseTestCase):
    @staticmethod
    def test_base():
        carbonara.AggregatedTimeSerie.from_data(
            3, 'mean',
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6])
        carbonara.AggregatedTimeSerie.from_data(
            "4s", 'mean',
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6])

    def test_benchmark(self):
        self.useFixture(fixtures.Timeout(300, gentle=True))
        carbonara.AggregatedTimeSerie.benchmark()

    def test_fetch_basic(self):
        ts = carbonara.AggregatedTimeSerie.from_data(
            timestamps=[datetime.datetime(2014, 1, 1, 12, 0, 0),
                        datetime.datetime(2014, 1, 1, 12, 0, 4),
                        datetime.datetime(2014, 1, 1, 12, 0, 9)],
            aggregation_method='mean',
            values=[3, 5, 6],
            sampling="1s")
        self.assertEqual(
            [(datetime.datetime(2014, 1, 1, 12), 1, 3),
             (datetime.datetime(2014, 1, 1, 12, 0, 4), 1, 5),
             (datetime.datetime(2014, 1, 1, 12, 0, 9), 1, 6)],
            list(ts.fetch()))
        self.assertEqual(
            [(datetime.datetime(2014, 1, 1, 12, 0, 4), 1, 5),
             (datetime.datetime(2014, 1, 1, 12, 0, 9), 1, 6)],
            list(ts.fetch(
                from_timestamp=datetime.datetime(2014, 1, 1, 12, 0, 4))))
        self.assertEqual(
            [(datetime.datetime(2014, 1, 1, 12, 0, 4), 1, 5),
             (datetime.datetime(2014, 1, 1, 12, 0, 9), 1, 6)],
            list(ts.fetch(
                from_timestamp=iso8601.parse_date(
                    "2014-01-01 12:00:04"))))
        self.assertEqual(
            [(datetime.datetime(2014, 1, 1, 12, 0, 4), 1, 5),
             (datetime.datetime(2014, 1, 1, 12, 0, 9), 1, 6)],
            list(ts.fetch(
                from_timestamp=iso8601.parse_date(
                    "2014-01-01 13:00:04+01:00"))))

    def test_before_epoch(self):
        ts = carbonara.TimeSerie.from_tuples(
            [(datetime.datetime(1950, 1, 1, 12), 3),
             (datetime.datetime(2014, 1, 1, 12), 5),
             (datetime.datetime(2014, 1, 1, 12), 6)])

        self.assertRaises(carbonara.BeforeEpochError,
                          ts.group_serie, 60)

    @staticmethod
    def _resample(ts, sampling, agg, max_size=None):
        grouped = ts.group_serie(sampling)
        return carbonara.AggregatedTimeSerie.from_grouped_serie(
            grouped, sampling, agg, max_size=max_size)

    def test_74_percentile_serialized(self):
        ts = carbonara.TimeSerie.from_tuples(
            [(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
             (datetime.datetime(2014, 1, 1, 12, 0, 4), 5),
             (datetime.datetime(2014, 1, 1, 12, 0, 9), 6)])
        ts = self._resample(ts, 60, '74pct')

        self.assertEqual(1, len(ts))
        self.assertEqual(5.48, ts[datetime.datetime(2014, 1, 1, 12, 0, 0)])

        # Serialize and unserialize
        key = ts.get_split_key()
        o, s = ts.serialize(key)
        saved_ts = carbonara.AggregatedTimeSerie.unserialize(
            s, key, '74pct', ts.sampling)

        ts = carbonara.TimeSerie.from_tuples(
            [(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
             (datetime.datetime(2014, 1, 1, 12, 0, 4), 5),
             (datetime.datetime(2014, 1, 1, 12, 0, 9), 6)])
        ts = self._resample(ts, 60, '74pct')
        ts.merge(saved_ts)

        self.assertEqual(1, len(ts))
        self.assertEqual(5.48, ts[datetime.datetime(2014, 1, 1, 12, 0, 0)])

    def test_95_percentile(self):
        ts = carbonara.TimeSerie.from_tuples(
            [(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
             (datetime.datetime(2014, 1, 1, 12, 0, 4), 5),
             (datetime.datetime(2014, 1, 1, 12, 0, 9), 6)])
        ts = self._resample(ts, 60, '95pct')

        self.assertEqual(1, len(ts))
        self.assertEqual(5.9000000000000004,
                         ts[datetime.datetime(2014, 1, 1, 12, 0, 0)])

    def _do_test_aggregation(self, name, v1, v2):
        ts = carbonara.TimeSerie.from_tuples(
            [(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
             (datetime.datetime(2014, 1, 1, 12, 0, 4), 6),
             (datetime.datetime(2014, 1, 1, 12, 0, 9), 5),
             (datetime.datetime(2014, 1, 1, 12, 1, 4), 8),
             (datetime.datetime(2014, 1, 1, 12, 1, 6), 9)])
        ts = self._resample(ts, 60, name)

        self.assertEqual(2, len(ts))
        self.assertEqual(v1, ts[datetime.datetime(2014, 1, 1, 12, 0, 0)])
        self.assertEqual(v2, ts[datetime.datetime(2014, 1, 1, 12, 1, 0)])

    def test_aggregation_first(self):
        self._do_test_aggregation('first', 3, 8)

    def test_aggregation_last(self):
        self._do_test_aggregation('last', 5, 9)

    def test_aggregation_count(self):
        self._do_test_aggregation('count', 3, 2)

    def test_aggregation_sum(self):
        self._do_test_aggregation('sum', 14, 17)

    def test_aggregation_mean(self):
        self._do_test_aggregation('mean', 4.666666666666667, 8.5)

    def test_aggregation_median(self):
        self._do_test_aggregation('median', 5.0, 8.5)

    def test_aggregation_min(self):
        self._do_test_aggregation('min', 3, 8)

    def test_aggregation_max(self):
        self._do_test_aggregation('max', 6, 9)

    def test_aggregation_std(self):
        self._do_test_aggregation('std', 1.5275252316519465,
                                  0.70710678118654757)

    def test_aggregation_std_with_unique(self):
        ts = carbonara.TimeSerie.from_tuples(
            [(datetime.datetime(2014, 1, 1, 12, 0, 0), 3)])
        ts = self._resample(ts, 60, 'std')
        self.assertEqual(0, len(ts), ts.ts.values)

        ts = carbonara.TimeSerie.from_tuples(
            [(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
             (datetime.datetime(2014, 1, 1, 12, 0, 4), 6),
             (datetime.datetime(2014, 1, 1, 12, 0, 9), 5),
             (datetime.datetime(2014, 1, 1, 12, 1, 6), 9)])
        ts = self._resample(ts, 60, "std")

        self.assertEqual(1, len(ts))
        self.assertEqual(1.5275252316519465,
                         ts[datetime.datetime(2014, 1, 1, 12, 0, 0)])

    def test_different_length_in_timestamps_and_data(self):
        self.assertRaises(ValueError,
                          carbonara.AggregatedTimeSerie.from_data,
                          3, 'mean',
                          [datetime.datetime(2014, 1, 1, 12, 0, 0),
                           datetime.datetime(2014, 1, 1, 12, 0, 4),
                           datetime.datetime(2014, 1, 1, 12, 0, 9)],
                          [3, 5])

    def test_max_size(self):
        ts = carbonara.TimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6])
        ts = self._resample(ts, 1, 'mean', max_size=2)

        self.assertEqual(2, len(ts))
        self.assertEqual(5, ts[0])
        self.assertEqual(6, ts[1])

    def test_down_sampling(self):
        ts = carbonara.TimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 7])
        ts = self._resample(ts, 300, 'mean')

        self.assertEqual(1, len(ts))
        self.assertEqual(5, ts[datetime.datetime(2014, 1, 1, 12, 0, 0)])

    def test_down_sampling_with_max_size(self):
        ts = carbonara.TimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 1, 4),
             datetime.datetime(2014, 1, 1, 12, 1, 9),
             datetime.datetime(2014, 1, 1, 12, 2, 12)],
            [3, 5, 7, 1])
        ts = self._resample(ts, 60, 'mean', max_size=2)

        self.assertEqual(2, len(ts))
        self.assertEqual(6, ts[datetime.datetime(2014, 1, 1, 12, 1, 0)])
        self.assertEqual(1, ts[datetime.datetime(2014, 1, 1, 12, 2, 0)])

    def test_down_sampling_with_max_size_and_method_max(self):
        ts = carbonara.TimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 1, 4),
             datetime.datetime(2014, 1, 1, 12, 1, 9),
             datetime.datetime(2014, 1, 1, 12, 2, 12)],
            [3, 5, 70, 1])
        ts = self._resample(ts, 60, 'max', max_size=2)

        self.assertEqual(2, len(ts))
        self.assertEqual(70, ts[datetime.datetime(2014, 1, 1, 12, 1, 0)])
        self.assertEqual(1, ts[datetime.datetime(2014, 1, 1, 12, 2, 0)])

    @staticmethod
    def _resample_and_merge(ts, agg_dict):
        """Helper method that mimics _add_measures workflow."""
        grouped = ts.group_serie(agg_dict['sampling'])
        existing = agg_dict.get('return')
        agg_dict['return'] = carbonara.AggregatedTimeSerie.from_grouped_serie(
            grouped, agg_dict['sampling'], agg_dict['agg'],
            max_size=agg_dict.get('size'))
        if existing:
            agg_dict['return'].merge(existing)

    def test_aggregated_different_archive_no_overlap(self):
        tsc1 = {'sampling': 60, 'size': 50, 'agg': 'mean'}
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1['sampling'])
        tsc2 = {'sampling': 60, 'size': 50, 'agg': 'mean'}
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2['sampling'])

        tsb1.set_values([(datetime.datetime(2014, 1, 1, 11, 46, 4), 4)],
                        before_truncate_callback=functools.partial(
                            self._resample_and_merge, agg_dict=tsc1))
        tsb2.set_values([(datetime.datetime(2014, 1, 1, 9, 1, 4), 4)],
                        before_truncate_callback=functools.partial(
                            self._resample_and_merge, agg_dict=tsc2))

        dtfrom = datetime.datetime(2014, 1, 1, 11, 0, 0)
        self.assertRaises(carbonara.UnAggregableTimeseries,
                          carbonara.AggregatedTimeSerie.aggregated,
                          [tsc1['return'], tsc2['return']],
                          from_timestamp=dtfrom, aggregation='mean')

    def test_aggregated_different_archive_no_overlap2(self):
        tsc1 = {'sampling': 60, 'size': 50, 'agg': 'mean'}
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1['sampling'])
        tsc2 = carbonara.AggregatedTimeSerie(sampling=60, max_size=50,
                                             aggregation_method='mean')

        tsb1.set_values([(datetime.datetime(2014, 1, 1, 12, 3, 0), 4)],
                        before_truncate_callback=functools.partial(
                            self._resample_and_merge, agg_dict=tsc1))
        self.assertRaises(carbonara.UnAggregableTimeseries,
                          carbonara.AggregatedTimeSerie.aggregated,
                          [tsc1['return'], tsc2], aggregation='mean')

    def test_aggregated_different_archive_overlap(self):
        tsc1 = {'sampling': 60, 'size': 10, 'agg': 'mean'}
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1['sampling'])
        tsc2 = {'sampling': 60, 'size': 10, 'agg': 'mean'}
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2['sampling'])

        # NOTE(sileht): minute 8 is missing in both and
        # minute 7 in tsc2 too, but it looks like we have
        # enough point to do the aggregation
        tsb1.set_values([
            (datetime.datetime(2014, 1, 1, 11, 0, 0), 4),
            (datetime.datetime(2014, 1, 1, 12, 1, 0), 3),
            (datetime.datetime(2014, 1, 1, 12, 2, 0), 2),
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 4),
            (datetime.datetime(2014, 1, 1, 12, 4, 0), 2),
            (datetime.datetime(2014, 1, 1, 12, 5, 0), 3),
            (datetime.datetime(2014, 1, 1, 12, 6, 0), 4),
            (datetime.datetime(2014, 1, 1, 12, 7, 0), 10),
            (datetime.datetime(2014, 1, 1, 12, 9, 0), 2),
        ], before_truncate_callback=functools.partial(
            self._resample_and_merge, agg_dict=tsc1))

        tsb2.set_values([
            (datetime.datetime(2014, 1, 1, 12, 1, 0), 3),
            (datetime.datetime(2014, 1, 1, 12, 2, 0), 4),
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 4),
            (datetime.datetime(2014, 1, 1, 12, 4, 0), 6),
            (datetime.datetime(2014, 1, 1, 12, 5, 0), 3),
            (datetime.datetime(2014, 1, 1, 12, 6, 0), 6),
            (datetime.datetime(2014, 1, 1, 12, 9, 0), 2),
            (datetime.datetime(2014, 1, 1, 12, 11, 0), 2),
            (datetime.datetime(2014, 1, 1, 12, 12, 0), 2),
        ], before_truncate_callback=functools.partial(
            self._resample_and_merge, agg_dict=tsc2))

        dtfrom = datetime.datetime(2014, 1, 1, 12, 0, 0)
        dtto = datetime.datetime(2014, 1, 1, 12, 10, 0)

        # By default we require 100% of point that overlap
        # so that fail
        self.assertRaises(carbonara.UnAggregableTimeseries,
                          carbonara.AggregatedTimeSerie.aggregated,
                          [tsc1['return'], tsc2['return']],
                          from_timestamp=dtfrom,
                          to_timestamp=dtto, aggregation='mean')

        # Retry with 80% and it works
        output = carbonara.AggregatedTimeSerie.aggregated([
            tsc1['return'], tsc2['return']],
            from_timestamp=dtfrom, to_timestamp=dtto,
            aggregation='mean', needed_percent_of_overlap=80.0)

        self.assertEqual([
            (datetime.datetime(
                2014, 1, 1, 12, 1, 0
            ), 60.0, 3.0),
            (datetime.datetime(
                2014, 1, 1, 12, 2, 0
            ), 60.0, 3.0),
            (datetime.datetime(
                2014, 1, 1, 12, 3, 0
            ), 60.0, 4.0),
            (datetime.datetime(
                2014, 1, 1, 12, 4, 0
            ), 60.0, 4.0),
            (datetime.datetime(
                2014, 1, 1, 12, 5, 0
            ), 60.0, 3.0),
            (datetime.datetime(
                2014, 1, 1, 12, 6, 0
            ), 60.0, 5.0),
            (datetime.datetime(
                2014, 1, 1, 12, 7, 0
            ), 60.0, 10.0),
            (datetime.datetime(
                2014, 1, 1, 12, 9, 0
            ), 60.0, 2.0),
        ], list(output))

    def test_aggregated_different_archive_overlap_edge_missing1(self):
        tsc1 = {'sampling': 60, 'size': 10, 'agg': 'mean'}
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1['sampling'])
        tsc2 = {'sampling': 60, 'size': 10, 'agg': 'mean'}
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2['sampling'])

        tsb1.set_values([
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 9),
            (datetime.datetime(2014, 1, 1, 12, 4, 0), 1),
            (datetime.datetime(2014, 1, 1, 12, 5, 0), 2),
            (datetime.datetime(2014, 1, 1, 12, 6, 0), 7),
            (datetime.datetime(2014, 1, 1, 12, 7, 0), 5),
            (datetime.datetime(2014, 1, 1, 12, 8, 0), 3),
        ], before_truncate_callback=functools.partial(
            self._resample_and_merge, agg_dict=tsc1))

        tsb2.set_values([
            (datetime.datetime(2014, 1, 1, 11, 0, 0), 6),
            (datetime.datetime(2014, 1, 1, 12, 1, 0), 2),
            (datetime.datetime(2014, 1, 1, 12, 2, 0), 13),
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 24),
            (datetime.datetime(2014, 1, 1, 12, 4, 0), 4),
            (datetime.datetime(2014, 1, 1, 12, 5, 0), 16),
            (datetime.datetime(2014, 1, 1, 12, 6, 0), 12),
        ], before_truncate_callback=functools.partial(
            self._resample_and_merge, agg_dict=tsc2))

        # By default we require 100% of point that overlap
        # but we allow that the last datapoint is missing
        # of the precisest granularity
        output = carbonara.AggregatedTimeSerie.aggregated([
            tsc1['return'], tsc2['return']], aggregation='sum')

        self.assertEqual([
            (datetime.datetime(
                2014, 1, 1, 12, 3, 0
            ), 60.0, 33.0),
            (datetime.datetime(
                2014, 1, 1, 12, 4, 0
            ), 60.0, 5.0),
            (datetime.datetime(
                2014, 1, 1, 12, 5, 0
            ), 60.0, 18.0),
            (datetime.datetime(
                2014, 1, 1, 12, 6, 0
            ), 60.0, 19.0),
        ], list(output))

    def test_aggregated_different_archive_overlap_edge_missing2(self):
        tsc1 = {'sampling': 60, 'size': 10, 'agg': 'mean'}
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1['sampling'])
        tsc2 = {'sampling': 60, 'size': 10, 'agg': 'mean'}
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2['sampling'])

        tsb1.set_values([
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 4),
        ], before_truncate_callback=functools.partial(
            self._resample_and_merge, agg_dict=tsc1))

        tsb2.set_values([
            (datetime.datetime(2014, 1, 1, 11, 0, 0), 4),
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 4),
        ], before_truncate_callback=functools.partial(
            self._resample_and_merge, agg_dict=tsc2))

        output = carbonara.AggregatedTimeSerie.aggregated(
            [tsc1['return'], tsc2['return']], aggregation='mean')
        self.assertEqual([
            (datetime.datetime(
                2014, 1, 1, 12, 3, 0
            ), 60.0, 4.0),
        ], list(output))

    def test_fetch(self):
        ts = {'sampling': 60, 'size': 10, 'agg': 'mean'}
        tsb = carbonara.BoundTimeSerie(block_size=ts['sampling'])

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 11, 46, 4), 4),
            (datetime.datetime(2014, 1, 1, 11, 47, 34), 8),
            (datetime.datetime(2014, 1, 1, 11, 50, 54), 50),
            (datetime.datetime(2014, 1, 1, 11, 54, 45), 4),
            (datetime.datetime(2014, 1, 1, 11, 56, 49), 4),
            (datetime.datetime(2014, 1, 1, 11, 57, 22), 6),
            (datetime.datetime(2014, 1, 1, 11, 58, 22), 5),
            (datetime.datetime(2014, 1, 1, 12, 1, 4), 4),
            (datetime.datetime(2014, 1, 1, 12, 1, 9), 7),
            (datetime.datetime(2014, 1, 1, 12, 2, 1), 15),
            (datetime.datetime(2014, 1, 1, 12, 2, 12), 1),
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 3),
            (datetime.datetime(2014, 1, 1, 12, 4, 9), 7),
            (datetime.datetime(2014, 1, 1, 12, 5, 1), 15),
            (datetime.datetime(2014, 1, 1, 12, 5, 12), 1),
            (datetime.datetime(2014, 1, 1, 12, 6, 0, 2), 3),
        ], before_truncate_callback=functools.partial(
            self._resample_and_merge, agg_dict=ts))

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 12, 6), 5),
        ], before_truncate_callback=functools.partial(
            self._resample_and_merge, agg_dict=ts))

        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 11, 54), 60.0, 4.0),
            (datetime.datetime(2014, 1, 1, 11, 56), 60.0, 4.0),
            (datetime.datetime(2014, 1, 1, 11, 57), 60.0, 6.0),
            (datetime.datetime(2014, 1, 1, 11, 58), 60.0, 5.0),
            (datetime.datetime(2014, 1, 1, 12, 1), 60.0, 5.5),
            (datetime.datetime(2014, 1, 1, 12, 2), 60.0, 8.0),
            (datetime.datetime(2014, 1, 1, 12, 3), 60.0, 3.0),
            (datetime.datetime(2014, 1, 1, 12, 4), 60.0, 7.0),
            (datetime.datetime(2014, 1, 1, 12, 5), 60.0, 8.0),
            (datetime.datetime(2014, 1, 1, 12, 6), 60.0, 4.0)
        ], list(ts['return'].fetch()))

        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 12, 1), 60.0, 5.5),
            (datetime.datetime(2014, 1, 1, 12, 2), 60.0, 8.0),
            (datetime.datetime(2014, 1, 1, 12, 3), 60.0, 3.0),
            (datetime.datetime(2014, 1, 1, 12, 4), 60.0, 7.0),
            (datetime.datetime(2014, 1, 1, 12, 5), 60.0, 8.0),
            (datetime.datetime(2014, 1, 1, 12, 6), 60.0, 4.0)
        ], list(ts['return'].fetch(datetime.datetime(2014, 1, 1, 12, 0, 0))))

    def test_aggregated_some_overlap_with_fill_zero(self):
        tsc1 = {'sampling': 60, 'size': 10, 'agg': 'mean'}
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1['sampling'])
        tsc2 = {'sampling': 60, 'size': 10, 'agg': 'mean'}
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2['sampling'])

        tsb1.set_values([
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 9),
            (datetime.datetime(2014, 1, 1, 12, 4, 0), 1),
            (datetime.datetime(2014, 1, 1, 12, 5, 0), 2),
            (datetime.datetime(2014, 1, 1, 12, 6, 0), 7),
            (datetime.datetime(2014, 1, 1, 12, 7, 0), 5),
            (datetime.datetime(2014, 1, 1, 12, 8, 0), 3),
        ], before_truncate_callback=functools.partial(
            self._resample_and_merge, agg_dict=tsc1))

        tsb2.set_values([
            (datetime.datetime(2014, 1, 1, 12, 0, 0), 6),
            (datetime.datetime(2014, 1, 1, 12, 1, 0), 2),
            (datetime.datetime(2014, 1, 1, 12, 2, 0), 13),
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 24),
            (datetime.datetime(2014, 1, 1, 12, 4, 0), 4),
            (datetime.datetime(2014, 1, 1, 12, 5, 0), 16),
            (datetime.datetime(2014, 1, 1, 12, 6, 0), 12),
        ], before_truncate_callback=functools.partial(
            self._resample_and_merge, agg_dict=tsc2))

        output = carbonara.AggregatedTimeSerie.aggregated([
            tsc1['return'], tsc2['return']], aggregation='mean', fill=0)

        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 12, 0, 0), 60.0, 3.0),
            (datetime.datetime(2014, 1, 1, 12, 1, 0), 60.0, 1.0),
            (datetime.datetime(2014, 1, 1, 12, 2, 0), 60.0, 6.5),
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 60.0, 16.5),
            (datetime.datetime(2014, 1, 1, 12, 4, 0), 60.0, 2.5),
            (datetime.datetime(2014, 1, 1, 12, 5, 0), 60.0, 9.0),
            (datetime.datetime(2014, 1, 1, 12, 6, 0), 60.0, 9.5),
            (datetime.datetime(2014, 1, 1, 12, 7, 0), 60.0, 2.5),
            (datetime.datetime(2014, 1, 1, 12, 8, 0), 60.0, 1.5),
        ], list(output))

    def test_aggregated_some_overlap_with_fill_null(self):
        tsc1 = {'sampling': 60, 'size': 10, 'agg': 'mean'}
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1['sampling'])
        tsc2 = {'sampling': 60, 'size': 10, 'agg': 'mean'}
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2['sampling'])

        tsb1.set_values([
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 9),
            (datetime.datetime(2014, 1, 1, 12, 4, 0), 1),
            (datetime.datetime(2014, 1, 1, 12, 5, 0), 2),
            (datetime.datetime(2014, 1, 1, 12, 6, 0), 7),
            (datetime.datetime(2014, 1, 1, 12, 7, 0), 5),
            (datetime.datetime(2014, 1, 1, 12, 8, 0), 3),
        ], before_truncate_callback=functools.partial(
            self._resample_and_merge, agg_dict=tsc1))

        tsb2.set_values([
            (datetime.datetime(2014, 1, 1, 12, 0, 0), 6),
            (datetime.datetime(2014, 1, 1, 12, 1, 0), 2),
            (datetime.datetime(2014, 1, 1, 12, 2, 0), 13),
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 24),
            (datetime.datetime(2014, 1, 1, 12, 4, 0), 4),
            (datetime.datetime(2014, 1, 1, 12, 5, 0), 16),
            (datetime.datetime(2014, 1, 1, 12, 6, 0), 12),
        ], before_truncate_callback=functools.partial(
            self._resample_and_merge, agg_dict=tsc2))

        output = carbonara.AggregatedTimeSerie.aggregated([
            tsc1['return'], tsc2['return']], aggregation='mean', fill='null')

        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 12, 0, 0), 60.0, 6.0),
            (datetime.datetime(2014, 1, 1, 12, 1, 0), 60.0, 2.0),
            (datetime.datetime(2014, 1, 1, 12, 2, 0), 60.0, 13.0),
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 60.0, 16.5),
            (datetime.datetime(2014, 1, 1, 12, 4, 0), 60.0, 2.5),
            (datetime.datetime(2014, 1, 1, 12, 5, 0), 60.0, 9.0),
            (datetime.datetime(2014, 1, 1, 12, 6, 0), 60.0, 9.5),
            (datetime.datetime(2014, 1, 1, 12, 7, 0), 60.0, 5.0),
            (datetime.datetime(2014, 1, 1, 12, 8, 0), 60.0, 3.0),
        ], list(output))

    def test_aggregate_no_points_with_fill_zero(self):
        tsc1 = {'sampling': 60, 'size': 10, 'agg': 'mean'}
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1['sampling'])
        tsc2 = {'sampling': 60, 'size': 10, 'agg': 'mean'}
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2['sampling'])

        tsb1.set_values([
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 9),
            (datetime.datetime(2014, 1, 1, 12, 4, 0), 1),
            (datetime.datetime(2014, 1, 1, 12, 7, 0), 5),
            (datetime.datetime(2014, 1, 1, 12, 8, 0), 3),
        ], before_truncate_callback=functools.partial(
            self._resample_and_merge, agg_dict=tsc1))

        tsb2.set_values([
            (datetime.datetime(2014, 1, 1, 12, 0, 0), 6),
            (datetime.datetime(2014, 1, 1, 12, 1, 0), 2),
            (datetime.datetime(2014, 1, 1, 12, 2, 0), 13),
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 24),
            (datetime.datetime(2014, 1, 1, 12, 4, 0), 4),
        ], before_truncate_callback=functools.partial(
            self._resample_and_merge, agg_dict=tsc2))

        output = carbonara.AggregatedTimeSerie.aggregated([
            tsc1['return'], tsc2['return']], aggregation='mean', fill=0)

        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 12, 0, 0), 60.0, 3.0),
            (datetime.datetime(2014, 1, 1, 12, 1, 0), 60.0, 1.0),
            (datetime.datetime(2014, 1, 1, 12, 2, 0), 60.0, 6.5),
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 60.0, 16.5),
            (datetime.datetime(2014, 1, 1, 12, 4, 0), 60.0, 2.5),
            (datetime.datetime(2014, 1, 1, 12, 7, 0), 60.0, 2.5),
            (datetime.datetime(2014, 1, 1, 12, 8, 0), 60.0, 1.5),
        ], list(output))

    def test_fetch_agg_pct(self):
        ts = {'sampling': 1, 'size': 3600 * 24, 'agg': '90pct'}
        tsb = carbonara.BoundTimeSerie(block_size=ts['sampling'])

        tsb.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
                        (datetime.datetime(2014, 1, 1, 12, 0, 0, 123), 4),
                        (datetime.datetime(2014, 1, 1, 12, 0, 2), 4)],
                       before_truncate_callback=functools.partial(
                           self._resample_and_merge, agg_dict=ts))

        result = ts['return'].fetch(datetime.datetime(2014, 1, 1, 12, 0, 0))
        reference = [
            (datetime.datetime(
                2014, 1, 1, 12, 0, 0
            ), 1.0, 3.9),
            (datetime.datetime(
                2014, 1, 1, 12, 0, 2
            ), 1.0, 4)
        ]

        self.assertEqual(len(reference), len(list(result)))

        for ref, res in zip(reference, result):
            self.assertEqual(ref[0], res[0])
            self.assertEqual(ref[1], res[1])
            # Rounding \o/
            self.assertAlmostEqual(ref[2], res[2])

        tsb.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 2, 113), 110)],
                       before_truncate_callback=functools.partial(
                           self._resample_and_merge, agg_dict=ts))

        result = ts['return'].fetch(datetime.datetime(2014, 1, 1, 12, 0, 0))
        reference = [
            (datetime.datetime(
                2014, 1, 1, 12, 0, 0
            ), 1.0, 3.9),
            (datetime.datetime(
                2014, 1, 1, 12, 0, 2
            ), 1.0, 99.4)
        ]

        self.assertEqual(len(reference), len(list(result)))

        for ref, res in zip(reference, result):
            self.assertEqual(ref[0], res[0])
            self.assertEqual(ref[1], res[1])
            # Rounding \o/
            self.assertAlmostEqual(ref[2], res[2])

    def test_fetch_nano(self):
        ts = {'sampling': 0.2, 'size': 10, 'agg': 'mean'}
        tsb = carbonara.BoundTimeSerie(block_size=ts['sampling'])

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 11, 46, 0, 200123), 4),
            (datetime.datetime(2014, 1, 1, 11, 46, 0, 340000), 8),
            (datetime.datetime(2014, 1, 1, 11, 47, 0, 323154), 50),
            (datetime.datetime(2014, 1, 1, 11, 48, 0, 590903), 4),
            (datetime.datetime(2014, 1, 1, 11, 48, 0, 903291), 4),
        ], before_truncate_callback=functools.partial(
            self._resample_and_merge, agg_dict=ts))

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 11, 48, 0, 821312), 5),
        ], before_truncate_callback=functools.partial(
            self._resample_and_merge, agg_dict=ts))

        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 11, 46, 0, 200000), 0.2, 6.0),
            (datetime.datetime(2014, 1, 1, 11, 47, 0, 200000), 0.2, 50.0),
            (datetime.datetime(2014, 1, 1, 11, 48, 0, 400000), 0.2, 4.0),
            (datetime.datetime(2014, 1, 1, 11, 48, 0, 800000), 0.2, 4.5)
        ], list(ts['return'].fetch()))

    def test_fetch_agg_std(self):
        # NOTE (gordc): this is a good test to ensure we drop NaN entries
        # 2014-01-01 12:00:00 will appear if we don't dropna()
        ts = {'sampling': 60, 'size': 60, 'agg': 'std'}
        tsb = carbonara.BoundTimeSerie(block_size=ts['sampling'])

        tsb.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
                        (datetime.datetime(2014, 1, 1, 12, 1, 4), 4),
                        (datetime.datetime(2014, 1, 1, 12, 1, 9), 7),
                        (datetime.datetime(2014, 1, 1, 12, 2, 1), 15),
                        (datetime.datetime(2014, 1, 1, 12, 2, 12), 1)],
                       before_truncate_callback=functools.partial(
                           self._resample_and_merge, agg_dict=ts))

        self.assertEqual([
            (datetime.datetime(
                2014, 1, 1, 12, 1, 0
            ), 60.0, 2.1213203435596424),
            (datetime.datetime(
                2014, 1, 1, 12, 2, 0
            ), 60.0, 9.8994949366116654),
        ], list(ts['return'].fetch(datetime.datetime(2014, 1, 1, 12, 0, 0))))

        tsb.set_values([(datetime.datetime(2014, 1, 1, 12, 2, 13), 110)],
                       before_truncate_callback=functools.partial(
                           self._resample_and_merge, agg_dict=ts))

        self.assertEqual([
            (datetime.datetime(
                2014, 1, 1, 12, 1, 0
            ), 60.0, 2.1213203435596424),
            (datetime.datetime(
                2014, 1, 1, 12, 2, 0
            ), 60.0, 59.304300012730948),
        ], list(ts['return'].fetch(datetime.datetime(2014, 1, 1, 12, 0, 0))))

    def test_fetch_agg_max(self):
        ts = {'sampling': 60, 'size': 60, 'agg': 'max'}
        tsb = carbonara.BoundTimeSerie(block_size=ts['sampling'])

        tsb.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
                        (datetime.datetime(2014, 1, 1, 12, 1, 4), 4),
                        (datetime.datetime(2014, 1, 1, 12, 1, 9), 7),
                        (datetime.datetime(2014, 1, 1, 12, 2, 1), 15),
                        (datetime.datetime(2014, 1, 1, 12, 2, 12), 1)],
                       before_truncate_callback=functools.partial(
                           self._resample_and_merge, agg_dict=ts))

        self.assertEqual([
            (datetime.datetime(
                2014, 1, 1, 12, 0, 0
            ), 60.0, 3),
            (datetime.datetime(
                2014, 1, 1, 12, 1, 0
            ), 60.0, 7),
            (datetime.datetime(
                2014, 1, 1, 12, 2, 0
            ), 60.0, 15),
        ], list(ts['return'].fetch(datetime.datetime(2014, 1, 1, 12, 0, 0))))

        tsb.set_values([(datetime.datetime(2014, 1, 1, 12, 2, 13), 110)],
                       before_truncate_callback=functools.partial(
                           self._resample_and_merge, agg_dict=ts))

        self.assertEqual([
            (datetime.datetime(
                2014, 1, 1, 12, 0, 0
            ), 60.0, 3),
            (datetime.datetime(
                2014, 1, 1, 12, 1, 0
            ), 60.0, 7),
            (datetime.datetime(
                2014, 1, 1, 12, 2, 0
            ), 60.0, 110),
        ], list(ts['return'].fetch(datetime.datetime(2014, 1, 1, 12, 0, 0))))

    def test_serialize(self):
        ts = {'sampling': 0.5, 'agg': 'mean'}
        tsb = carbonara.BoundTimeSerie(block_size=ts['sampling'])

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 12, 0, 0, 1234), 3),
            (datetime.datetime(2014, 1, 1, 12, 0, 0, 321), 6),
            (datetime.datetime(2014, 1, 1, 12, 1, 4, 234), 5),
            (datetime.datetime(2014, 1, 1, 12, 1, 9, 32), 7),
            (datetime.datetime(2014, 1, 1, 12, 2, 12, 532), 1),
        ], before_truncate_callback=functools.partial(
            self._resample_and_merge, agg_dict=ts))

        key = ts['return'].get_split_key()
        o, s = ts['return'].serialize(key)
        self.assertEqual(ts['return'],
                         carbonara.AggregatedTimeSerie.unserialize(
                             s, key,
                             'mean', 0.5))

    def test_no_truncation(self):
        ts = {'sampling': 60, 'agg': 'mean'}
        tsb = carbonara.BoundTimeSerie()

        for i in six.moves.range(1, 11):
            tsb.set_values([
                (datetime.datetime(2014, 1, 1, 12, i, i), float(i))
            ], before_truncate_callback=functools.partial(
                self._resample_and_merge, agg_dict=ts))
            tsb.set_values([
                (datetime.datetime(2014, 1, 1, 12, i, i + 1), float(i + 1))
            ], before_truncate_callback=functools.partial(
                self._resample_and_merge, agg_dict=ts))
            self.assertEqual(i, len(list(ts['return'].fetch())))

    def test_back_window(self):
        """Back window testing.

        Test the back window on an archive is not longer than the window we
        aggregate on.
        """
        ts = {'sampling': 1, 'size': 60, 'agg': 'mean'}
        tsb = carbonara.BoundTimeSerie(block_size=ts['sampling'])

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 12, 0, 1, 2300), 1),
            (datetime.datetime(2014, 1, 1, 12, 0, 1, 4600), 2),
            (datetime.datetime(2014, 1, 1, 12, 0, 2, 4500), 3),
            (datetime.datetime(2014, 1, 1, 12, 0, 2, 7800), 4),
            (datetime.datetime(2014, 1, 1, 12, 0, 3, 8), 2.5),
        ], before_truncate_callback=functools.partial(
            self._resample_and_merge, agg_dict=ts))

        self.assertEqual(
            [
                (datetime.datetime(
                    2014, 1, 1, 12, 0, 1
                ), 1.0, 1.5),
                (datetime.datetime(
                    2014, 1, 1, 12, 0, 2
                ), 1.0, 3.5),
                (datetime.datetime(
                    2014, 1, 1, 12, 0, 3
                ), 1.0, 2.5),
            ],
            list(ts['return'].fetch()))

    def test_back_window_ignore(self):
        """Back window testing.

        Test the back window on an archive is not longer than the window we
        aggregate on.
        """
        ts = {'sampling': 1, 'size': 60, 'agg': 'mean'}
        tsb = carbonara.BoundTimeSerie(block_size=ts['sampling'])

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 12, 0, 1, 2300), 1),
            (datetime.datetime(2014, 1, 1, 12, 0, 1, 4600), 2),
            (datetime.datetime(2014, 1, 1, 12, 0, 2, 4500), 3),
            (datetime.datetime(2014, 1, 1, 12, 0, 2, 7800), 4),
            (datetime.datetime(2014, 1, 1, 12, 0, 3, 8), 2.5),
        ], before_truncate_callback=functools.partial(
            self._resample_and_merge, agg_dict=ts))

        self.assertEqual(
            [
                (datetime.datetime(
                    2014, 1, 1, 12, 0, 1
                ), 1.0, 1.5),
                (datetime.datetime(
                    2014, 1, 1, 12, 0, 2
                ), 1.0, 3.5),
                (datetime.datetime(
                    2014, 1, 1, 12, 0, 3
                ), 1.0, 2.5),
            ],
            list(ts['return'].fetch()))

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 12, 0, 2, 99), 9),
        ], before_truncate_callback=functools.partial(
            self._resample_and_merge, agg_dict=ts))

        self.assertEqual(
            [
                (datetime.datetime(
                    2014, 1, 1, 12, 0, 1
                ), 1.0, 1.5),
                (datetime.datetime(
                    2014, 1, 1, 12, 0, 2
                ), 1.0, 3.5),
                (datetime.datetime(
                    2014, 1, 1, 12, 0, 3
                ), 1.0, 2.5),
            ],
            list(ts['return'].fetch()))

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 12, 0, 2, 99), 9),
            (datetime.datetime(2014, 1, 1, 12, 0, 3, 9), 4.5),
        ], before_truncate_callback=functools.partial(
            self._resample_and_merge, agg_dict=ts))

        self.assertEqual(
            [
                (datetime.datetime(
                    2014, 1, 1, 12, 0, 1
                ), 1.0, 1.5),
                (datetime.datetime(
                    2014, 1, 1, 12, 0, 2
                ), 1.0, 3.5),
                (datetime.datetime(
                    2014, 1, 1, 12, 0, 3
                ), 1.0, 3.5),
            ],
            list(ts['return'].fetch()))

    def test_aggregated_nominal(self):
        tsc1 = {'sampling': 60, 'size': 10, 'agg': 'mean'}
        tsc12 = {'sampling': 300, 'size': 6, 'agg': 'mean'}
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc12['sampling'])
        tsc2 = {'sampling': 60, 'size': 10, 'agg': 'mean'}
        tsc22 = {'sampling': 300, 'size': 6, 'agg': 'mean'}
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc22['sampling'])

        def ts1_update(ts):
            grouped = ts.group_serie(tsc1['sampling'])
            existing = tsc1.get('return')
            tsc1['return'] = carbonara.AggregatedTimeSerie.from_grouped_serie(
                grouped, tsc1['sampling'], tsc1['agg'],
                max_size=tsc1['size'])
            if existing:
                tsc1['return'].merge(existing)
            grouped = ts.group_serie(tsc12['sampling'])
            existing = tsc12.get('return')
            tsc12['return'] = carbonara.AggregatedTimeSerie.from_grouped_serie(
                grouped, tsc12['sampling'], tsc12['agg'],
                max_size=tsc12['size'])
            if existing:
                tsc12['return'].merge(existing)

        def ts2_update(ts):
            grouped = ts.group_serie(tsc2['sampling'])
            existing = tsc2.get('return')
            tsc2['return'] = carbonara.AggregatedTimeSerie.from_grouped_serie(
                grouped, tsc2['sampling'], tsc2['agg'],
                max_size=tsc2['size'])
            if existing:
                tsc2['return'].merge(existing)
            grouped = ts.group_serie(tsc22['sampling'])
            existing = tsc22.get('return')
            tsc22['return'] = carbonara.AggregatedTimeSerie.from_grouped_serie(
                grouped, tsc22['sampling'], tsc22['agg'],
                max_size=tsc22['size'])
            if existing:
                tsc22['return'].merge(existing)

        tsb1.set_values([
            (datetime.datetime(2014, 1, 1, 11, 46, 4), 4),
            (datetime.datetime(2014, 1, 1, 11, 47, 34), 8),
            (datetime.datetime(2014, 1, 1, 11, 50, 54), 50),
            (datetime.datetime(2014, 1, 1, 11, 54, 45), 4),
            (datetime.datetime(2014, 1, 1, 11, 56, 49), 4),
            (datetime.datetime(2014, 1, 1, 11, 57, 22), 6),
            (datetime.datetime(2014, 1, 1, 11, 58, 22), 5),
            (datetime.datetime(2014, 1, 1, 12, 1, 4), 4),
            (datetime.datetime(2014, 1, 1, 12, 1, 9), 7),
            (datetime.datetime(2014, 1, 1, 12, 2, 1), 15),
            (datetime.datetime(2014, 1, 1, 12, 2, 12), 1),
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 3),
            (datetime.datetime(2014, 1, 1, 12, 4, 9), 7),
            (datetime.datetime(2014, 1, 1, 12, 5, 1), 15),
            (datetime.datetime(2014, 1, 1, 12, 5, 12), 1),
            (datetime.datetime(2014, 1, 1, 12, 6, 0), 3),
        ], before_truncate_callback=ts1_update)

        tsb2.set_values([
            (datetime.datetime(2014, 1, 1, 11, 46, 4), 6),
            (datetime.datetime(2014, 1, 1, 11, 47, 34), 5),
            (datetime.datetime(2014, 1, 1, 11, 50, 54), 51),
            (datetime.datetime(2014, 1, 1, 11, 54, 45), 5),
            (datetime.datetime(2014, 1, 1, 11, 56, 49), 5),
            (datetime.datetime(2014, 1, 1, 11, 57, 22), 7),
            (datetime.datetime(2014, 1, 1, 11, 58, 22), 5),
            (datetime.datetime(2014, 1, 1, 12, 1, 4), 5),
            (datetime.datetime(2014, 1, 1, 12, 1, 9), 8),
            (datetime.datetime(2014, 1, 1, 12, 2, 1), 10),
            (datetime.datetime(2014, 1, 1, 12, 2, 12), 2),
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 6),
            (datetime.datetime(2014, 1, 1, 12, 4, 9), 4),
            (datetime.datetime(2014, 1, 1, 12, 5, 1), 10),
            (datetime.datetime(2014, 1, 1, 12, 5, 12), 1),
            (datetime.datetime(2014, 1, 1, 12, 6, 0), 1),
        ], before_truncate_callback=ts2_update)

        output = carbonara.AggregatedTimeSerie.aggregated(
            [tsc1['return'], tsc12['return'], tsc2['return'], tsc22['return']],
            'mean')
        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 11, 45), 300.0, 5.75),
            (datetime.datetime(2014, 1, 1, 11, 50), 300.0, 27.5),
            (datetime.datetime(2014, 1, 1, 11, 55), 300.0, 5.3333333333333339),
            (datetime.datetime(2014, 1, 1, 12, 0), 300.0, 6.0),
            (datetime.datetime(2014, 1, 1, 12, 5), 300.0, 5.1666666666666661),
            (datetime.datetime(2014, 1, 1, 11, 54), 60.0, 4.5),
            (datetime.datetime(2014, 1, 1, 11, 56), 60.0, 4.5),
            (datetime.datetime(2014, 1, 1, 11, 57), 60.0, 6.5),
            (datetime.datetime(2014, 1, 1, 11, 58), 60.0, 5.0),
            (datetime.datetime(2014, 1, 1, 12, 1), 60.0, 6.0),
            (datetime.datetime(2014, 1, 1, 12, 2), 60.0, 7.0),
            (datetime.datetime(2014, 1, 1, 12, 3), 60.0, 4.5),
            (datetime.datetime(2014, 1, 1, 12, 4), 60.0, 5.5),
            (datetime.datetime(2014, 1, 1, 12, 5), 60.0, 6.75),
            (datetime.datetime(2014, 1, 1, 12, 6), 60.0, 2.0),
        ], list(output))

    def test_aggregated_partial_overlap(self):
        tsc1 = {'sampling': 1, 'size': 86400, 'agg': 'mean'}
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1['sampling'])
        tsc2 = {'sampling': 1, 'size': 60, 'agg': 'mean'}
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2['sampling'])

        tsb1.set_values([
            (datetime.datetime(2015, 12, 3, 13, 19, 15), 1),
            (datetime.datetime(2015, 12, 3, 13, 20, 15), 1),
            (datetime.datetime(2015, 12, 3, 13, 21, 15), 1),
            (datetime.datetime(2015, 12, 3, 13, 22, 15), 1),
        ], before_truncate_callback=functools.partial(
            self._resample_and_merge, agg_dict=tsc1))

        tsb2.set_values([
            (datetime.datetime(2015, 12, 3, 13, 21, 15), 10),
            (datetime.datetime(2015, 12, 3, 13, 22, 15), 10),
            (datetime.datetime(2015, 12, 3, 13, 23, 15), 10),
            (datetime.datetime(2015, 12, 3, 13, 24, 15), 10),
        ], before_truncate_callback=functools.partial(
            self._resample_and_merge, agg_dict=tsc2))

        output = carbonara.AggregatedTimeSerie.aggregated(
            [tsc1['return'], tsc2['return']], aggregation="sum")

        self.assertEqual([
            (datetime.datetime(
                2015, 12, 3, 13, 21, 15
            ), 1.0, 11.0),
            (datetime.datetime(
                2015, 12, 3, 13, 22, 15
            ), 1.0, 11.0),
        ], list(output))

        dtfrom = datetime.datetime(2015, 12, 3, 13, 17, 0)
        dtto = datetime.datetime(2015, 12, 3, 13, 25, 0)

        output = carbonara.AggregatedTimeSerie.aggregated(
            [tsc1['return'], tsc2['return']],
            from_timestamp=dtfrom, to_timestamp=dtto,
            aggregation="sum", needed_percent_of_overlap=0)

        self.assertEqual([
            (datetime.datetime(
                2015, 12, 3, 13, 19, 15
            ), 1.0, 1.0),
            (datetime.datetime(
                2015, 12, 3, 13, 20, 15
            ), 1.0, 1.0),
            (datetime.datetime(
                2015, 12, 3, 13, 21, 15
            ), 1.0, 11.0),
            (datetime.datetime(
                2015, 12, 3, 13, 22, 15
            ), 1.0, 11.0),
            (datetime.datetime(
                2015, 12, 3, 13, 23, 15
            ), 1.0, 10.0),
            (datetime.datetime(
                2015, 12, 3, 13, 24, 15
            ), 1.0, 10.0),
        ], list(output))

        # By default we require 100% of point that overlap
        # so that fail if from or to is set
        self.assertRaises(carbonara.UnAggregableTimeseries,
                          carbonara.AggregatedTimeSerie.aggregated,
                          [tsc1['return'], tsc2['return']],
                          to_timestamp=dtto, aggregation='mean')
        self.assertRaises(carbonara.UnAggregableTimeseries,
                          carbonara.AggregatedTimeSerie.aggregated,
                          [tsc1['return'], tsc2['return']],
                          from_timestamp=dtfrom, aggregation='mean')

        # Retry with 50% and it works
        output = carbonara.AggregatedTimeSerie.aggregated(
            [tsc1['return'], tsc2['return']], from_timestamp=dtfrom,
            aggregation="sum",
            needed_percent_of_overlap=50.0)
        self.assertEqual([
            (datetime.datetime(
                2015, 12, 3, 13, 19, 15
            ), 1.0, 1.0),
            (datetime.datetime(
                2015, 12, 3, 13, 20, 15
            ), 1.0, 1.0),
            (datetime.datetime(
                2015, 12, 3, 13, 21, 15
            ), 1.0, 11.0),
            (datetime.datetime(
                2015, 12, 3, 13, 22, 15
            ), 1.0, 11.0),
        ], list(output))

        output = carbonara.AggregatedTimeSerie.aggregated(
            [tsc1['return'], tsc2['return']], to_timestamp=dtto,
            aggregation="sum",
            needed_percent_of_overlap=50.0)
        self.assertEqual([
            (datetime.datetime(
                2015, 12, 3, 13, 21, 15
            ), 1.0, 11.0),
            (datetime.datetime(
                2015, 12, 3, 13, 22, 15
            ), 1.0, 11.0),
            (datetime.datetime(
                2015, 12, 3, 13, 23, 15
            ), 1.0, 10.0),
            (datetime.datetime(
                2015, 12, 3, 13, 24, 15
            ), 1.0, 10.0),
        ], list(output))

    def test_split_key(self):
        self.assertEqual(
            datetime.datetime(2014, 10, 7),
            carbonara.SplitKey.from_timestamp_and_sampling(
                datetime.datetime(2015, 1, 1, 15, 3), 3600).as_datetime())
        self.assertEqual(
            datetime.datetime(2014, 12, 31, 18),
            carbonara.SplitKey.from_timestamp_and_sampling(
                datetime.datetime(2015, 1, 1, 15, 3), 58).as_datetime())
        self.assertEqual(
            1420048800.0,
            float(carbonara.SplitKey.from_timestamp_and_sampling(
                datetime.datetime(2015, 1, 1, 15, 3), 58)))

        key = carbonara.SplitKey.from_timestamp_and_sampling(
            datetime.datetime(2015, 1, 1, 15, 3), 3600)

        self.assertGreater(key, pandas.Timestamp(0))

        self.assertGreaterEqual(key, pandas.Timestamp(0))

    def test_split_key_next(self):
        self.assertEqual(
            datetime.datetime(2015, 3, 6),
            next(carbonara.SplitKey.from_timestamp_and_sampling(
                datetime.datetime(2015, 1, 1, 15, 3), 3600)).as_datetime())
        self.assertEqual(
            datetime.datetime(2015, 8, 3),
            next(next(carbonara.SplitKey.from_timestamp_and_sampling(
                datetime.datetime(2015, 1, 1, 15, 3), 3600))).as_datetime())
        self.assertEqual(
            113529600000.0,
            float(next(carbonara.SplitKey.from_timestamp_and_sampling(
                datetime.datetime(2015, 1, 1, 15, 3), 3600 * 24 * 365))))

    def test_split(self):
        sampling = 5
        points = 100000
        ts = carbonara.TimeSerie.from_data(
            timestamps=map(datetime.datetime.utcfromtimestamp,
                           six.moves.range(points)),
            values=six.moves.range(points))
        agg = self._resample(ts, sampling, 'mean')

        grouped_points = list(agg.split())

        self.assertEqual(
            math.ceil((points / float(sampling))
                      / carbonara.SplitKey.POINTS_PER_SPLIT),
            len(grouped_points))
        self.assertEqual("0.0",
                         str(carbonara.SplitKey(grouped_points[0][0], 0)))
        # 3600 × 5s = 5 hours
        self.assertEqual(datetime.datetime(1970, 1, 1, 5),
                         grouped_points[1][0].as_datetime())
        self.assertEqual(carbonara.SplitKey.POINTS_PER_SPLIT,
                         len(grouped_points[0][1]))

    def test_from_timeseries(self):
        sampling = 5
        points = 100000
        ts = carbonara.TimeSerie.from_data(
            timestamps=map(datetime.datetime.utcfromtimestamp,
                           six.moves.range(points)),
            values=six.moves.range(points))
        agg = self._resample(ts, sampling, 'mean')

        split = [t[1] for t in list(agg.split())]

        self.assertEqual(agg,
                         carbonara.AggregatedTimeSerie.from_timeseries(
                             split,
                             sampling=agg.sampling,
                             max_size=agg.max_size,
                             aggregation_method=agg.aggregation_method))

    def test_resample(self):
        ts = carbonara.TimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9),
             datetime.datetime(2014, 1, 1, 12, 0, 11),
             datetime.datetime(2014, 1, 1, 12, 0, 12)],
            [3, 5, 6, 2, 4])
        agg_ts = self._resample(ts, 5, 'mean')
        self.assertEqual(3, len(agg_ts))

        agg_ts = agg_ts.resample(10)
        self.assertEqual(2, len(agg_ts))
        self.assertEqual(5, agg_ts[0])
        self.assertEqual(3, agg_ts[1])
