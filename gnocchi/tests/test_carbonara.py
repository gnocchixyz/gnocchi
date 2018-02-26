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
import operator

import fixtures
import iso8601
import numpy
import six

from gnocchi import carbonara
from gnocchi.tests import base


def datetime64(*args):
    return numpy.datetime64(datetime.datetime(*args))


class TestBoundTimeSerie(base.BaseTestCase):
    def test_benchmark(self):
        self.useFixture(fixtures.Timeout(300, gentle=True))
        carbonara.BoundTimeSerie.benchmark()

    @staticmethod
    def test_base():
        carbonara.BoundTimeSerie.from_data(
            [datetime64(2014, 1, 1, 12, 0, 0),
             datetime64(2014, 1, 1, 12, 0, 4),
             datetime64(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6])

    def test_block_size(self):
        ts = carbonara.BoundTimeSerie.from_data(
            [datetime64(2014, 1, 1, 12, 0, 5),
             datetime64(2014, 1, 1, 12, 0, 9)],
            [5, 6],
            block_size=numpy.timedelta64(5, 's'))
        self.assertEqual(2, len(ts))
        ts.set_values(numpy.array([(datetime64(2014, 1, 1, 12, 0, 10), 3),
                                   (datetime64(2014, 1, 1, 12, 0, 11), 4)],
                                  dtype=carbonara.TIMESERIES_ARRAY_DTYPE))
        self.assertEqual(2, len(ts))

    def test_block_size_back_window(self):
        ts = carbonara.BoundTimeSerie.from_data(
            [datetime64(2014, 1, 1, 12, 0, 0),
             datetime64(2014, 1, 1, 12, 0, 4),
             datetime64(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6],
            block_size=numpy.timedelta64(5, 's'),
            back_window=1)
        self.assertEqual(3, len(ts))
        ts.set_values(numpy.array([(datetime64(2014, 1, 1, 12, 0, 10), 3),
                                   (datetime64(2014, 1, 1, 12, 0, 11), 4)],
                                  dtype=carbonara.TIMESERIES_ARRAY_DTYPE))
        self.assertEqual(3, len(ts))

    def test_block_size_unordered(self):
        ts = carbonara.BoundTimeSerie.from_data(
            [datetime64(2014, 1, 1, 12, 0, 5),
             datetime64(2014, 1, 1, 12, 0, 9)],
            [5, 23],
            block_size=numpy.timedelta64(5, 's'))
        self.assertEqual(2, len(ts))
        ts.set_values(numpy.array([(datetime64(2014, 1, 1, 12, 0, 11), 3),
                                   (datetime64(2014, 1, 1, 12, 0, 10), 4)],
                                  dtype=carbonara.TIMESERIES_ARRAY_DTYPE))
        self.assertEqual(2, len(ts))

    def test_duplicate_timestamps(self):
        ts = carbonara.BoundTimeSerie.from_data(
            [datetime64(2014, 1, 1, 12, 0, 0),
             datetime64(2014, 1, 1, 12, 0, 9)],
            [10, 23])
        self.assertEqual(2, len(ts))
        self.assertEqual(10.0, ts[0][1])
        self.assertEqual(23.0, ts[1][1])

        ts.set_values(numpy.array([(datetime64(2014, 1, 1, 13, 0, 10), 3),
                                   (datetime64(2014, 1, 1, 13, 0, 11), 9),
                                   (datetime64(2014, 1, 1, 13, 0, 11), 8),
                                   (datetime64(2014, 1, 1, 13, 0, 11), 7),
                                   (datetime64(2014, 1, 1, 13, 0, 11), 4)],
                                  dtype=carbonara.TIMESERIES_ARRAY_DTYPE))
        self.assertEqual(4, len(ts))
        self.assertEqual(10.0, ts[0][1])
        self.assertEqual(23.0, ts[1][1])
        self.assertEqual(3.0, ts[2][1])
        self.assertEqual(9.0, ts[3][1])


class TestAggregatedTimeSerie(base.BaseTestCase):
    def test_benchmark(self):
        self.useFixture(fixtures.Timeout(300, gentle=True))
        carbonara.AggregatedTimeSerie.benchmark()

    def test_fetch_basic(self):
        ts = carbonara.AggregatedTimeSerie.from_data(
            timestamps=[datetime64(2014, 1, 1, 12, 0, 0),
                        datetime64(2014, 1, 1, 12, 0, 4),
                        datetime64(2014, 1, 1, 12, 0, 9)],
            values=[3, 5, 6],
            aggregation=carbonara.Aggregation(
                "mean", numpy.timedelta64(1, 's'), None))
        self.assertEqual(
            [(datetime64(2014, 1, 1, 12), 3),
             (datetime64(2014, 1, 1, 12, 0, 4), 5),
             (datetime64(2014, 1, 1, 12, 0, 9), 6)],
            list(ts.fetch()))
        self.assertEqual(
            [(datetime64(2014, 1, 1, 12, 0, 4), 5),
             (datetime64(2014, 1, 1, 12, 0, 9), 6)],
            list(ts.fetch(
                from_timestamp=datetime64(2014, 1, 1, 12, 0, 4))))
        self.assertEqual(
            [(datetime64(2014, 1, 1, 12, 0, 4), 5),
             (datetime64(2014, 1, 1, 12, 0, 9), 6)],
            list(ts.fetch(
                from_timestamp=numpy.datetime64(iso8601.parse_date(
                    "2014-01-01 12:00:04")))))
        self.assertEqual(
            [(datetime64(2014, 1, 1, 12, 0, 4), 5),
             (datetime64(2014, 1, 1, 12, 0, 9), 6)],
            list(ts.fetch(
                from_timestamp=numpy.datetime64(iso8601.parse_date(
                    "2014-01-01 13:00:04+01:00")))))

    def test_before_epoch(self):
        ts = carbonara.TimeSerie.from_data(
            [datetime64(1950, 1, 1, 12),
             datetime64(2014, 1, 1, 12),
             datetime64(2014, 1, 1, 12)],
            [3, 5, 6])

        self.assertRaises(carbonara.BeforeEpochError,
                          ts.group_serie, 60)

    @staticmethod
    def _resample(ts, sampling, agg, derived=False):
        aggregation = carbonara.Aggregation(agg, sampling, None)
        grouped = ts.group_serie(sampling)
        if derived:
            grouped = grouped.derived()
        return carbonara.AggregatedTimeSerie.from_grouped_serie(
            grouped, aggregation)

    def test_derived_mean(self):
        ts = carbonara.TimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 1, 2),
             datetime.datetime(2014, 1, 1, 12, 1, 14),
             datetime.datetime(2014, 1, 1, 12, 1, 24),
             datetime.datetime(2014, 1, 1, 12, 2, 4),
             datetime.datetime(2014, 1, 1, 12, 2, 35),
             datetime.datetime(2014, 1, 1, 12, 2, 42),
             datetime.datetime(2014, 1, 1, 12, 3, 2),
             datetime.datetime(2014, 1, 1, 12, 3, 22),  # Counter reset
             datetime.datetime(2014, 1, 1, 12, 3, 42),
             datetime.datetime(2014, 1, 1, 12, 4, 9)],
            [50, 55, 65, 66, 70, 83, 92, 103, 105, 5, 7, 23])
        ts = self._resample(ts, numpy.timedelta64(60, 's'), 'mean',
                            derived=True)

        self.assertEqual(5, len(ts))
        self.assertEqual(
            [(datetime64(2014, 1, 1, 12, 0, 0), 5),
             (datetime64(2014, 1, 1, 12, 1, 0), 5),
             (datetime64(2014, 1, 1, 12, 2, 0), 11),
             (datetime64(2014, 1, 1, 12, 3, 0), -32),
             (datetime64(2014, 1, 1, 12, 4, 0), 16)],
            list(ts.fetch(
                from_timestamp=datetime64(2014, 1, 1, 12))))

    def test_derived_hole(self):
        ts = carbonara.TimeSerie.from_data(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 1, 2),
             datetime.datetime(2014, 1, 1, 12, 1, 14),
             datetime.datetime(2014, 1, 1, 12, 1, 24),
             datetime.datetime(2014, 1, 1, 12, 3, 2),
             datetime.datetime(2014, 1, 1, 12, 3, 22),
             datetime.datetime(2014, 1, 1, 12, 3, 42),
             datetime.datetime(2014, 1, 1, 12, 4, 9)],
            [50, 55, 65, 66, 70, 105, 108, 200, 202])
        ts = self._resample(ts, numpy.timedelta64(60, 's'), 'last',
                            derived=True)

        self.assertEqual(4, len(ts))
        self.assertEqual(
            [(datetime64(2014, 1, 1, 12, 0, 0), 5),
             (datetime64(2014, 1, 1, 12, 1, 0), 4),
             (datetime64(2014, 1, 1, 12, 3, 0), 92),
             (datetime64(2014, 1, 1, 12, 4, 0), 2)],
            list(ts.fetch(
                from_timestamp=datetime64(2014, 1, 1, 12))))

    def test_74_percentile_serialized(self):
        ts = carbonara.TimeSerie.from_data(
            [datetime64(2014, 1, 1, 12, 0, 0),
             datetime64(2014, 1, 1, 12, 0, 4),
             datetime64(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6])
        ts = self._resample(ts, numpy.timedelta64(60, 's'), '74pct')

        self.assertEqual(1, len(ts))
        self.assertEqual(5.48, ts[datetime64(2014, 1, 1, 12, 0, 0)][1])

        # Serialize and unserialize
        key = ts.get_split_key()
        o, s = ts.serialize(key)
        saved_ts = carbonara.AggregatedTimeSerie.unserialize(
            s, key, ts.aggregation)

        self.assertEqual(ts.aggregation, saved_ts.aggregation)

        ts = carbonara.TimeSerie.from_data(
            [datetime64(2014, 1, 1, 12, 0, 0),
             datetime64(2014, 1, 1, 12, 0, 4),
             datetime64(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6])
        ts = self._resample(ts, numpy.timedelta64(60, 's'), '74pct')
        saved_ts.merge(ts)

        self.assertEqual(1, len(ts))
        self.assertEqual(5.48, ts[datetime64(2014, 1, 1, 12, 0, 0)][1])

    def test_95_percentile(self):
        ts = carbonara.TimeSerie.from_data(
            [datetime64(2014, 1, 1, 12, 0, 0),
             datetime64(2014, 1, 1, 12, 0, 4),
             datetime64(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6])
        ts = self._resample(ts, numpy.timedelta64(60, 's'), '95pct')

        self.assertEqual(1, len(ts))
        self.assertEqual(5.9000000000000004,
                         ts[datetime64(2014, 1, 1, 12, 0, 0)][1])

    def _do_test_aggregation(self, name, v1, v2, v3):
        # NOTE(gordc): test data must have a group of odd count to properly
        # test 50pct test case.
        ts = carbonara.TimeSerie.from_data(
            [datetime64(2014, 1, 1, 12, 0, 0),
             datetime64(2014, 1, 1, 12, 0, 10),
             datetime64(2014, 1, 1, 12, 0, 20),
             datetime64(2014, 1, 1, 12, 0, 30),
             datetime64(2014, 1, 1, 12, 0, 40),
             datetime64(2014, 1, 1, 12, 1, 0),
             datetime64(2014, 1, 1, 12, 1, 10),
             datetime64(2014, 1, 1, 12, 1, 20),
             datetime64(2014, 1, 1, 12, 1, 30),
             datetime64(2014, 1, 1, 12, 1, 40),
             datetime64(2014, 1, 1, 12, 1, 50),
             datetime64(2014, 1, 1, 12, 2, 0),
             datetime64(2014, 1, 1, 12, 2, 10)],
            [3, 5, 2, 3, 5, 8, 11, 22, 10, 42, 9, 4, 2])
        ts = self._resample(ts, numpy.timedelta64(60, 's'), name)

        self.assertEqual(3, len(ts))
        self.assertEqual(v1, ts[datetime64(2014, 1, 1, 12, 0, 0)][1])
        self.assertEqual(v2, ts[datetime64(2014, 1, 1, 12, 1, 0)][1])
        self.assertEqual(v3, ts[datetime64(2014, 1, 1, 12, 2, 0)][1])

    def test_aggregation_first(self):
        self._do_test_aggregation('first', 3, 8, 4)

    def test_aggregation_last(self):
        self._do_test_aggregation('last', 5, 9, 2)

    def test_aggregation_count(self):
        self._do_test_aggregation('count', 5, 6, 2)

    def test_aggregation_sum(self):
        self._do_test_aggregation('sum', 18, 102, 6)

    def test_aggregation_mean(self):
        self._do_test_aggregation('mean', 3.6, 17, 3)

    def test_aggregation_median(self):
        self._do_test_aggregation('median', 3.0, 10.5, 3)

    def test_aggregation_50pct(self):
        self._do_test_aggregation('50pct', 3.0, 10.5, 3)

    def test_aggregation_56pct(self):
        self._do_test_aggregation('56pct', 3.4800000000000004,
                                  10.8, 3.120000000000001)

    def test_aggregation_min(self):
        self._do_test_aggregation('min', 2, 8, 2)

    def test_aggregation_max(self):
        self._do_test_aggregation('max', 5, 42, 4)

    def test_aggregation_std(self):
        self._do_test_aggregation('std', 1.3416407864998738,
                                  13.266499161421599, 1.4142135623730951)

    def test_aggregation_std_with_unique(self):
        ts = carbonara.TimeSerie.from_data(
            [datetime64(2014, 1, 1, 12, 0, 0)], [3])
        ts = self._resample(ts, numpy.timedelta64(60, 's'), 'std')
        self.assertEqual(0, len(ts), ts.values)

        ts = carbonara.TimeSerie.from_data(
            [datetime64(2014, 1, 1, 12, 0, 0),
             datetime64(2014, 1, 1, 12, 0, 4),
             datetime64(2014, 1, 1, 12, 0, 9),
             datetime64(2014, 1, 1, 12, 1, 6)],
            [3, 6, 5, 9])
        ts = self._resample(ts, numpy.timedelta64(60, 's'), "std")

        self.assertEqual(1, len(ts))
        self.assertEqual(1.5275252316519465,
                         ts[datetime64(2014, 1, 1, 12, 0, 0)][1])

    def test_different_length_in_timestamps_and_data(self):
        self.assertRaises(
            ValueError,
            carbonara.AggregatedTimeSerie.from_data,
            carbonara.Aggregation('mean', numpy.timedelta64(3, 's'), None),
            [datetime64(2014, 1, 1, 12, 0, 0),
             datetime64(2014, 1, 1, 12, 0, 4),
             datetime64(2014, 1, 1, 12, 0, 9)],
            [3, 5])

    def test_truncate(self):
        ts = carbonara.TimeSerie.from_data(
            [datetime64(2014, 1, 1, 12, 0, 0),
             datetime64(2014, 1, 1, 12, 0, 4),
             datetime64(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6])
        ts = self._resample(ts, numpy.timedelta64(1, 's'), 'mean')

        ts.truncate(datetime64(2014, 1, 1, 12, 0, 0))

        self.assertEqual(2, len(ts))
        self.assertEqual(5, ts[0][1])
        self.assertEqual(6, ts[1][1])

    def test_down_sampling(self):
        ts = carbonara.TimeSerie.from_data(
            [datetime64(2014, 1, 1, 12, 0, 0),
             datetime64(2014, 1, 1, 12, 0, 4),
             datetime64(2014, 1, 1, 12, 0, 9)],
            [3, 5, 7])
        ts = self._resample(ts, numpy.timedelta64(300, 's'), 'mean')

        self.assertEqual(1, len(ts))
        self.assertEqual(5, ts[datetime64(2014, 1, 1, 12, 0, 0)][1])

    def test_down_sampling_and_truncate(self):
        ts = carbonara.TimeSerie.from_data(
            [datetime64(2014, 1, 1, 12, 0, 0),
             datetime64(2014, 1, 1, 12, 1, 4),
             datetime64(2014, 1, 1, 12, 1, 9),
             datetime64(2014, 1, 1, 12, 2, 12)],
            [3, 5, 7, 1])
        ts = self._resample(ts, numpy.timedelta64(60, 's'), 'mean')

        ts.truncate(datetime64(2014, 1, 1, 12, 0, 59))

        self.assertEqual(2, len(ts))
        self.assertEqual(6, ts[datetime64(2014, 1, 1, 12, 1, 0)][1])
        self.assertEqual(1, ts[datetime64(2014, 1, 1, 12, 2, 0)][1])

    def test_down_sampling_and_truncate_and_method_max(self):
        ts = carbonara.TimeSerie.from_data(
            [datetime64(2014, 1, 1, 12, 0, 0),
             datetime64(2014, 1, 1, 12, 1, 4),
             datetime64(2014, 1, 1, 12, 1, 9),
             datetime64(2014, 1, 1, 12, 2, 12)],
            [3, 5, 70, 1])
        ts = self._resample(ts, numpy.timedelta64(60, 's'), 'max')

        ts.truncate(datetime64(2014, 1, 1, 12, 0, 59))

        self.assertEqual(2, len(ts))
        self.assertEqual(70, ts[datetime64(2014, 1, 1, 12, 1, 0)][1])
        self.assertEqual(1, ts[datetime64(2014, 1, 1, 12, 2, 0)][1])

    @staticmethod
    def _resample_and_merge(ts, agg_dict):
        """Helper method that mimics _compute_splits_operations workflow."""
        grouped = ts.group_serie(agg_dict['sampling'])
        existing = agg_dict.get('return')
        agg_dict['return'] = carbonara.AggregatedTimeSerie.from_grouped_serie(
            grouped, carbonara.Aggregation(
                agg_dict['agg'], agg_dict['sampling'], None))
        if existing:
            existing.merge(agg_dict['return'])
            agg_dict['return'] = existing

    def test_fetch(self):
        ts = {'sampling': numpy.timedelta64(60, 's'),
              'size': 10, 'agg': 'mean'}
        tsb = carbonara.BoundTimeSerie(block_size=ts['sampling'])

        tsb.set_values(numpy.array([
            (datetime64(2014, 1, 1, 11, 46, 4), 4),
            (datetime64(2014, 1, 1, 11, 47, 34), 8),
            (datetime64(2014, 1, 1, 11, 50, 54), 50),
            (datetime64(2014, 1, 1, 11, 54, 45), 4),
            (datetime64(2014, 1, 1, 11, 56, 49), 4),
            (datetime64(2014, 1, 1, 11, 57, 22), 6),
            (datetime64(2014, 1, 1, 11, 58, 22), 5),
            (datetime64(2014, 1, 1, 12, 1, 4), 4),
            (datetime64(2014, 1, 1, 12, 1, 9), 7),
            (datetime64(2014, 1, 1, 12, 2, 1), 15),
            (datetime64(2014, 1, 1, 12, 2, 12), 1),
            (datetime64(2014, 1, 1, 12, 3, 0), 3),
            (datetime64(2014, 1, 1, 12, 4, 9), 7),
            (datetime64(2014, 1, 1, 12, 5, 1), 15),
            (datetime64(2014, 1, 1, 12, 5, 12), 1),
            (datetime64(2014, 1, 1, 12, 6, 0, 2), 3)],
            dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
            before_truncate_callback=functools.partial(
                self._resample_and_merge, agg_dict=ts))

        tsb.set_values(numpy.array([(datetime64(2014, 1, 1, 12, 6), 5)],
                                   dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
                       before_truncate_callback=functools.partial(
                           self._resample_and_merge, agg_dict=ts))

        self.assertEqual([
            (numpy.datetime64('2014-01-01T11:46:00.000000000'), 4.0),
            (numpy.datetime64('2014-01-01T11:47:00.000000000'), 8.0),
            (numpy.datetime64('2014-01-01T11:50:00.000000000'), 50.0),
            (datetime64(2014, 1, 1, 11, 54), 4.0),
            (datetime64(2014, 1, 1, 11, 56), 4.0),
            (datetime64(2014, 1, 1, 11, 57), 6.0),
            (datetime64(2014, 1, 1, 11, 58), 5.0),
            (datetime64(2014, 1, 1, 12, 1), 5.5),
            (datetime64(2014, 1, 1, 12, 2), 8.0),
            (datetime64(2014, 1, 1, 12, 3), 3.0),
            (datetime64(2014, 1, 1, 12, 4), 7.0),
            (datetime64(2014, 1, 1, 12, 5), 8.0),
            (datetime64(2014, 1, 1, 12, 6), 4.0)
        ], list(ts['return'].fetch()))

        self.assertEqual([
            (datetime64(2014, 1, 1, 12, 1), 5.5),
            (datetime64(2014, 1, 1, 12, 2), 8.0),
            (datetime64(2014, 1, 1, 12, 3), 3.0),
            (datetime64(2014, 1, 1, 12, 4), 7.0),
            (datetime64(2014, 1, 1, 12, 5), 8.0),
            (datetime64(2014, 1, 1, 12, 6), 4.0)
        ], list(ts['return'].fetch(datetime64(2014, 1, 1, 12, 0, 0))))

    def test_fetch_agg_pct(self):
        ts = {'sampling': numpy.timedelta64(1, 's'),
              'size': 3600 * 24, 'agg': '90pct'}
        tsb = carbonara.BoundTimeSerie(block_size=ts['sampling'])

        tsb.set_values(numpy.array([(datetime64(2014, 1, 1, 12, 0, 0), 3),
                                    (datetime64(2014, 1, 1, 12, 0, 0, 123), 4),
                                    (datetime64(2014, 1, 1, 12, 0, 2), 4)],
                                   dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
                       before_truncate_callback=functools.partial(
                           self._resample_and_merge, agg_dict=ts))

        result = ts['return'].fetch(datetime64(2014, 1, 1, 12, 0, 0))
        reference = [
            (datetime64(
                2014, 1, 1, 12, 0, 0
            ), 3.9),
            (datetime64(
                2014, 1, 1, 12, 0, 2
            ), 4)
        ]

        self.assertEqual(len(reference), len(list(result)))

        for ref, res in zip(reference, result):
            self.assertEqual(ref[0], res[0])
            # Rounding \o/
            self.assertAlmostEqual(ref[1], res[1])

        tsb.set_values(numpy.array([
            (datetime64(2014, 1, 1, 12, 0, 2, 113), 110)],
            dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
            before_truncate_callback=functools.partial(
                self._resample_and_merge, agg_dict=ts))

        result = ts['return'].fetch(datetime64(2014, 1, 1, 12, 0, 0))
        reference = [
            (datetime64(
                2014, 1, 1, 12, 0, 0
            ), 3.9),
            (datetime64(
                2014, 1, 1, 12, 0, 2
            ), 99.4)
        ]

        self.assertEqual(len(reference), len(list(result)))

        for ref, res in zip(reference, result):
            self.assertEqual(ref[0], res[0])
            # Rounding \o/
            self.assertAlmostEqual(ref[1], res[1])

    def test_fetch_nano(self):
        ts = {'sampling': numpy.timedelta64(200, 'ms'),
              'size': 10, 'agg': 'mean'}
        tsb = carbonara.BoundTimeSerie(block_size=ts['sampling'])

        tsb.set_values(numpy.array([
            (datetime64(2014, 1, 1, 11, 46, 0, 200123), 4),
            (datetime64(2014, 1, 1, 11, 46, 0, 340000), 8),
            (datetime64(2014, 1, 1, 11, 47, 0, 323154), 50),
            (datetime64(2014, 1, 1, 11, 48, 0, 590903), 4),
            (datetime64(2014, 1, 1, 11, 48, 0, 903291), 4)],
            dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
            before_truncate_callback=functools.partial(
                self._resample_and_merge, agg_dict=ts))

        tsb.set_values(numpy.array([
            (datetime64(2014, 1, 1, 11, 48, 0, 821312), 5)],
            dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
            before_truncate_callback=functools.partial(
                self._resample_and_merge, agg_dict=ts))

        self.assertEqual([
            (datetime64(2014, 1, 1, 11, 46, 0, 200000), 6.0),
            (datetime64(2014, 1, 1, 11, 47, 0, 200000), 50.0),
            (datetime64(2014, 1, 1, 11, 48, 0, 400000), 4.0),
            (datetime64(2014, 1, 1, 11, 48, 0, 800000), 4.5)
        ], list(ts['return'].fetch()))
        self.assertEqual(numpy.timedelta64(200000000, 'ns'),
                         ts['return'].aggregation.granularity)

    def test_fetch_agg_std(self):
        # NOTE (gordc): this is a good test to ensure we drop NaN entries
        # 2014-01-01 12:00:00 will appear if we don't dropna()
        ts = {'sampling': numpy.timedelta64(60, 's'),
              'size': 60, 'agg': 'std'}
        tsb = carbonara.BoundTimeSerie(block_size=ts['sampling'])

        tsb.set_values(numpy.array([(datetime64(2014, 1, 1, 12, 0, 0), 3),
                                    (datetime64(2014, 1, 1, 12, 1, 4), 4),
                                    (datetime64(2014, 1, 1, 12, 1, 9), 7),
                                    (datetime64(2014, 1, 1, 12, 2, 1), 15),
                                    (datetime64(2014, 1, 1, 12, 2, 12), 1)],
                                   dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
                       before_truncate_callback=functools.partial(
                           self._resample_and_merge, agg_dict=ts))

        self.assertEqual([
            (datetime64(2014, 1, 1, 12, 1, 0), 2.1213203435596424),
            (datetime64(2014, 1, 1, 12, 2, 0), 9.8994949366116654),
        ], list(ts['return'].fetch(datetime64(2014, 1, 1, 12, 0, 0))))

        tsb.set_values(numpy.array([(datetime64(2014, 1, 1, 12, 2, 13), 110)],
                                   dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
                       before_truncate_callback=functools.partial(
                           self._resample_and_merge, agg_dict=ts))

        self.assertEqual([
            (datetime64(2014, 1, 1, 12, 1, 0), 2.1213203435596424),
            (datetime64(2014, 1, 1, 12, 2, 0), 59.304300012730948),
        ], list(ts['return'].fetch(datetime64(2014, 1, 1, 12, 0, 0))))

    def test_fetch_agg_max(self):
        ts = {'sampling': numpy.timedelta64(60, 's'),
              'size': 60, 'agg': 'max'}
        tsb = carbonara.BoundTimeSerie(block_size=ts['sampling'])

        tsb.set_values(numpy.array([(datetime64(2014, 1, 1, 12, 0, 0), 3),
                                    (datetime64(2014, 1, 1, 12, 1, 4), 4),
                                    (datetime64(2014, 1, 1, 12, 1, 9), 7),
                                    (datetime64(2014, 1, 1, 12, 2, 1), 15),
                                    (datetime64(2014, 1, 1, 12, 2, 12), 1)],
                                   dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
                       before_truncate_callback=functools.partial(
                           self._resample_and_merge, agg_dict=ts))

        self.assertEqual([
            (datetime64(2014, 1, 1, 12, 0, 0), 3),
            (datetime64(2014, 1, 1, 12, 1, 0), 7),
            (datetime64(2014, 1, 1, 12, 2, 0), 15),
        ], list(ts['return'].fetch(datetime64(2014, 1, 1, 12, 0, 0))))

        tsb.set_values(numpy.array([(datetime64(2014, 1, 1, 12, 2, 13), 110)],
                                   dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
                       before_truncate_callback=functools.partial(
                           self._resample_and_merge, agg_dict=ts))

        self.assertEqual([
            (datetime64(2014, 1, 1, 12, 0, 0), 3),
            (datetime64(2014, 1, 1, 12, 1, 0), 7),
            (datetime64(2014, 1, 1, 12, 2, 0), 110),
        ], list(ts['return'].fetch(datetime64(2014, 1, 1, 12, 0, 0))))

    def test_serialize(self):
        ts = {'sampling': numpy.timedelta64(500, 'ms'), 'agg': 'mean'}
        tsb = carbonara.BoundTimeSerie(block_size=ts['sampling'])

        tsb.set_values(numpy.array([
            (datetime64(2014, 1, 1, 12, 0, 0, 1234), 3),
            (datetime64(2014, 1, 1, 12, 0, 0, 321), 6),
            (datetime64(2014, 1, 1, 12, 1, 4, 234), 5),
            (datetime64(2014, 1, 1, 12, 1, 9, 32), 7),
            (datetime64(2014, 1, 1, 12, 2, 12, 532), 1)],
            dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
            before_truncate_callback=functools.partial(
                self._resample_and_merge, agg_dict=ts))

        key = ts['return'].get_split_key()
        o, s = ts['return'].serialize(key)
        self.assertEqual(ts['return'],
                         carbonara.AggregatedTimeSerie.unserialize(
                             s, key, ts['return'].aggregation))

    def test_no_truncation(self):
        ts = {'sampling': numpy.timedelta64(60, 's'), 'agg': 'mean'}
        tsb = carbonara.BoundTimeSerie()

        for i in six.moves.range(1, 11):
            tsb.set_values(numpy.array([
                (datetime64(2014, 1, 1, 12, i, i), float(i))],
                dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
                before_truncate_callback=functools.partial(
                    self._resample_and_merge, agg_dict=ts))
            tsb.set_values(numpy.array([
                (datetime64(2014, 1, 1, 12, i, i + 1), float(i + 1))],
                dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
                before_truncate_callback=functools.partial(
                    self._resample_and_merge, agg_dict=ts))
            self.assertEqual(i, len(list(ts['return'].fetch())))

    def test_back_window(self):
        """Back window testing.

        Test the back window on an archive is not longer than the window we
        aggregate on.
        """
        ts = {'sampling': numpy.timedelta64(1, 's'), 'size': 60, 'agg': 'mean'}
        tsb = carbonara.BoundTimeSerie(block_size=ts['sampling'])

        tsb.set_values(numpy.array([
            (datetime64(2014, 1, 1, 12, 0, 1, 2300), 1),
            (datetime64(2014, 1, 1, 12, 0, 1, 4600), 2),
            (datetime64(2014, 1, 1, 12, 0, 2, 4500), 3),
            (datetime64(2014, 1, 1, 12, 0, 2, 7800), 4),
            (datetime64(2014, 1, 1, 12, 0, 3, 8), 2.5)],
            dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
            before_truncate_callback=functools.partial(
                self._resample_and_merge, agg_dict=ts))

        self.assertEqual(
            [
                (datetime64(2014, 1, 1, 12, 0, 1), 1.5),
                (datetime64(2014, 1, 1, 12, 0, 2), 3.5),
                (datetime64(2014, 1, 1, 12, 0, 3), 2.5),
            ],
            list(ts['return'].fetch()))

    def test_back_window_ignore(self):
        """Back window testing.

        Test the back window on an archive is not longer than the window we
        aggregate on.
        """
        ts = {'sampling': numpy.timedelta64(1, 's'), 'size': 60, 'agg': 'mean'}
        tsb = carbonara.BoundTimeSerie(block_size=ts['sampling'])

        tsb.set_values(numpy.array([
            (datetime64(2014, 1, 1, 12, 0, 1, 2300), 1),
            (datetime64(2014, 1, 1, 12, 0, 1, 4600), 2),
            (datetime64(2014, 1, 1, 12, 0, 2, 4500), 3),
            (datetime64(2014, 1, 1, 12, 0, 2, 7800), 4),
            (datetime64(2014, 1, 1, 12, 0, 3, 8), 2.5)],
            dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
            before_truncate_callback=functools.partial(
                self._resample_and_merge, agg_dict=ts))

        self.assertEqual(
            [
                (datetime64(2014, 1, 1, 12, 0, 1), 1.5),
                (datetime64(2014, 1, 1, 12, 0, 2), 3.5),
                (datetime64(2014, 1, 1, 12, 0, 3), 2.5),
            ],
            list(ts['return'].fetch()))

        tsb.set_values(numpy.array([
            (datetime64(2014, 1, 1, 12, 0, 2, 99), 9)],
            dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
            before_truncate_callback=functools.partial(
                self._resample_and_merge, agg_dict=ts))

        self.assertEqual(
            [
                (datetime64(2014, 1, 1, 12, 0, 1), 1.5),
                (datetime64(2014, 1, 1, 12, 0, 2), 3.5),
                (datetime64(2014, 1, 1, 12, 0, 3), 2.5),
            ],
            list(ts['return'].fetch()))

        tsb.set_values(numpy.array([
            (datetime64(2014, 1, 1, 12, 0, 2, 99), 9),
            (datetime64(2014, 1, 1, 12, 0, 3, 9), 4.5)],
            dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
            before_truncate_callback=functools.partial(
                self._resample_and_merge, agg_dict=ts))

        self.assertEqual(
            [
                (datetime64(2014, 1, 1, 12, 0, 1), 1.5),
                (datetime64(2014, 1, 1, 12, 0, 2), 3.5),
                (datetime64(2014, 1, 1, 12, 0, 3), 3.5),
            ],
            list(ts['return'].fetch()))

    def test_split_key(self):
        self.assertEqual(
            numpy.datetime64("2014-10-07"),
            carbonara.SplitKey.from_timestamp_and_sampling(
                numpy.datetime64("2015-01-01T15:03"),
                numpy.timedelta64(3600, 's')))
        self.assertEqual(
            numpy.datetime64("2014-12-31 18:00"),
            carbonara.SplitKey.from_timestamp_and_sampling(
                numpy.datetime64("2015-01-01 15:03:58"),
                numpy.timedelta64(58, 's')))

        key = carbonara.SplitKey.from_timestamp_and_sampling(
            numpy.datetime64("2015-01-01 15:03"),
            numpy.timedelta64(3600, 's'))

        self.assertGreater(key, numpy.datetime64("1970"))

        self.assertGreaterEqual(key, numpy.datetime64("1970"))

    def test_split_key_cmp(self):
        dt1 = numpy.datetime64("2015-01-01T15:03")
        dt1_1 = numpy.datetime64("2015-01-01T15:03")
        dt2 = numpy.datetime64("2015-01-05T15:03")
        td = numpy.timedelta64(60, 's')
        td2 = numpy.timedelta64(300, 's')

        self.assertEqual(
            carbonara.SplitKey.from_timestamp_and_sampling(dt1, td),
            carbonara.SplitKey.from_timestamp_and_sampling(dt1, td))
        self.assertEqual(
            carbonara.SplitKey.from_timestamp_and_sampling(dt1, td),
            carbonara.SplitKey.from_timestamp_and_sampling(dt1_1, td))
        self.assertNotEqual(
            carbonara.SplitKey.from_timestamp_and_sampling(dt1, td),
            carbonara.SplitKey.from_timestamp_and_sampling(dt2, td))
        self.assertNotEqual(
            carbonara.SplitKey.from_timestamp_and_sampling(dt1, td),
            carbonara.SplitKey.from_timestamp_and_sampling(dt1, td2))

        self.assertLess(
            carbonara.SplitKey.from_timestamp_and_sampling(dt1, td),
            carbonara.SplitKey.from_timestamp_and_sampling(dt2, td))
        self.assertLessEqual(
            carbonara.SplitKey.from_timestamp_and_sampling(dt1, td),
            carbonara.SplitKey.from_timestamp_and_sampling(dt1, td))

        self.assertGreater(
            carbonara.SplitKey.from_timestamp_and_sampling(dt2, td),
            carbonara.SplitKey.from_timestamp_and_sampling(dt1, td))
        self.assertGreaterEqual(
            carbonara.SplitKey.from_timestamp_and_sampling(dt2, td),
            carbonara.SplitKey.from_timestamp_and_sampling(dt2, td))

    def test_split_key_cmp_negative(self):
        dt1 = numpy.datetime64("2015-01-01T15:03")
        dt1_1 = numpy.datetime64("2015-01-01T15:03")
        dt2 = numpy.datetime64("2015-01-05T15:03")
        td = numpy.timedelta64(60, 's')
        td2 = numpy.timedelta64(300, 's')

        self.assertFalse(
            carbonara.SplitKey.from_timestamp_and_sampling(dt1, td) !=
            carbonara.SplitKey.from_timestamp_and_sampling(dt1, td))
        self.assertFalse(
            carbonara.SplitKey.from_timestamp_and_sampling(dt1, td) !=
            carbonara.SplitKey.from_timestamp_and_sampling(dt1_1, td))
        self.assertFalse(
            carbonara.SplitKey.from_timestamp_and_sampling(dt1, td) ==
            carbonara.SplitKey.from_timestamp_and_sampling(dt2, td))
        self.assertFalse(
            carbonara.SplitKey.from_timestamp_and_sampling(dt1, td) ==
            carbonara.SplitKey.from_timestamp_and_sampling(dt2, td2))
        self.assertRaises(
            TypeError,
            operator.le,
            carbonara.SplitKey.from_timestamp_and_sampling(dt1, td),
            carbonara.SplitKey.from_timestamp_and_sampling(dt2, td2))
        self.assertRaises(
            TypeError,
            operator.ge,
            carbonara.SplitKey.from_timestamp_and_sampling(dt1, td),
            carbonara.SplitKey.from_timestamp_and_sampling(dt2, td2))
        self.assertRaises(
            TypeError,
            operator.gt,
            carbonara.SplitKey.from_timestamp_and_sampling(dt1, td),
            carbonara.SplitKey.from_timestamp_and_sampling(dt2, td2))
        self.assertRaises(
            TypeError,
            operator.lt,
            carbonara.SplitKey.from_timestamp_and_sampling(dt1, td),
            carbonara.SplitKey.from_timestamp_and_sampling(dt2, td2))

        self.assertFalse(
            carbonara.SplitKey.from_timestamp_and_sampling(dt1, td) >=
            carbonara.SplitKey.from_timestamp_and_sampling(dt2, td))
        self.assertFalse(
            carbonara.SplitKey.from_timestamp_and_sampling(dt1, td) >
            carbonara.SplitKey.from_timestamp_and_sampling(dt1, td))

        self.assertFalse(
            carbonara.SplitKey.from_timestamp_and_sampling(dt2, td) <=
            carbonara.SplitKey.from_timestamp_and_sampling(dt1, td))
        self.assertFalse(
            carbonara.SplitKey.from_timestamp_and_sampling(dt2, td) <
            carbonara.SplitKey.from_timestamp_and_sampling(dt2, td))

    def test_split_key_next(self):
        self.assertEqual(
            numpy.datetime64("2015-03-06"),
            next(carbonara.SplitKey.from_timestamp_and_sampling(
                numpy.datetime64("2015-01-01 15:03"),
                numpy.timedelta64(3600, 's'))))
        self.assertEqual(
            numpy.datetime64("2015-08-03"),
            next(next(carbonara.SplitKey.from_timestamp_and_sampling(
                numpy.datetime64("2015-01-01T15:03"),
                numpy.timedelta64(3600, 's')))))

    def test_split(self):
        sampling = numpy.timedelta64(5, 's')
        points = 100000
        ts = carbonara.TimeSerie.from_data(
            timestamps=list(map(datetime.datetime.utcfromtimestamp,
                                six.moves.range(points))),
            values=list(six.moves.range(points)))
        agg = self._resample(ts, sampling, 'mean')

        grouped_points = list(agg.split())

        self.assertEqual(
            math.ceil((points / sampling.astype(float))
                      / carbonara.SplitKey.POINTS_PER_SPLIT),
            len(grouped_points))
        self.assertEqual("0.0",
                         str(carbonara.SplitKey(grouped_points[0][0], 0)))
        # 3600 × 5s = 5 hours
        self.assertEqual(datetime64(1970, 1, 1, 5),
                         grouped_points[1][0])
        self.assertEqual(carbonara.SplitKey.POINTS_PER_SPLIT,
                         len(grouped_points[0][1]))

    def test_from_timeseries(self):
        sampling = numpy.timedelta64(5, 's')
        points = 100000
        ts = carbonara.TimeSerie.from_data(
            timestamps=list(map(datetime.datetime.utcfromtimestamp,
                                six.moves.range(points))),
            values=list(six.moves.range(points)))
        agg = self._resample(ts, sampling, 'mean')

        split = [t[1] for t in list(agg.split())]

        self.assertEqual(agg,
                         carbonara.AggregatedTimeSerie.from_timeseries(
                             split, aggregation=agg.aggregation))

    def test_resample(self):
        ts = carbonara.TimeSerie.from_data(
            [datetime64(2014, 1, 1, 12, 0, 0),
             datetime64(2014, 1, 1, 12, 0, 4),
             datetime64(2014, 1, 1, 12, 0, 9),
             datetime64(2014, 1, 1, 12, 0, 11),
             datetime64(2014, 1, 1, 12, 0, 12)],
            [3, 5, 6, 2, 4])
        agg_ts = self._resample(ts, numpy.timedelta64(5, 's'), 'mean')
        self.assertEqual(3, len(agg_ts))

        agg_ts = agg_ts.resample(numpy.timedelta64(10, 's'))
        self.assertEqual(2, len(agg_ts))
        self.assertEqual(5, agg_ts[0][1])
        self.assertEqual(3, agg_ts[1][1])

    def test_iter(self):
        ts = carbonara.TimeSerie.from_data(
            [datetime64(2014, 1, 1, 12, 0, 0),
             datetime64(2014, 1, 1, 12, 0, 11),
             datetime64(2014, 1, 1, 12, 0, 12)],
            [3, 5, 6])
        self.assertEqual([
            (numpy.datetime64('2014-01-01T12:00:00'), 3.),
            (numpy.datetime64('2014-01-01T12:00:11'), 5.),
            (numpy.datetime64('2014-01-01T12:00:12'), 6.),
        ], list(ts))
