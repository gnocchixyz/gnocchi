# -*- encoding: utf-8 -*-
#
# Copyright © 2014-2015 eNovance
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
import datetime
import os
import subprocess
import tempfile

import fixtures
from oslotest import base
# TODO(jd) We shouldn't use pandas here
import pandas
import six

from gnocchi import carbonara


class TestBoundTimeSerie(base.BaseTestCase):
    @staticmethod
    def test_base():
        carbonara.BoundTimeSerie([datetime.datetime(2014, 1, 1, 12, 0, 0),
                                  datetime.datetime(2014, 1, 1, 12, 0, 4),
                                  datetime.datetime(2014, 1, 1, 12, 0, 9)],
                                 [3, 5, 6])

    def test_block_size(self):
        ts = carbonara.BoundTimeSerie(
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
        ts = carbonara.BoundTimeSerie(
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
        ts = carbonara.BoundTimeSerie(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 9),
             datetime.datetime(2014, 1, 1, 12, 0, 5)],
            [10, 5, 23],
            block_size='5s')
        self.assertEqual(2, len(ts))
        ts.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 11), 3),
                       (datetime.datetime(2014, 1, 1, 12, 0, 10), 4)])
        self.assertEqual(2, len(ts))

    def test_duplicate_timestamps(self):
        ts = carbonara.BoundTimeSerie(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 9),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [10, 5, 23])
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
        carbonara.AggregatedTimeSerie(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6])

    def test_bad_percentile(self):
        for bad_percentile in ('0pct', '100pct', '-1pct', '123pct'):
            self.assertRaises(carbonara.UnknownAggregationMethod,
                              carbonara.AggregatedTimeSerie,
                              sampling='1Min',
                              aggregation_method=bad_percentile)

    def test_74_percentile_serialized(self):
        ts = carbonara.AggregatedTimeSerie(sampling='1Min',
                                           aggregation_method='74pct')
        ts.update(carbonara.TimeSerie.from_tuples(
            [(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
             (datetime.datetime(2014, 1, 1, 12, 0, 4), 5),
             (datetime.datetime(2014, 1, 1, 12, 0, 9), 6)]))

        self.assertEqual(1, len(ts))
        self.assertEqual(5.48, ts[datetime.datetime(2014, 1, 1, 12, 0, 0)])

        # Serialize and unserialize
        ts = carbonara.AggregatedTimeSerie.unserialize(ts.serialize())

        ts.update(carbonara.TimeSerie.from_tuples(
            [(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
             (datetime.datetime(2014, 1, 1, 12, 0, 4), 5),
             (datetime.datetime(2014, 1, 1, 12, 0, 9), 6)]))

        self.assertEqual(1, len(ts))
        self.assertEqual(5.48, ts[datetime.datetime(2014, 1, 1, 12, 0, 0)])

    def test_95_percentile(self):
        ts = carbonara.AggregatedTimeSerie(sampling='1Min',
                                           aggregation_method='95pct')
        ts.update(carbonara.TimeSerie.from_tuples(
            [(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
             (datetime.datetime(2014, 1, 1, 12, 0, 4), 5),
             (datetime.datetime(2014, 1, 1, 12, 0, 9), 6)]))

        self.assertEqual(1, len(ts))
        self.assertEqual(5.9000000000000004,
                         ts[datetime.datetime(2014, 1, 1, 12, 0, 0)])

    def test_different_length_in_timestamps_and_data(self):
        self.assertRaises(ValueError,
                          carbonara.AggregatedTimeSerie,
                          [datetime.datetime(2014, 1, 1, 12, 0, 0),
                           datetime.datetime(2014, 1, 1, 12, 0, 4),
                           datetime.datetime(2014, 1, 1, 12, 0, 9)],
                          [3, 5])

    def test_max_size(self):
        ts = carbonara.AggregatedTimeSerie(
            max_size=2)
        ts.update(carbonara.TimeSerie(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6]))
        self.assertEqual(2, len(ts))
        self.assertEqual(5, ts[0])
        self.assertEqual(6, ts[1])

    def test_down_sampling(self):
        ts = carbonara.AggregatedTimeSerie(sampling='5Min')
        ts.update(carbonara.TimeSerie(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 7]))
        self.assertEqual(1, len(ts))
        self.assertEqual(5, ts[datetime.datetime(2014, 1, 1, 12, 0, 0)])

    def test_down_sampling_with_max_size(self):
        ts = carbonara.AggregatedTimeSerie(
            sampling='1Min',
            max_size=2)
        ts.update(carbonara.TimeSerie(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 1, 4),
             datetime.datetime(2014, 1, 1, 12, 1, 9),
             datetime.datetime(2014, 1, 1, 12, 2, 12)],
            [3, 5, 7, 1]))
        self.assertEqual(2, len(ts))
        self.assertEqual(6, ts[datetime.datetime(2014, 1, 1, 12, 1, 0)])
        self.assertEqual(1, ts[datetime.datetime(2014, 1, 1, 12, 2, 0)])

    def test_down_sampling_with_max_size_and_method_max(self):
        ts = carbonara.AggregatedTimeSerie(
            sampling='1Min',
            max_size=2,
            aggregation_method='max')
        ts.update(carbonara.TimeSerie(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 1, 4),
             datetime.datetime(2014, 1, 1, 12, 1, 9),
             datetime.datetime(2014, 1, 1, 12, 2, 12)],
            [3, 5, 70, 1]))
        self.assertEqual(2, len(ts))
        self.assertEqual(70, ts[datetime.datetime(2014, 1, 1, 12, 1, 0)])
        self.assertEqual(1, ts[datetime.datetime(2014, 1, 1, 12, 2, 0)])

    def test_to_dict_from_dict(self):
        ts = carbonara.AggregatedTimeSerie(
            sampling='1Min',
            max_size=2,
            aggregation_method='max')
        ts.update(carbonara.TimeSerie(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 1, 4),
             datetime.datetime(2014, 1, 1, 12, 1, 9),
             datetime.datetime(2014, 1, 1, 12, 2, 12)],
            [3, 5, 7, 1]))
        ts2 = carbonara.AggregatedTimeSerie.from_dict(ts.to_dict())
        self.assertEqual(ts, ts2)


class TestTimeSerieArchive(base.BaseTestCase):

    def test_empty_update(self):
        tsc = carbonara.TimeSerieArchive.from_definitions(
            [(60, 10),
             (300, 6)])
        tsb = carbonara.BoundTimeSerie(block_size=tsc.max_block_size)
        tsb.set_values([], before_truncate_callback=tsc.update)

        self.assertEqual([], tsc.fetch())

    def test_fetch(self):
        tsc = carbonara.TimeSerieArchive.from_definitions(
            [(60, 10),
             (300, 6)])
        tsb = carbonara.BoundTimeSerie(block_size=tsc.max_block_size)

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
            (datetime.datetime(2014, 1, 1, 12, 6, 0), 3),
        ], before_truncate_callback=tsc.update)

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 12, 5, 13), 5),
        ], before_truncate_callback=tsc.update)

        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 11, 45), 300.0, 6.0),
            (datetime.datetime(2014, 1, 1, 11, 50), 300.0, 27.0),
            (datetime.datetime(2014, 1, 1, 11, 55), 300.0, 5.0),
            (datetime.datetime(2014, 1, 1, 12, 00), 300.0, 6.166666666666667),
            (datetime.datetime(2014, 1, 1, 12, 5), 300.0, 6.0),
            (datetime.datetime(2014, 1, 1, 11, 54), 60.0, 4.0),
            (datetime.datetime(2014, 1, 1, 11, 56), 60.0, 4.0),
            (datetime.datetime(2014, 1, 1, 11, 57), 60.0, 6.0),
            (datetime.datetime(2014, 1, 1, 11, 58), 60.0, 5.0),
            (datetime.datetime(2014, 1, 1, 12, 1), 60.0, 5.5),
            (datetime.datetime(2014, 1, 1, 12, 2), 60.0, 8.0),
            (datetime.datetime(2014, 1, 1, 12, 3), 60.0, 3.0),
            (datetime.datetime(2014, 1, 1, 12, 4), 60.0, 7.0),
            (datetime.datetime(2014, 1, 1, 12, 5), 60.0, 7.0),
            (datetime.datetime(2014, 1, 1, 12, 6), 60.0, 3.0)
        ], tsc.fetch())

        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 12), 300.0, 6.166666666666667),
            (datetime.datetime(2014, 1, 1, 12, 5), 300.0, 6.0),
            (datetime.datetime(2014, 1, 1, 12, 1), 60.0, 5.5),
            (datetime.datetime(2014, 1, 1, 12, 2), 60.0, 8.0),
            (datetime.datetime(2014, 1, 1, 12, 3), 60.0, 3.0),
            (datetime.datetime(2014, 1, 1, 12, 4), 60.0, 7.0),
            (datetime.datetime(2014, 1, 1, 12, 5), 60.0, 7.0),
            (datetime.datetime(2014, 1, 1, 12, 6), 60.0, 3.0)
        ], tsc.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0)))

    def test_fetch_agg_pct(self):
        tsc = carbonara.TimeSerieArchive.from_definitions(
            [(1, 3600 * 24),
             (60, 24 * 60 * 30)],
            aggregation_method='90pct')
        tsb = carbonara.BoundTimeSerie(block_size=tsc.max_block_size)

        # NOTE(jd) What's interesting in this test is that we lack a point for
        # a second, so we have an interval with no value
        tsb.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
                        (datetime.datetime(2014, 1, 1, 12, 0, 0, 123), 4),
                        (datetime.datetime(2014, 1, 1, 12, 0, 2), 4)],
                       before_truncate_callback=tsc.update)

        result = tsc.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0))
        reference = [
            (pandas.Timestamp('2014-01-01 12:00:00'),
             60.0, 4),
            (pandas.Timestamp('2014-01-01 12:00:00'),
             1.0, 3.9),
            (pandas.Timestamp('2014-01-01 12:00:02'),
             1.0, 4)
        ]

        self.assertEqual(len(reference), len(result))

        for ref, res in zip(reference, result):
            self.assertEqual(ref[0], res[0])
            self.assertEqual(ref[1], res[1])
            # Rounding \o/
            self.assertAlmostEqual(ref[2], res[2])

        tsb.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 2, 113), 110)],
                       before_truncate_callback=tsc.update)

        result = tsc.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0))
        reference = [
            (pandas.Timestamp('2014-01-01 12:00:00'),
             60.0, 78.2),
            (pandas.Timestamp('2014-01-01 12:00:00'),
             1.0, 3.9),
            (pandas.Timestamp('2014-01-01 12:00:02'),
             1.0, 99.4)
        ]

        self.assertEqual(len(reference), len(result))

        for ref, res in zip(reference, result):
            self.assertEqual(ref[0], res[0])
            self.assertEqual(ref[1], res[1])
            # Rounding \o/
            self.assertAlmostEqual(ref[2], res[2])

    def test_fetch_nano(self):
        tsc = carbonara.TimeSerieArchive.from_definitions(
            [(0.2, 10),
             (0.5, 6)])
        tsb = carbonara.BoundTimeSerie(block_size=tsc.max_block_size)

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 11, 46, 0, 200123), 4),
            (datetime.datetime(2014, 1, 1, 11, 46, 0, 340000), 8),
            (datetime.datetime(2014, 1, 1, 11, 47, 0, 323154), 50),
            (datetime.datetime(2014, 1, 1, 11, 48, 0, 590903), 4),
            (datetime.datetime(2014, 1, 1, 11, 48, 0, 903291), 4),
        ], before_truncate_callback=tsc.update)

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 11, 48, 0, 821312), 5),
        ], before_truncate_callback=tsc.update)

        self.assertEqual([
            (datetime.datetime(2014, 1, 1, 11, 46), 0.5, 6.0),
            (datetime.datetime(2014, 1, 1, 11, 47), 0.5, 50.0),
            (datetime.datetime(2014, 1, 1, 11, 48, 0, 500000), 0.5,
             4.333333333333333),
            (datetime.datetime(2014, 1, 1, 11, 46, 0, 200000), 0.2, 6.0),
            (datetime.datetime(2014, 1, 1, 11, 47, 0, 200000), 0.2, 50.0),
            (datetime.datetime(2014, 1, 1, 11, 48, 0, 400000), 0.2, 4.0),
            (datetime.datetime(2014, 1, 1, 11, 48, 0, 800000), 0.2, 4.5)
        ], tsc.fetch())

    def test_fetch_agg_std(self):
        tsc = carbonara.TimeSerieArchive.from_definitions(
            [(60, 60),
             (300, 24)],
            aggregation_method='std')
        tsb = carbonara.BoundTimeSerie(block_size=tsc.max_block_size)

        tsb.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
                        (datetime.datetime(2014, 1, 1, 12, 1, 4), 4),
                        (datetime.datetime(2014, 1, 1, 12, 1, 9), 7),
                        (datetime.datetime(2014, 1, 1, 12, 2, 1), 15),
                        (datetime.datetime(2014, 1, 1, 12, 2, 12), 1)],
                       before_truncate_callback=tsc.update)

        self.assertEqual([
            (pandas.Timestamp('2014-01-01 12:00:00'),
             300.0, 5.4772255750516612),
            (pandas.Timestamp('2014-01-01 12:01:00'),
             60.0, 2.1213203435596424),
            (pandas.Timestamp('2014-01-01 12:02:00'),
             60.0, 9.8994949366116654),
        ], tsc.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0)))

        tsb.set_values([(datetime.datetime(2014, 1, 1, 12, 2, 13), 110)],
                       before_truncate_callback=tsc.update)

        self.assertEqual([
            (pandas.Timestamp('2014-01-01 12:00:00'),
             300.0, 42.739521132865619),
            (pandas.Timestamp('2014-01-01 12:01:00'),
             60.0, 2.1213203435596424),
            (pandas.Timestamp('2014-01-01 12:02:00'),
             60.0, 59.304300012730948),
        ], tsc.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0)))

    def test_fetch_agg_max(self):
        tsc = carbonara.TimeSerieArchive.from_definitions(
            [(60, 60),
             (300, 24)],
            aggregation_method='max')
        tsb = carbonara.BoundTimeSerie(block_size=tsc.max_block_size)

        tsb.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
                        (datetime.datetime(2014, 1, 1, 12, 1, 4), 4),
                        (datetime.datetime(2014, 1, 1, 12, 1, 9), 7),
                        (datetime.datetime(2014, 1, 1, 12, 2, 1), 15),
                        (datetime.datetime(2014, 1, 1, 12, 2, 12), 1)],
                       before_truncate_callback=tsc.update)

        self.assertEqual([
            (pandas.Timestamp('2014-01-01 12:00:00'), 300.0, 15),
            (pandas.Timestamp('2014-01-01 12:00:00'), 60.0, 3),
            (pandas.Timestamp('2014-01-01 12:01:00'), 60.0, 7),
            (pandas.Timestamp('2014-01-01 12:02:00'), 60.0, 15),
        ], tsc.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0)))

        tsb.set_values([(datetime.datetime(2014, 1, 1, 12, 2, 13), 110)],
                       before_truncate_callback=tsc.update)

        self.assertEqual([
            (pandas.Timestamp('2014-01-01 12:00:00'), 300.0, 110),
            (pandas.Timestamp('2014-01-01 12:00:00'), 60.0, 3),
            (pandas.Timestamp('2014-01-01 12:01:00'), 60.0, 7),
            (pandas.Timestamp('2014-01-01 12:02:00'), 60.0, 110),
        ], tsc.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0)))

    def test_serialize(self):
        tsc = carbonara.TimeSerieArchive.from_definitions(
            [(0.5, None),
             (2, None)])

        tsb = carbonara.BoundTimeSerie(block_size=tsc.max_block_size)

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 12, 0, 0, 1234), 3),
            (datetime.datetime(2014, 1, 1, 12, 0, 0, 321), 6),
            (datetime.datetime(2014, 1, 1, 12, 1, 4, 234), 5),
            (datetime.datetime(2014, 1, 1, 12, 1, 9, 32), 7),
            (datetime.datetime(2014, 1, 1, 12, 2, 12, 532), 1),
        ], before_truncate_callback=tsc.update)

        self.assertEqual(tsc,
                         carbonara.TimeSerieArchive.unserialize(
                             tsc.serialize()))

    def test_from_dict_resampling_stddev(self):
        d = {'timeserie': {'values': {u'2013-01-01 23:45:01.182000': 1.0,
                                      u'2013-01-01 23:45:02.975000': 2.0,
                                      u'2013-01-01 23:45:03.689000': 3.0,
                                      u'2013-01-01 23:45:04.292000': 4.0,
                                      u'2013-01-01 23:45:05.416000': 5.0,
                                      u'2013-01-01 23:45:06.995000': 6.0,
                                      u'2013-01-01 23:45:07.065000': 7.0,
                                      u'2013-01-01 23:45:08.634000': 8.0,
                                      u'2013-01-01 23:45:09.572000': 9.0,
                                      u'2013-01-01 23:45:10.672000': 10.0},
                           'timespan': u'120S'},
             'archives': [{'aggregation_method': u'std',
                           'values': {u'2013-01-01 23:40:00':
                                      3.0276503540974917,
                                      u'2013-01-01 23:45:00':
                                      3.0276503540974917},
                           'max_size': 3600,
                           'sampling': u'60S'}]}
        timeseries = carbonara.TimeSerieArchive.from_dict(d)
        measure = timeseries.fetch()
        self.assertEqual(2, len(measure))
        measure = timeseries.fetch('2013-01-01 23:45:00',
                                   '2013-01-01 23:46:00')
        self.assertEqual(pandas.Timestamp('2013-01-01 23:45:00'),
                         measure[0][0])
        self.assertAlmostEquals(measure[0][2], 3.0276503540974917)

    def test_no_truncation(self):
        ts = carbonara.TimeSerieArchive.from_definitions(
            [(60, None)])
        tsb = carbonara.BoundTimeSerie()

        for i in six.moves.range(1, 11):
            tsb.set_values([
                (datetime.datetime(2014, 1, 1, 12, i, i), float(i))
            ], before_truncate_callback=ts.update)
            tsb.set_values([
                (datetime.datetime(2014, 1, 1, 12, i, i + 1), float(i + 1))
            ], before_truncate_callback=ts.update)
            self.assertEqual(i, len(ts.fetch()))

    def test_back_window(self):
        """Back window testing.

        Test the the back window on an archive is not longer than the window we
        aggregate on.
        """
        ts = carbonara.TimeSerieArchive.from_definitions(
            [(1, 60)])
        tsb = carbonara.BoundTimeSerie(block_size=ts.max_block_size)

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 12, 0, 1, 2300), 1),
            (datetime.datetime(2014, 1, 1, 12, 0, 1, 4600), 2),
            (datetime.datetime(2014, 1, 1, 12, 0, 2, 4500), 3),
            (datetime.datetime(2014, 1, 1, 12, 0, 2, 7800), 4),
            (datetime.datetime(2014, 1, 1, 12, 0, 3, 8), 2.5),
        ], before_truncate_callback=ts.update)

        self.assertEqual(
            [
                (pandas.Timestamp('2014-01-01 12:00:01'), 1.0, 1.5),
                (pandas.Timestamp('2014-01-01 12:00:02'), 1.0, 3.5),
                (pandas.Timestamp('2014-01-01 12:00:03'), 1.0, 2.5),
            ],
            ts.fetch())

        try:
            tsb.set_values([
                (datetime.datetime(2014, 1, 1, 12, 0, 2, 99), 9),
            ])
        except carbonara.NoDeloreanAvailable as e:
            self.assertEqual(
                six.text_type(e),
                u"2014-01-01 12:00:02.000099 is before 2014-01-01 12:00:03")
            self.assertEqual(datetime.datetime(2014, 1, 1, 12, 0, 2, 99),
                             e.bad_timestamp)
            self.assertEqual(datetime.datetime(2014, 1, 1, 12, 0, 3),
                             e.first_timestamp)
        else:
            self.fail("No exception raised")

    def test_back_window_ignore(self):
        """Back window testing.

        Test the the back window on an archive is not longer than the window we
        aggregate on.
        """
        ts = carbonara.TimeSerieArchive.from_definitions(
            [(1, 60)])
        tsb = carbonara.BoundTimeSerie(block_size=1)

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 12, 0, 1, 2300), 1),
            (datetime.datetime(2014, 1, 1, 12, 0, 1, 4600), 2),
            (datetime.datetime(2014, 1, 1, 12, 0, 2, 4500), 3),
            (datetime.datetime(2014, 1, 1, 12, 0, 2, 7800), 4),
            (datetime.datetime(2014, 1, 1, 12, 0, 3, 8), 2.5),
        ], before_truncate_callback=ts.update)

        self.assertEqual(
            [
                (pandas.Timestamp('2014-01-01 12:00:01'), 1.0, 1.5),
                (pandas.Timestamp('2014-01-01 12:00:02'), 1.0, 3.5),
                (pandas.Timestamp('2014-01-01 12:00:03'), 1.0, 2.5),
            ],
            ts.fetch())

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 12, 0, 2, 99), 9),
        ], ignore_too_old_timestamps=True, before_truncate_callback=ts.update)

        self.assertEqual(
            [
                (pandas.Timestamp('2014-01-01 12:00:01'), 1.0, 1.5),
                (pandas.Timestamp('2014-01-01 12:00:02'), 1.0, 3.5),
                (pandas.Timestamp('2014-01-01 12:00:03'), 1.0, 2.5),
            ],
            ts.fetch())

        tsb.set_values([
            (datetime.datetime(2014, 1, 1, 12, 0, 2, 99), 9),
            (datetime.datetime(2014, 1, 1, 12, 0, 3, 9), 4.5),
        ], ignore_too_old_timestamps=True, before_truncate_callback=ts.update)

        self.assertEqual(
            [
                (pandas.Timestamp('2014-01-01 12:00:01'), 1.0, 1.5),
                (pandas.Timestamp('2014-01-01 12:00:02'), 1.0, 3.5),
                (pandas.Timestamp('2014-01-01 12:00:03'), 1.0, 3.5),
            ],
            ts.fetch())

    def test_aggregated_nominal(self):
        tsc1 = carbonara.TimeSerieArchive.from_definitions(
            [(60, 10),
             (300, 6)])
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1.max_block_size)
        tsc2 = carbonara.TimeSerieArchive.from_definitions(
            [(60, 10),
             (300, 6)])
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2.max_block_size)

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
        ], before_truncate_callback=tsc1.update)

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
        ], before_truncate_callback=tsc2.update)

        output = carbonara.TimeSerieArchive.aggregated([tsc1, tsc2])
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
        ], output)

    def test_aggregated_different_archive(self):
        tsc1 = carbonara.TimeSerieArchive.from_definitions(
            [(60, 50),
             (120, 24)])
        tsc2 = carbonara.TimeSerieArchive.from_definitions(
            [(180, 50),
             (300, 24)])

        self.assertRaises(carbonara.UnAggregableTimeseries,
                          carbonara.TimeSerieArchive.aggregated,
                          [tsc1, tsc2])

    def test_aggregated_different_archive_no_overlap(self):
        tsc1 = carbonara.TimeSerieArchive.from_definitions(
            [(60, 50),
             (120, 24)])
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1.max_block_size)
        tsc2 = carbonara.TimeSerieArchive.from_definitions(
            [(60, 50)])
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2.max_block_size)

        tsb1.set_values([(datetime.datetime(2014, 1, 1, 11, 46, 4), 4)],
                        before_truncate_callback=tsc1.update)
        tsb2.set_values([(datetime.datetime(2014, 1, 1, 9, 1, 4), 4)],
                        before_truncate_callback=tsc2.update)

        dtfrom = datetime.datetime(2014, 1, 1, 11, 0, 0)
        self.assertRaises(carbonara.UnAggregableTimeseries,
                          carbonara.TimeSerieArchive.aggregated,
                          [tsc1, tsc2], from_timestamp=dtfrom)

    def test_aggregated_different_archive_no_overlap2(self):
        tsc1 = carbonara.TimeSerieArchive.from_definitions(
            [(60, 50),
             (120, 24)])
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1.max_block_size)
        tsc2 = carbonara.TimeSerieArchive.from_definitions(
            [(60, 50)])

        tsb1.set_values([(datetime.datetime(2014, 1, 1, 12, 3, 0), 4)],
                        before_truncate_callback=tsc1.update)
        self.assertRaises(carbonara.UnAggregableTimeseries,
                          carbonara.TimeSerieArchive.aggregated,
                          [tsc1, tsc2])

    def test_aggregated_different_archive_no_overlap_but_dont_care(self):
        tsc1 = carbonara.TimeSerieArchive.from_definitions(
            [(60, 50),
             (120, 24)])
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1.max_block_size)
        tsc2 = carbonara.TimeSerieArchive.from_definitions(
            [(60, 50)])

        tsb1.set_values([(datetime.datetime(2014, 1, 1, 12, 3, 0), 4)],
                        before_truncate_callback=tsc1.update)

        res = carbonara.TimeSerieArchive.aggregated(
            [tsc1, tsc2], needed_percent_of_overlap=0)
        self.assertEqual([(pandas.Timestamp('2014-01-01 12:03:00'),
                           60.0, 4.0)], res)

    def test_aggregated_different_archive_overlap(self):
        tsc1 = carbonara.TimeSerieArchive.from_definitions(
            [(60, 10),
             (600, 6)])
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1.max_block_size)
        tsc2 = carbonara.TimeSerieArchive.from_definitions(
            [(60, 10)])
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2.max_block_size)

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
        ], before_truncate_callback=tsc1.update)

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
        ], before_truncate_callback=tsc2.update)

        dtfrom = datetime.datetime(2014, 1, 1, 12, 0, 0)
        dtto = datetime.datetime(2014, 1, 1, 12, 10, 0)

        # By default we require 100% of point that overlap
        # so that fail
        self.assertRaises(carbonara.UnAggregableTimeseries,
                          carbonara.TimeSerieArchive.aggregated,
                          [tsc1, tsc2], from_timestamp=dtfrom,
                          to_timestamp=dtto)

        # Retry with 80% and it works
        output = carbonara.TimeSerieArchive.aggregated([
            tsc1, tsc2], from_timestamp=dtfrom, to_timestamp=dtto,
            needed_percent_of_overlap=80.0)

        self.assertEqual([
            (pandas.Timestamp('2014-01-01 12:01:00'), 60.0, 3.0),
            (pandas.Timestamp('2014-01-01 12:02:00'), 60.0, 3.0),
            (pandas.Timestamp('2014-01-01 12:03:00'), 60.0, 4.0),
            (pandas.Timestamp('2014-01-01 12:04:00'), 60.0, 4.0),
            (pandas.Timestamp('2014-01-01 12:05:00'), 60.0, 3.0),
            (pandas.Timestamp('2014-01-01 12:06:00'), 60.0, 5.0),
            (pandas.Timestamp('2014-01-01 12:07:00'), 60.0, 10.0),
            (pandas.Timestamp('2014-01-01 12:09:00'), 60.0, 2.0),
        ], output)

    def test_aggregated_different_archive_overlap_edge_missing1(self):
        tsc1 = carbonara.TimeSerieArchive.from_definitions([(60, 10)])
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1.max_block_size)
        tsc2 = carbonara.TimeSerieArchive.from_definitions([(60, 10)])
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2.max_block_size)

        tsb1.set_values([
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 9),
            (datetime.datetime(2014, 1, 1, 12, 4, 0), 1),
            (datetime.datetime(2014, 1, 1, 12, 5, 0), 2),
            (datetime.datetime(2014, 1, 1, 12, 6, 0), 7),
            (datetime.datetime(2014, 1, 1, 12, 7, 0), 5),
            (datetime.datetime(2014, 1, 1, 12, 8, 0), 3),
        ], before_truncate_callback=tsc1.update)

        tsb2.set_values([
            (datetime.datetime(2014, 1, 1, 11, 0, 0), 6),
            (datetime.datetime(2014, 1, 1, 12, 1, 0), 2),
            (datetime.datetime(2014, 1, 1, 12, 2, 0), 13),
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 24),
            (datetime.datetime(2014, 1, 1, 12, 4, 0), 4),
            (datetime.datetime(2014, 1, 1, 12, 5, 0), 16),
            (datetime.datetime(2014, 1, 1, 12, 6, 0), 12),
        ], before_truncate_callback=tsc2.update)

        # By default we require 100% of point that overlap
        # but we allow that the last datapoint is missing
        # of the precisest granularity
        output = carbonara.TimeSerieArchive.aggregated([
            tsc1, tsc2], aggregation='sum')

        self.assertEqual([
            (pandas.Timestamp('2014-01-01 12:03:00'), 60.0, 33.0),
            (pandas.Timestamp('2014-01-01 12:04:00'), 60.0, 5.0),
            (pandas.Timestamp('2014-01-01 12:05:00'), 60.0, 18.0),
            (pandas.Timestamp('2014-01-01 12:06:00'), 60.0, 19.0),
        ], output)

    def test_aggregated_different_archive_overlap_edge_missing2(self):
        tsc1 = carbonara.TimeSerieArchive.from_definitions([(60, 10)])
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1.max_block_size)
        tsc2 = carbonara.TimeSerieArchive.from_definitions([(60, 10)])
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2.max_block_size)

        tsb1.set_values([
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 4),
        ], before_truncate_callback=tsc1.update)

        tsb2.set_values([
            (datetime.datetime(2014, 1, 1, 11, 0, 0), 4),
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 4),
        ], before_truncate_callback=tsc2.update)

        output = carbonara.TimeSerieArchive.aggregated([tsc1, tsc2])
        self.assertEqual([
            (pandas.Timestamp('2014-01-01 12:03:00'), 60.0, 4.0),
        ], output)

    def test_aggregated_partial_overlap(self):
        tsc1 = carbonara.TimeSerieArchive.from_definitions([(1, 86400)])
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1.max_block_size)
        tsc2 = carbonara.TimeSerieArchive.from_definitions([(1, 86400)])
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2.max_block_size)

        tsb1.set_values([
            (datetime.datetime(2015, 12, 3, 13, 19, 15), 1),
            (datetime.datetime(2015, 12, 3, 13, 20, 15), 1),
            (datetime.datetime(2015, 12, 3, 13, 21, 15), 1),
            (datetime.datetime(2015, 12, 3, 13, 22, 15), 1),
        ], before_truncate_callback=tsc1.update)

        tsb2.set_values([
            (datetime.datetime(2015, 12, 3, 13, 21, 15), 10),
            (datetime.datetime(2015, 12, 3, 13, 22, 15), 10),
            (datetime.datetime(2015, 12, 3, 13, 23, 15), 10),
            (datetime.datetime(2015, 12, 3, 13, 24, 15), 10),
        ], before_truncate_callback=tsc2.update)

        output = carbonara.TimeSerieArchive.aggregated(
            [tsc1, tsc2], aggregation="sum")

        self.assertEqual([
            (pandas.Timestamp('2015-12-03 13:21:15'), 1.0, 11.0),
            (pandas.Timestamp('2015-12-03 13:22:15'), 1.0, 11.0),
        ], output)

        dtfrom = datetime.datetime(2015, 12, 3, 13, 17, 0)
        dtto = datetime.datetime(2015, 12, 3, 13, 25, 0)

        output = carbonara.TimeSerieArchive.aggregated(
            [tsc1, tsc2], from_timestamp=dtfrom, to_timestamp=dtto,
            aggregation="sum", needed_percent_of_overlap=0)

        self.assertEqual([
            (pandas.Timestamp('2015-12-03 13:19:15'), 1.0, 1.0),
            (pandas.Timestamp('2015-12-03 13:20:15'), 1.0, 1.0),
            (pandas.Timestamp('2015-12-03 13:21:15'), 1.0, 11.0),
            (pandas.Timestamp('2015-12-03 13:22:15'), 1.0, 11.0),
            (pandas.Timestamp('2015-12-03 13:23:15'), 1.0, 10.0),
            (pandas.Timestamp('2015-12-03 13:24:15'), 1.0, 10.0),
        ], output)

        # By default we require 100% of point that overlap
        # so that fail if from or to is set
        self.assertRaises(carbonara.UnAggregableTimeseries,
                          carbonara.TimeSerieArchive.aggregated,
                          [tsc1, tsc2], to_timestamp=dtto)
        self.assertRaises(carbonara.UnAggregableTimeseries,
                          carbonara.TimeSerieArchive.aggregated,
                          [tsc1, tsc2], from_timestamp=dtfrom)


class CarbonaraCmd(base.BaseTestCase):

    def setUp(self):
        super(CarbonaraCmd, self).setUp()
        self.useFixture(fixtures.NestedTempfile())

    def test_create(self):
        filename = tempfile.mktemp()
        subp = subprocess.Popen(['carbonara-create',
                                 '1,2',
                                 filename],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        out, err = subp.communicate()
        subp.wait()
        os.stat(filename)
        self.assertEqual(0, subp.returncode)
        self.assertEqual(b"", out)

    def test_dump(self):
        filename = tempfile.mktemp()
        subp = subprocess.Popen(['carbonara-create',
                                 '1,2',
                                 filename])
        subp.wait()
        subp = subprocess.Popen(['carbonara-dump',
                                 filename],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        out, err = subp.communicate()
        subp.wait()
        self.assertIn(b"Aggregation method", out)

    def test_update(self):
        filename = tempfile.mktemp()
        subp = subprocess.Popen(['carbonara-create',
                                 '2,2',
                                 filename])
        subp.wait()
        self.assertEqual(0, subp.returncode)

        subp = subprocess.Popen(['carbonara-update',
                                 '2014-12-23 23:23:23,1',
                                 '2014-12-23 23:23:24,10',
                                 filename])
        subp.wait()
        self.assertEqual(0, subp.returncode)

        subp = subprocess.Popen(['carbonara-update',
                                 '2014-12-23 23:23:25,7',
                                 filename])
        subp.wait()
        self.assertEqual(0, subp.returncode)

        subp = subprocess.Popen(['carbonara-dump',
                                 filename],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        out, err = subp.communicate()
        subp.wait()
        self.assertEqual(0, subp.returncode)
        self.assertEqual(u"""Aggregation method: mean
Number of aggregated timeseries: 1

Aggregated timeserie #1: 2s × 2 = 0:00:04
Number of measures: 2
+---------------------+-------+
|      Timestamp      | Value |
+---------------------+-------+
| 2014-12-23 23:23:22 |  1.0  |
| 2014-12-23 23:23:24 |  7.0  |
+---------------------+-------+
""", out.decode('utf-8'))
