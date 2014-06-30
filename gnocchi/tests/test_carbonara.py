# -*- encoding: utf-8 -*-
#
# Copyright © 2014 eNovance
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

import pandas
import testtools

from gnocchi import carbonara


class TestBoundTimeSerie(testtools.TestCase):
    def test_base(self):
        carbonara.BoundTimeSerie([datetime.datetime(2014, 1, 1, 12, 0, 0),
                                  datetime.datetime(2014, 1, 1, 12, 0, 4),
                                  datetime.datetime(2014, 1, 1, 12, 0, 9)],
                                 [3, 5, 6])

    def test_timespan(self):
        ts = carbonara.BoundTimeSerie(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6],
            timespan='5s')
        self.assertEqual(len(ts), 2)
        ts.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 10), 3),
                       (datetime.datetime(2014, 1, 1, 12, 0, 11), 4)])
        self.assertEqual(len(ts), 3)

    def test_timespan_unordered(self):
        ts = carbonara.BoundTimeSerie(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 9),
             datetime.datetime(2014, 1, 1, 12, 0, 5)],
            [10, 5, 23],
            timespan='5s')
        self.assertEqual(len(ts), 2)
        ts.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 11), 3),
                       (datetime.datetime(2014, 1, 1, 12, 0, 10), 4)])
        self.assertEqual(len(ts), 3)

    def test_timespan_timelimit(self):
        ts = carbonara.BoundTimeSerie(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6],
            timespan='5s')
        self.assertEqual(len(ts), 2)
        ts.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 10), 3),
                       (datetime.datetime(2014, 1, 1, 12, 0, 11), 4)])
        self.assertEqual(len(ts), 3)

        self.assertRaises(
            carbonara.NoDeloreanAvailable,
            ts.set_values,
            [(datetime.datetime(2014, 1, 1, 12, 0, 0), 42)],
        )


class TestAggregatedTimeSerie(testtools.TestCase):

    def test_base(self):
        carbonara.AggregatedTimeSerie(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6])

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
        ts.set_values(zip(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 4),
             datetime.datetime(2014, 1, 1, 12, 0, 9)],
            [3, 5, 6]))
        self.assertEqual(2, len(ts))
        self.assertEqual(ts[0], 5)
        self.assertEqual(ts[1], 6)

    def test_down_sampling(self):
        ts = carbonara.AggregatedTimeSerie(sampling='5Min')
        ts.set_values(zip(
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
        ts.set_values(zip(
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
        ts.set_values(zip(
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
        ts.set_values(zip(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 1, 4),
             datetime.datetime(2014, 1, 1, 12, 1, 9),
             datetime.datetime(2014, 1, 1, 12, 2, 12)],
            [3, 5, 7, 1]))
        ts2 = carbonara.AggregatedTimeSerie.from_dict(ts.to_dict())
        self.assertEqual(ts, ts2)

    def test_serialize(self):
        ts = carbonara.AggregatedTimeSerie(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 1, 4),
             datetime.datetime(2014, 1, 1, 12, 2, 9),
             datetime.datetime(2014, 1, 1, 12, 3, 12)],
            [3, 5, 7, 100],
            sampling='1Min',
            block_size='1Min',
            max_size=10)
        s = ts.serialize()
        self.assertEqual(ts, carbonara.AggregatedTimeSerie.unserialize(s))

    def test_truncate_block_size(self):
        ts = carbonara.AggregatedTimeSerie(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 0, 5),
             datetime.datetime(2014, 1, 1, 12, 1, 4),
             datetime.datetime(2014, 1, 1, 12, 1, 9),
             datetime.datetime(2014, 1, 1, 12, 2, 12)],
            [3, 8, 5, 7, 1],
            max_size=5,
            block_size=pandas.tseries.offsets.Minute(1))
        self.assertEqual(5, len(ts))
        ts.set_values([(datetime.datetime(2014, 1, 1, 12, 3, 19), 123)])
        self.assertEqual(4, len(ts))
        self.assertEqual(5, ts[datetime.datetime(2014, 1, 1, 12, 1, 4)])
        self.assertEqual(7, ts[datetime.datetime(2014, 1, 1, 12, 1, 9)])
        self.assertEqual(1, ts[datetime.datetime(2014, 1, 1, 12, 2, 12)])
        self.assertEqual(123, ts[datetime.datetime(2014, 1, 1, 12, 3, 19)])
        ts.set_values([(datetime.datetime(2014, 1, 1, 12, 3, 20), 124)])
        self.assertEqual(5, len(ts))


class TestTimeSerieArchive(testtools.TestCase):

    def test_fetch(self):
        tsc = carbonara.TimeSerieArchive.from_definitions(
            [(pandas.tseries.offsets.Minute(1), 60),
             (pandas.tseries.offsets.Minute(5), 24)])

        tsc.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
                        (datetime.datetime(2014, 1, 1, 12, 1, 4), 4),
                        (datetime.datetime(2014, 1, 1, 12, 1, 9), 7),
                        (datetime.datetime(2014, 1, 1, 12, 2, 1), 15),
                        (datetime.datetime(2014, 1, 1, 12, 2, 12), 1)])

        r = tsc.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0))

        self.assertEqual(3, r[datetime.datetime(2014, 1, 1, 12, 0, 0)])
        self.assertEqual(5.5, r[datetime.datetime(2014, 1, 1, 12, 1, 0)])
        self.assertEqual(8, r[datetime.datetime(2014, 1, 1, 12, 2, 0)])

        tsc.set_values([(datetime.datetime(2014, 1, 1, 12, 2, 10), 11)])

        r = tsc.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0))

        self.assertEqual(3, r[datetime.datetime(2014, 1, 1, 12, 0, 0)])
        self.assertEqual(5.5, r[datetime.datetime(2014, 1, 1, 12, 1, 0)])
        self.assertEqual(9, r[datetime.datetime(2014, 1, 1, 12, 2, 0)])

    def test_fetch_agg_std(self):
        tsc = carbonara.TimeSerieArchive.from_definitions(
            [(pandas.tseries.offsets.Minute(1), 60),
             (pandas.tseries.offsets.Minute(5), 24)],
            aggregation_method='std')

        tsc.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
                        (datetime.datetime(2014, 1, 1, 12, 1, 4), 4),
                        (datetime.datetime(2014, 1, 1, 12, 1, 9), 7),
                        (datetime.datetime(2014, 1, 1, 12, 2, 1), 15),
                        (datetime.datetime(2014, 1, 1, 12, 2, 12), 1)])

        r = tsc.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0))

        self.assertAlmostEqual(5.4772255750516612,
                               r[datetime.datetime(2014, 1, 1, 12, 0, 0)])
        self.assertAlmostEqual(2.1213203435596424,
                               r[datetime.datetime(2014, 1, 1, 12, 1, 0)])
        self.assertAlmostEqual(9.8994949366116654,
                               r[datetime.datetime(2014, 1, 1, 12, 2, 0)])

        tsc.set_values([(datetime.datetime(2014, 1, 1, 12, 2, 10), 110)])

        r = tsc.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0))

        self.assertAlmostEqual(42.739521132865619,
                               r[datetime.datetime(2014, 1, 1, 12, 0, 0)])
        self.assertEqual(2.1213203435596424,
                         r[datetime.datetime(2014, 1, 1, 12, 1, 0)])
        self.assertEqual(59.304300012730948,
                         r[datetime.datetime(2014, 1, 1, 12, 2, 0)])

    def test_fetch_agg_max(self):
        tsc = carbonara.TimeSerieArchive.from_definitions(
            [(pandas.tseries.offsets.Minute(1), 60),
             (pandas.tseries.offsets.Minute(5), 24)],
            aggregation_method='max')

        tsc.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
                        (datetime.datetime(2014, 1, 1, 12, 1, 4), 4),
                        (datetime.datetime(2014, 1, 1, 12, 1, 9), 7),
                        (datetime.datetime(2014, 1, 1, 12, 2, 1), 15),
                        (datetime.datetime(2014, 1, 1, 12, 2, 12), 1)])

        r = tsc.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0))

        self.assertEqual(3, r[datetime.datetime(2014, 1, 1, 12, 0, 0)])
        self.assertEqual(7, r[datetime.datetime(2014, 1, 1, 12, 1, 0)])
        self.assertEqual(15, r[datetime.datetime(2014, 1, 1, 12, 2, 0)])

        tsc.set_values([(datetime.datetime(2014, 1, 1, 12, 2, 10), 110)])

        r = tsc.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0))

        self.assertEqual(3, r[datetime.datetime(2014, 1, 1, 12, 0, 0)])
        self.assertEqual(7, r[datetime.datetime(2014, 1, 1, 12, 1, 0)])
        self.assertEqual(110, r[datetime.datetime(2014, 1, 1, 12, 2, 0)])

    def test_serialize(self):
        tsc = carbonara.TimeSerieArchive.from_definitions(
            [(pandas.tseries.offsets.Minute(1), None),
             (pandas.tseries.offsets.Minute(5), None)])
        tsc.set_values([
            (datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
            (datetime.datetime(2014, 1, 1, 12, 1, 4), 5),
            (datetime.datetime(2014, 1, 1, 12, 1, 9), 7),
            (datetime.datetime(2014, 1, 1, 12, 2, 12), 1),
        ])

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
                           'block_size': u'60S',
                           'values': {u'2013-01-01 23:40:00':
                                      3.0276503540974917,
                                      u'2013-01-01 23:45:00':
                                      3.0276503540974917},
                           'max_size': 3600,
                           'sampling': u'60S'}]}
        timeseries = carbonara.TimeSerieArchive.from_dict(d)
        measure = timeseries.fetch()
        self.assertEqual(2, len(measure))
        ts = pandas.Timestamp('2013-01-01 23:45:00', tz=None)
        self.assertAlmostEquals(measure.get(ts), 3.0276503540974917)

    def test_truncation(self):
        ts = carbonara.TimeSerieArchive.from_definitions(
            [(pandas.tseries.offsets.Minute(1), None)])

        for i in xrange(1, 11):
            ts.set_values([
                (datetime.datetime(2014, 1, 1, 12, i, i), float(i))
            ])
            self.assertEqual(i, len(ts.fetch()))

    def test_truncation_with_serialization(self):
        # start with an empty timeseries with a single (60, 3600) archive,
        # as it would be stored
        d = {'timeserie': {'values': {},
                           'timespan': u'120S'},
             'archives': [{'aggregation_method': u'mean',
                           'block_size': u'60S',
                           'values': {},
                           'max_size': 3600,
                           'sampling': u'60S'}]}

        # inject single data points 61s apart, round-triping to and from the
        # storage representation on each iteration
        for i in xrange(1, 11):
            timeseries = carbonara.TimeSerieArchive.from_dict(d)
            measures = timeseries.fetch()
            self.assertEqual(i - 1, len(measures))
            timeseries.set_values([
                (datetime.datetime(2014, 1, 1, 12, i, i), float(i))
            ])
            d = timeseries.to_dict()
            # since we should keep up to 3600 archived datapoints,
            # we expect all 10 of the *aggregated* (as opposed to raw)
            # datapoints not to be discarded
            self.assertEqual(i, len(d['archives'][0]['values']))
