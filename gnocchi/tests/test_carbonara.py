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

from oslotest import base
import pandas
import six

from gnocchi import carbonara


class TestBoundTimeSerie(base.BaseTestCase):
    def test_base(self):
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


class TestAggregatedTimeSerie(base.BaseTestCase):

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
        self.assertEqual(5, ts[0])
        self.assertEqual(6, ts[1])

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
            max_size=10)
        s = ts.serialize()
        self.assertEqual(ts, carbonara.AggregatedTimeSerie.unserialize(s))


class TestTimeSerieArchive(base.BaseTestCase):

    def test_fetch(self):
        tsc = carbonara.TimeSerieArchive.from_definitions(
            [(pandas.tseries.offsets.Minute(1), 10),
             (pandas.tseries.offsets.Minute(5), 6)])

        tsc.set_values([
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
        ])

        self.assertEqual([
            (pandas.Timestamp('2014-01-01 11:45:00'), 300.0, 6.0),
            (pandas.Timestamp('2014-01-01 11:50:00'), 300.0, 27.0),
            (pandas.Timestamp('2014-01-01 11:54:00'), 60.0, 4.0),
            (pandas.Timestamp('2014-01-01 11:56:00'), 60.0, 4.0),
            (pandas.Timestamp('2014-01-01 11:57:00'), 60.0, 6.0),
            (pandas.Timestamp('2014-01-01 11:58:00'), 60.0, 5.0),
            (pandas.Timestamp('2014-01-01 12:01:00'), 60.0, 5.5),
            (pandas.Timestamp('2014-01-01 12:02:00'), 60.0, 8.0),
            (pandas.Timestamp('2014-01-01 12:03:00'), 60.0, 3.0),
            (pandas.Timestamp('2014-01-01 12:04:00'), 60.0, 7.0),
            (pandas.Timestamp('2014-01-01 12:05:00'), 60.0, 8.0),
            (pandas.Timestamp('2014-01-01 12:06:00'), 60.0, 3.0),
        ], tsc.fetch())

        self.assertEqual([
            (pandas.Timestamp('2014-01-01 12:00:00'),
             300.0, 6.166666666666667),
            (pandas.Timestamp('2014-01-01 12:01:00'), 60.0, 5.5),
            (pandas.Timestamp('2014-01-01 12:02:00'), 60.0, 8.0),
            (pandas.Timestamp('2014-01-01 12:03:00'), 60.0, 3.0),
            (pandas.Timestamp('2014-01-01 12:04:00'), 60.0, 7.0),
            (pandas.Timestamp('2014-01-01 12:05:00'), 60.0, 8.0),
            (pandas.Timestamp('2014-01-01 12:06:00'), 60.0, 3.0),
        ], tsc.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0)))

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

        self.assertEqual([
            (pandas.Timestamp('2014-01-01 12:00:00'),
             300.0, 5.4772255750516612),
            (pandas.Timestamp('2014-01-01 12:01:00'),
             60.0, 2.1213203435596424),
            (pandas.Timestamp('2014-01-01 12:02:00'),
             60.0, 9.8994949366116654),
        ], tsc.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0)))

        tsc.set_values([(datetime.datetime(2014, 1, 1, 12, 2, 13), 110)])

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
            [(pandas.tseries.offsets.Minute(1), 60),
             (pandas.tseries.offsets.Minute(5), 24)],
            aggregation_method='max')

        tsc.set_values([(datetime.datetime(2014, 1, 1, 12, 0, 0), 3),
                        (datetime.datetime(2014, 1, 1, 12, 1, 4), 4),
                        (datetime.datetime(2014, 1, 1, 12, 1, 9), 7),
                        (datetime.datetime(2014, 1, 1, 12, 2, 1), 15),
                        (datetime.datetime(2014, 1, 1, 12, 2, 12), 1)])

        self.assertEqual([
            (pandas.Timestamp('2014-01-01 12:00:00'), 300.0, 15),
            (pandas.Timestamp('2014-01-01 12:00:00'), 60.0, 3),
            (pandas.Timestamp('2014-01-01 12:01:00'), 60.0, 7),
            (pandas.Timestamp('2014-01-01 12:02:00'), 60.0, 15),
        ], tsc.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0)))

        tsc.set_values([(datetime.datetime(2014, 1, 1, 12, 2, 13), 110)])

        self.assertEqual([
            (pandas.Timestamp('2014-01-01 12:00:00'), 300.0, 110),
            (pandas.Timestamp('2014-01-01 12:00:00'), 60.0, 3),
            (pandas.Timestamp('2014-01-01 12:01:00'), 60.0, 7),
            (pandas.Timestamp('2014-01-01 12:02:00'), 60.0, 110),
        ], tsc.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0)))

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

    def test_truncation(self):
        ts = carbonara.TimeSerieArchive.from_definitions(
            [(pandas.tseries.offsets.Minute(1), None)])

        for i in six.moves.range(1, 11):
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
                           'values': {},
                           'max_size': 3600,
                           'sampling': u'60S'}]}

        # inject single data points 61s apart, round-triping to and from the
        # storage representation on each iteration
        for i in six.moves.range(1, 11):
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

    def test_back_window(self):
        """Back window testing.

        Test the the back window on an archive is not longer than the window we
        aggregate on.
        """
        ts = carbonara.TimeSerieArchive.from_definitions(
            [(pandas.tseries.offsets.Second(1), 60)])

        ts.set_values([
            (datetime.datetime(2014, 1, 1, 12, 0, 1, 2300), 1),
            (datetime.datetime(2014, 1, 1, 12, 0, 1, 4600), 2),
            (datetime.datetime(2014, 1, 1, 12, 0, 2, 4500), 3),
            (datetime.datetime(2014, 1, 1, 12, 0, 2, 7800), 4),
            (datetime.datetime(2014, 1, 1, 12, 0, 3, 8), 2.5),
        ])

        self.assertEqual(
            [
                (pandas.Timestamp('2014-01-01 12:00:01'), 1.0, 1.5),
                (pandas.Timestamp('2014-01-01 12:00:02'), 1.0, 3.5),
                (pandas.Timestamp('2014-01-01 12:00:03'), 1.0, 2.5),
            ],
            ts.fetch())

        try:
            ts.set_values([
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

    def test_aggregated_nominal(self):
        tsc1 = carbonara.TimeSerieArchive.from_definitions(
            [(pandas.tseries.offsets.Minute(1), 10),
             (pandas.tseries.offsets.Minute(5), 6)])
        tsc2 = carbonara.TimeSerieArchive.from_definitions(
            [(pandas.tseries.offsets.Minute(1), 10),
             (pandas.tseries.offsets.Minute(5), 6)])

        tsc1.set_values([
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
        ])

        tsc2.set_values([
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
        ])

        output = carbonara.TimeSerieArchive.aggregated([tsc1, tsc2])
        self.assertEqual([
            (pandas.Timestamp('2014-01-01 11:45:00'), 300.0, 5.75),
            (pandas.Timestamp('2014-01-01 11:50:00'), 300.0, 27.5),
            (pandas.Timestamp('2014-01-01 11:54:00'), 60.0, 4.5),
            (pandas.Timestamp('2014-01-01 11:56:00'), 60.0, 4.5),
            (pandas.Timestamp('2014-01-01 11:57:00'), 60.0, 6.5),
            (pandas.Timestamp('2014-01-01 11:58:00'), 60.0, 5.0),
            (pandas.Timestamp('2014-01-01 12:01:00'), 60.0, 6.0),
            (pandas.Timestamp('2014-01-01 12:02:00'), 60.0, 7.0),
            (pandas.Timestamp('2014-01-01 12:03:00'), 60.0, 4.5),
            (pandas.Timestamp('2014-01-01 12:04:00'), 60.0, 5.5),
            (pandas.Timestamp('2014-01-01 12:05:00'), 60.0, 6.75),
            (pandas.Timestamp('2014-01-01 12:06:00'), 60.0, 2.0),
        ], output)

    def test_aggregated_different_archive(self):
        tsc1 = carbonara.TimeSerieArchive.from_definitions(
            [(pandas.tseries.offsets.Minute(1), 50),
             (pandas.tseries.offsets.Minute(2), 24)])
        tsc2 = carbonara.TimeSerieArchive.from_definitions(
            [(pandas.tseries.offsets.Minute(3), 50),
             (pandas.tseries.offsets.Minute(5), 24)])

        self.assertRaises(carbonara.UnAggregableTimeseries,
                          carbonara.TimeSerieArchive.aggregated,
                          [tsc1, tsc2])

    def test_aggregated_different_archive_no_overlap(self):
        tsc1 = carbonara.TimeSerieArchive.from_definitions(
            [(pandas.tseries.offsets.Minute(1), 50),
             (pandas.tseries.offsets.Minute(2), 24)])
        tsc2 = carbonara.TimeSerieArchive.from_definitions(
            [(pandas.tseries.offsets.Minute(1), 50)])

        tsc1.set_values([(datetime.datetime(2014, 1, 1, 11, 46, 4), 4)])
        tsc2.set_values([(datetime.datetime(2014, 1, 1, 9, 1, 4), 4)])

        dtfrom = datetime.datetime(2014, 1, 1, 11, 0, 0)
        self.assertRaises(carbonara.UnAggregableTimeseries,
                          carbonara.TimeSerieArchive.aggregated,
                          [tsc1, tsc2], from_timestamp=dtfrom)

    def test_aggregated_different_archive_no_enough_overlap(self):
        tsc1 = carbonara.TimeSerieArchive.from_definitions(
            [(pandas.tseries.offsets.Minute(1), 50),
             (pandas.tseries.offsets.Minute(2), 24)])
        tsc2 = carbonara.TimeSerieArchive.from_definitions(
            [(pandas.tseries.offsets.Minute(1), 50)])

        tsc1.set_values([(datetime.datetime(2014, 1, 1, 11, 46, 4), 4),
                         (datetime.datetime(2014, 1, 1, 12, 46, 4), 5),
                         (datetime.datetime(2014, 1, 1, 15, 46, 4), 8),
                         (datetime.datetime(2014, 1, 1, 17, 46, 4), 9),
                         (datetime.datetime(2014, 1, 1, 18, 46, 4), 1)])
        tsc2.set_values([(datetime.datetime(2014, 1, 1, 3, 1, 4), 5),
                         (datetime.datetime(2014, 1, 1, 9, 1, 4), 9),
                         (datetime.datetime(2014, 1, 1, 11, 46, 4), 2)])

        dtfrom = datetime.datetime(2014, 1, 1, 11, 0, 0)
        self.assertRaises(carbonara.UnAggregableTimeseries,
                          carbonara.TimeSerieArchive.aggregated,
                          [tsc1, tsc2], from_timestamp=dtfrom)

    def test_aggregated_different_archive_overlap(self):
        tsc1 = carbonara.TimeSerieArchive.from_definitions(
            [(pandas.tseries.offsets.Minute(1), 10),
             (pandas.tseries.offsets.Minute(10), 6)])
        tsc2 = carbonara.TimeSerieArchive.from_definitions(
            [(pandas.tseries.offsets.Minute(1), 10)])

        # NOTE(sileht): minute 8 is missing in both and
        # minute 7 in tsc2 too, but it looks like we have
        # enough point to do the aggregation
        tsc1.set_values([
            (datetime.datetime(2014, 1, 1, 11, 0, 0), 4),
            (datetime.datetime(2014, 1, 1, 12, 1, 0), 3),
            (datetime.datetime(2014, 1, 1, 12, 2, 0), 2),
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 4),
            (datetime.datetime(2014, 1, 1, 12, 4, 0), 2),
            (datetime.datetime(2014, 1, 1, 12, 5, 0), 3),
            (datetime.datetime(2014, 1, 1, 12, 6, 0), 4),
            (datetime.datetime(2014, 1, 1, 12, 7, 0), 10),
            (datetime.datetime(2014, 1, 1, 12, 9, 0), 2),
        ])

        tsc2.set_values([
            (datetime.datetime(2014, 1, 1, 12, 1, 0), 3),
            (datetime.datetime(2014, 1, 1, 12, 2, 0), 4),
            (datetime.datetime(2014, 1, 1, 12, 3, 0), 4),
            (datetime.datetime(2014, 1, 1, 12, 4, 0), 6),
            (datetime.datetime(2014, 1, 1, 12, 5, 0), 3),
            (datetime.datetime(2014, 1, 1, 12, 6, 0), 6),
            (datetime.datetime(2014, 1, 1, 12, 9, 0), 2),
            (datetime.datetime(2014, 1, 1, 12, 11, 0), 2),
            (datetime.datetime(2014, 1, 1, 12, 12, 0), 2),
        ])

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
