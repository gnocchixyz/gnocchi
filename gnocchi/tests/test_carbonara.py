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

import testtools

from gnocchi import carbonara


class TestTimeSerie(testtools.TestCase):

    def test_base(self):
        carbonara.TimeSerie([datetime.datetime(2014, 1, 1, 12, 0, 0),
                             datetime.datetime(2014, 1, 1, 12, 0, 4),
                             datetime.datetime(2014, 1, 1, 12, 0, 9)],
                            [3, 5, 6])

    def test_different_length_in_timestamps_and_data(self):
        self.assertRaises(ValueError,
                          carbonara.TimeSerie,
                          [datetime.datetime(2014, 1, 1, 12, 0, 0),
                           datetime.datetime(2014, 1, 1, 12, 0, 4),
                           datetime.datetime(2014, 1, 1, 12, 0, 9)],
                          [3, 5])

    def test_max_size(self):
        ts = carbonara.TimeSerie([datetime.datetime(2014, 1, 1, 12, 0, 0),
                                  datetime.datetime(2014, 1, 1, 12, 0, 4),
                                  datetime.datetime(2014, 1, 1, 12, 0, 9)],
                                 [3, 5, 6],
                                 max_size=2)
        self.assertEqual(2, len(ts))
        self.assertEqual(ts[0], 5)
        self.assertEqual(ts[1], 6)

    def test_down_sampling(self):
        ts = carbonara.TimeSerie([datetime.datetime(2014, 1, 1, 12, 0, 0),
                                  datetime.datetime(2014, 1, 1, 12, 0, 4),
                                  datetime.datetime(2014, 1, 1, 12, 0, 9)],
                                 [3, 5, 7],
                                 sampling='5Min')
        self.assertEqual(1, len(ts))
        self.assertEqual(5, ts[datetime.datetime(2014, 1, 1, 12, 0, 0)])

    def test_down_sampling_with_max_size(self):
        ts = carbonara.TimeSerie([datetime.datetime(2014, 1, 1, 12, 0, 0),
                                  datetime.datetime(2014, 1, 1, 12, 1, 4),
                                  datetime.datetime(2014, 1, 1, 12, 1, 9),
                                  datetime.datetime(2014, 1, 1, 12, 2, 12)],
                                 [3, 5, 7, 1],
                                 sampling='1Min',
                                 max_size=2)
        self.assertEqual(2, len(ts))
        self.assertEqual(6, ts[datetime.datetime(2014, 1, 1, 12, 1, 0)])
        self.assertEqual(1, ts[datetime.datetime(2014, 1, 1, 12, 2, 0)])

    def test_down_sampling_with_max_size_and_method_max(self):
        ts = carbonara.TimeSerie([datetime.datetime(2014, 1, 1, 12, 0, 0),
                                  datetime.datetime(2014, 1, 1, 12, 1, 4),
                                  datetime.datetime(2014, 1, 1, 12, 1, 9),
                                  datetime.datetime(2014, 1, 1, 12, 2, 12)],
                                 [3, 5, 7, 1],
                                 sampling='1Min',
                                 max_size=2,
                                 aggregation_method='max')
        self.assertEqual(2, len(ts))
        self.assertEqual(7, ts[datetime.datetime(2014, 1, 1, 12, 1, 0)])
        self.assertEqual(1, ts[datetime.datetime(2014, 1, 1, 12, 2, 0)])

    def test_to_dict_from_dict(self):
        ts = carbonara.TimeSerie([datetime.datetime(2014, 1, 1, 12, 0, 0),
                                  datetime.datetime(2014, 1, 1, 12, 1, 4),
                                  datetime.datetime(2014, 1, 1, 12, 1, 9),
                                  datetime.datetime(2014, 1, 1, 12, 2, 12)],
                                 [3, 5, 7, 1],
                                 sampling='1Min',
                                 max_size=2,
                                 aggregation_method='max')

        ts2 = carbonara.TimeSerie.from_dict(ts.to_dict())
        self.assertEqual(ts, ts2)

    def test_update(self):
        ts = carbonara.TimeSerie([datetime.datetime(2014, 1, 1, 12, 0, 0),
                                  datetime.datetime(2014, 1, 1, 12, 1, 4),
                                  datetime.datetime(2014, 1, 1, 12, 2, 9),
                                  datetime.datetime(2014, 1, 1, 12, 3, 12)],
                                 [3, 5, 7, 100],
                                 sampling='1Min',
                                 max_size=3)
        ts2 = carbonara.TimeSerie([datetime.datetime(2014, 1, 1, 12, 2, 0),
                                   datetime.datetime(2014, 1, 1, 12, 3, 4),
                                   datetime.datetime(2014, 1, 1, 12, 4, 9),
                                   datetime.datetime(2014, 1, 1, 12, 5, 12)],
                                  [8, 15, 27, 6])
        ts.update(ts2)

        self.assertEqual(3, len(ts))
        self.assertEqual(57.5, ts[datetime.datetime(2014, 1, 1, 12, 3, 0)])
        self.assertEqual(27, ts[datetime.datetime(2014, 1, 1, 12, 4, )])
        self.assertEqual(6, ts[datetime.datetime(2014, 1, 1, 12, 5, )])

    def test_serialize(self):
        ts = carbonara.TimeSerie(
            [datetime.datetime(2014, 1, 1, 12, 0, 0),
             datetime.datetime(2014, 1, 1, 12, 1, 4),
             datetime.datetime(2014, 1, 1, 12, 2, 9),
             datetime.datetime(2014, 1, 1, 12, 3, 12)],
            [3, 5, 7, 100],
            sampling='1Min',
            max_size=3)
        s = ts.serialize()
        self.assertEqual(ts, carbonara.TimeSerie.unserialize(s))


class TestTimeSerieCollection(testtools.TestCase):

    def test_fetch(self):
        ts = carbonara.TimeSerie([datetime.datetime(2014, 1, 1, 12, 0, 0),
                                  datetime.datetime(2014, 1, 1, 12, 1, 4),
                                  datetime.datetime(2014, 1, 1, 12, 1, 9),
                                  datetime.datetime(2014, 1, 1, 12, 2, 12)],
                                 [3, 5, 7, 1],
                                 sampling='1Min')
        ts2 = carbonara.TimeSerie([datetime.datetime(2014, 1, 1, 12, 0, 0),
                                   datetime.datetime(2014, 1, 1, 12, 1, 4),
                                   datetime.datetime(2014, 1, 1, 12, 1, 9),
                                   datetime.datetime(2014, 1, 1, 12, 2, 12)],
                                  [3, 5, 7, 1],
                                  sampling='5Min')

        tsc = carbonara.TimeSerieCollection([ts, ts2])
        r = tsc[datetime.datetime(2014, 1, 1, 12, 0, 0)]
        self.assertEqual(3, r[datetime.datetime(2014, 1, 1, 12, 0, 0)])
        self.assertEqual(6, r[datetime.datetime(2014, 1, 1, 12, 1, 0)])
        self.assertEqual(1, r[datetime.datetime(2014, 1, 1, 12, 2, 0)])

    def test_fetch_outside_low_precision(self):
        ts = carbonara.TimeSerie([datetime.datetime(2014, 1, 1, 12, 0, 0),
                                  datetime.datetime(2014, 1, 1, 12, 1, 4),
                                  datetime.datetime(2014, 1, 1, 12, 1, 9),
                                  datetime.datetime(2014, 1, 1, 12, 2, 12)],
                                 [3, 5, 7, 1],
                                 sampling='1Min')
        ts2 = carbonara.TimeSerie([datetime.datetime(2014, 1, 1, 12, 3, 0),
                                   datetime.datetime(2014, 1, 1, 12, 5, 4),
                                   datetime.datetime(2014, 1, 1, 12, 8, 9),
                                   datetime.datetime(2014, 1, 1, 12, 14, 12)],
                                  [32, 25, 27, 21],
                                  sampling='5Min')

        tsc = carbonara.TimeSerieCollection([ts, ts2])
        r = tsc.fetch(datetime.datetime(2014, 1, 1, 12, 0, 0))
        self.assertEqual(5, len(r))
        self.assertEqual(3, r[datetime.datetime(2014, 1, 1, 12, 0, 0)])
        self.assertEqual(6, r[datetime.datetime(2014, 1, 1, 12, 1, 0)])
        self.assertEqual(1, r[datetime.datetime(2014, 1, 1, 12, 2, 0)])
        self.assertEqual(26, r[datetime.datetime(2014, 1, 1, 12, 5, 0)])
        self.assertEqual(21, r[datetime.datetime(2014, 1, 1, 12, 10, 0)])
        self.assertRaises(KeyError, r.__getitem__,
                          datetime.datetime(2014, 1, 1, 12, 6, 0))

    def test_serialize(self):
        ts = carbonara.TimeSerie([datetime.datetime(2014, 1, 1, 12, 0, 0),
                                  datetime.datetime(2014, 1, 1, 12, 1, 4),
                                  datetime.datetime(2014, 1, 1, 12, 1, 9),
                                  datetime.datetime(2014, 1, 1, 12, 2, 12)],
                                 [3, 5, 7, 1],
                                 sampling='1Min')
        ts2 = carbonara.TimeSerie([datetime.datetime(2014, 1, 1, 12, 0, 0),
                                   datetime.datetime(2014, 1, 1, 12, 1, 4),
                                   datetime.datetime(2014, 1, 1, 12, 1, 9),
                                   datetime.datetime(2014, 1, 1, 12, 2, 12)],
                                  [3, 5, 7, 1],
                                  sampling='5Min')

        tsc = carbonara.TimeSerieCollection([ts, ts2])
        self.assertEqual(tsc,
                         carbonara.TimeSerieCollection.unserialize(
                             tsc.serialize()))
