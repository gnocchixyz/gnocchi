# -*- encoding: utf-8 -*-
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
import numpy
from numpy.testing import assert_equal

from gnocchi import calendar
from gnocchi.tests import base as tests_base


class TestCalender(tests_base.TestCase):

    def test_get_year(self):
        dates = numpy.array(['2018-01-01', '2019-01-01', '2020-01-01'],
                            dtype='datetime64[ns]')
        assert_equal(numpy.array(['2018', '2019', '2020'],
                                 dtype='datetime64[Y]'),
                     calendar.year(dates))

    def test_half_of_year(self):
        dates = numpy.arange('2018-01-01', '2018-12-31', dtype='datetime64[D]')
        assert_equal(numpy.array([1] * 181 + [2] * 183),
                     calendar.half_of_year(dates))

    def test_half_and_year(self):
        dates = numpy.arange('2018-01-01', '2018-12-31', dtype='datetime64[D]')
        assert_equal(numpy.array(['2018-01'] * 181 + ['2018-07'] * 183,
                                 dtype='datetime64[M]'),
                     calendar.half_and_year(dates))

    def test_quarter_of_year(self):
        dates = numpy.arange('2018-01-01', '2018-12-31', dtype='datetime64[D]')
        assert_equal(numpy.array([1] * 90 + [2] * 91 + [3] * 92 + [4] * 91),
                     calendar.quarter_of_year(dates))

    def test_quarter_and_year(self):
        dates = numpy.arange('2018-01-01', '2018-12-31', dtype='datetime64[D]')
        assert_equal(numpy.array(['2018-01'] * 90 + ['2018-04'] * 91 +
                                 ['2018-07'] * 92 + ['2018-10'] * 91,
                                 dtype='datetime64[M]'),
                     calendar.quarter_and_year(dates))

    def test_get_month_and_year(self):
        dates = numpy.array(['2018-01-01', '2019-03-01', '2020-05-01'],
                            dtype='datetime64[ns]')
        assert_equal(numpy.array(['2018-01', '2019-03', '2020-05'],
                                 dtype='datetime64[M]'),
                     calendar.month_and_year(dates))

    def test_day_of_week(self):
        dates = numpy.arange('2010-01-01', '2020-12-31', dtype='datetime64[D]')
        expected = numpy.array([i.isocalendar()[2] for i in
                                dates.astype('datetime64[ms]').astype(object)])
        # isocalendar sets sunday as 7, we set it as 0.
        expected[expected == 7] = 0
        assert_equal(expected, calendar.day_of_week(dates))

    def test_day_of_month(self):
        dates = numpy.array(['2017-12-29', '2017-12-30', '2017-12-31',
                             '2018-01-01', '2018-01-02', '2018-01-03'],
                            dtype='datetime64[ns]')
        assert_equal(numpy.array([29, 30, 31, 1, 2, 3]),
                     calendar.day_of_month(dates))

    def test_day_of_year(self):
        dates = numpy.array(['2017-12-29', '2017-12-30', '2017-12-31',
                             '2018-01-01', '2018-01-02', '2018-01-03'],
                            dtype='datetime64[ns]')
        assert_equal(numpy.array([362, 363, 364, 0, 1, 2]),
                     calendar.day_of_year(dates))
        dates = numpy.array(['2016-12-29', '2016-12-30', '2016-12-31'],
                            dtype='datetime64[ns]')
        assert_equal(numpy.array([363, 364, 365]),
                     calendar.day_of_year(dates))

    def test_iso_week_of_year(self):
        dates = numpy.arange('2010-01-01', '2020-12-31', dtype='datetime64[D]')
        expected = numpy.array([i.isocalendar()[1] for i in
                                dates.astype('datetime64[ms]').astype(object)])
        assert_equal(expected, calendar.iso_week_of_year(dates))

    def test_week_and_year(self):
        dates = numpy.array(['2017-12-29', '2017-12-30', '2017-12-31',
                             '2018-01-01', '2018-01-02', '2018-01-03'],
                            dtype='datetime64[ns]')
        assert_equal(numpy.array(['2017-12-24', '2017-12-24', '2017-12-31',
                                  '2017-12-31', '2017-12-31', '2017-12-31'],
                                 dtype='datetime64[D]'),
                     calendar.week_and_year(dates))
        dates = numpy.array(['2016-02-27', '2016-02-28', '2016-02-29'],
                            dtype='datetime64[ns]')
        assert_equal(numpy.array(['2016-02-21', '2016-02-28', '2016-02-28'],
                                 dtype='datetime64[D]'),
                     calendar.week_and_year(dates))

    def test_month_of_year(self):
        dates = numpy.array(['2018-01-01', '2019-03-01', '2020-05-01'],
                            dtype='datetime64[ns]')
        assert_equal(numpy.array([1, 3, 5]),
                     calendar.month_of_year(dates))
