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
"""Gregorian calendar grouping helpers."""

import numpy


def _month_of_year(datetimes):
    return (datetimes.astype('datetime64[M]', copy=False) -
            datetimes.astype('datetime64[Y]', copy=False) + 1)


def month_of_year(datetimes):
    """Return the calendar month of given dates."""
    return _month_of_year(datetimes).astype(int)


def iso_week_of_year(datetimes):
    """Return the ISO week of the year of given dates."""
    dates_offset = (datetimes.astype('datetime64[D]', copy=False) +
                    numpy.timedelta64(3, 'D')).astype(
                        'datetime64[W]', copy=False)
    return numpy.ceil(
        (dates_offset.astype('datetime64[D]', copy=False) -
         dates_offset.astype('datetime64[Y]', copy=False) + 1).astype(int) /
        7.0)


def week_and_year(datetimes):
    """Return the week of the year, grouped on Sunday, for given dates."""
    return ((datetimes.astype('datetime64[D]', copy=False) +
             numpy.timedelta64(4, 'D')).astype('datetime64[W]', copy=False) -
            numpy.timedelta64(4, 'D'))


def day_of_year(datetimes):
    """Return the day of the year of given dates."""
    return (datetimes.astype('datetime64[D]', copy=False) -
            datetimes.astype('datetime64[Y]', copy=False)).astype(int)


def day_of_month(datetimes):
    """Return the day of the month of given dates."""
    return (datetimes.astype('datetime64[D]', copy=False) -
            datetimes.astype('datetime64[M]', copy=False) + 1).astype(int)


def day_of_week(datetimes):
    """Return the day of the week of given dates. Sunday(0) to Saturday(6)."""
    return (datetimes.astype('datetime64[D]', copy=False) +
            numpy.timedelta64(4, 'D')).astype(int) % 7


def month_and_year(datetimes):
    """Return the month and year of given dates."""
    return datetimes.astype('datetime64[M]', copy=False)


def quarter_and_year(datetimes):
    """Return the quarter per year of given dates."""
    return (((_month_of_year(datetimes) - 1) // 3) * 3) + year(datetimes)


def quarter_of_year(datetimes):
    """Return the quarter of the year of given dates."""
    return ((_month_of_year(datetimes) - 1) // 3 + 1).astype(int)


def half_and_year(datetimes):
    """Return the half per year of given dates."""
    return (((_month_of_year(datetimes) - 1) // 6) * 6) + year(datetimes)


def half_of_year(datetimes):
    """Return the half of the year of given dates."""
    return ((_month_of_year(datetimes) - 1) // 6 + 1).astype(int)


def year(datetimes):
    """Return the year of given dates."""
    return datetimes.astype('datetime64[Y]', copy=False)


GROUPINGS = {
    'Y': year,
    'H': half_and_year,
    'Q': quarter_and_year,
    'M': month_and_year,
    'W': week_and_year}
