# -*- encoding: utf-8 -*-
#
# Copyright 2014-2015 OpenStack Foundation
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

import numpy
import pandas
import six

from gnocchi import aggregates

from oslo_utils import strutils
from oslo_utils import timeutils
from pytimeparse import timeparse


class MovingAverage(aggregates.CustomAggregator):

    @staticmethod
    def check_window_valid(window):
        """Takes in the window parameter string, reformats as a float."""
        if window is None:
            msg = 'Moving aggregate must have window specified.'
            raise aggregates.CustomAggFailure(msg)
        try:
            return float(timeparse.timeparse(six.text_type(window)))
        except Exception:
            raise aggregates.CustomAggFailure('Invalid value for window')

    @staticmethod
    def retrieve_data(storage_obj, metric, start, stop, window):
        """Retrieves finest-res data available from storage."""
        all_data = storage_obj.get_measures(metric, start, stop)

        try:
            min_grain = min(set([row[1] for row in all_data if row[1] == 0
                                 or window % row[1] == 0]))
        except Exception:
            msg = ("No data available that is either full-res or "
                   "of a granularity that factors into the window size "
                   "you specified.")
            raise aggregates.CustomAggFailure(msg)

        return min_grain, pandas.Series([r[2] for r in all_data
                                         if r[1] == min_grain],
                                        [r[0] for r in all_data
                                         if r[1] == min_grain])

    @staticmethod
    def aggregate_data(data, func, window, min_grain, center=False,
                       min_size=1):
        """Calculates moving func of data with sampling width of window.

        :param data: Series of timestamp, value pairs
        :param func: the function to use when aggregating
        :param window: (float) range of data to use in each aggregation.
        :param min_grain: granularity of the data being passed in.
        :param center: whether to index the aggregated values by the first
            timestamp of the values picked up by the window or by the central
            timestamp.
        :param min_size: if the number of points in the window is less than
            min_size, the aggregate is not computed and nan is returned for
            that iteration.
        """

        if center:
            center = strutils.bool_from_string(center)

        def moving_window(x):
            msec = datetime.timedelta(milliseconds=1)
            zero = datetime.timedelta(seconds=0)
            half_span = datetime.timedelta(seconds=window / 2)
            start = timeutils.normalize_time(data.index[0])
            stop = timeutils.normalize_time(
                data.index[-1] + datetime.timedelta(seconds=min_grain))
            # min_grain addition necessary since each bin of rolled-up data
            # is indexed by leftmost timestamp of bin.

            left = half_span if center else zero
            right = 2 * half_span - left - msec
            # msec subtraction is so we don't include right endpoint in slice.

            x = timeutils.normalize_time(x)

            if x - left >= start and x + right <= stop:
                dslice = data[x - left: x + right]

                if center and dslice.size % 2 == 0:
                    return func([func(data[x - msec - left: x - msec + right]),
                                 func(data[x + msec - left: x + msec + right])
                                 ])

                # (NOTE) atmalagon: the msec shift here is so that we have two
                # consecutive windows; one centered at time x - msec,
                # and one centered at time x + msec. We then average the
                # aggregates from the two windows; this result is centered
                # at time x. Doing this double average is a way to return a
                # centered average indexed by a timestamp that existed in
                # the input data (which wouldn't be the case for an even number
                # of points if we did only one centered average).

            else:
                return numpy.nan
            if dslice.size < min_size:
                return numpy.nan
            return func(dslice)
        try:
            result = pandas.Series(data.index).apply(moving_window)

            # change from integer index to timestamp index
            result.index = data.index

            return [(t, window, r) for t, r
                    in six.iteritems(result[~result.isnull()])]
        except Exception as e:
            raise aggregates.CustomAggFailure(str(e))

    def compute(self, storage_obj, metric, start, stop, window=None,
                center=False):
        """Returns list of (timestamp, window, aggregated value) tuples.

        :param storage_obj: a call is placed to the storage object to retrieve
            the stored data.
        :param metric: the metric
        :param start: start timestamp
        :param stop: stop timestamp
        :param window: format string specifying the size over which to
            aggregate the retrieved data
        :param center: how to index the aggregated data (central timestamp or
            leftmost timestamp)
        """
        window = self.check_window_valid(window)
        min_grain, data = self.retrieve_data(storage_obj, metric, start,
                                             stop, window)
        return self.aggregate_data(data, numpy.mean, window, min_grain, center,
                                   min_size=1)
