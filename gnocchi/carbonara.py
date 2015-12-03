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
"""Time series data manipulation, better with pancetta."""

import functools
import logging
import operator
import re

import msgpack
import pandas
import six


LOG = logging.getLogger(__name__)

AGGREGATION_METHODS = set(('mean', 'sum', 'last', 'max', 'min',
                           'std', 'median', 'first', 'count'))


class NoDeloreanAvailable(Exception):
    """Error raised when trying to insert a value that is too old."""

    def __init__(self, first_timestamp, bad_timestamp):
        self.first_timestamp = first_timestamp
        self.bad_timestamp = bad_timestamp
        super(NoDeloreanAvailable, self).__init__(
            "%s is before %s" % (bad_timestamp, first_timestamp))


class UnAggregableTimeseries(Exception):
    """Error raised when timeseries cannot be aggregated."""
    def __init__(self, reason):
        self.reason = reason
        super(UnAggregableTimeseries, self).__init__(reason)


class UnknownAggregationMethod(Exception):
    """Error raised when the aggregation method is unknown."""
    def __init__(self, agg):
        self.aggregation_method = agg
        super(UnknownAggregationMethod, self).__init__(
            "Unknown aggregation method `%s'" % agg)


class SerializableMixin(object):

    @classmethod
    def unserialize(cls, data):
        return cls.from_dict(msgpack.loads(data, encoding='utf-8'))

    @classmethod
    def unserialize_from_file(cls, stream):
        return cls.from_dict(msgpack.unpack(stream, encoding='utf-8'))

    def serialize(self):
        return msgpack.dumps(self.to_dict())

    def serialize_to_file(self, stream):
        return msgpack.pack(self.to_dict(), stream)


class TimeSerie(SerializableMixin):
    """A representation of series of a timestamp with a value.

    Duplicate timestamps are not allowed and will be filtered to use the
    last in the group when the TimeSerie is created or extended.
    """

    def __init__(self, timestamps=None, values=None):
        self.ts = pandas.Series(values, timestamps).groupby(
            level=0).last().sort_index()

    @classmethod
    def from_tuples(cls, timestamps_values):
        return cls(*zip(*timestamps_values))

    def __eq__(self, other):
        return (isinstance(other, TimeSerie)
                and self.ts.all() == other.ts.all())

    def __getitem__(self, key):
        return self.ts[key]

    def set_values(self, values):
        t = pandas.Series(*reversed(list(zip(*values)))).groupby(
            level=0).last()
        self.ts = t.combine_first(self.ts).sort_index()

    def __len__(self):
        return len(self.ts)

    @staticmethod
    def _timestamps_and_values_from_dict(values):
        v = tuple(
            zip(*dict(
                (pandas.Timestamp(k), v)
                for k, v in six.iteritems(values)).items()))
        if v:
            return v
        return (), ()

    @classmethod
    def from_dict(cls, d):
        """Build a time series from a dict.

        The dict format must be datetime as key and values as values.

        :param d: The dict.
        :returns: A TimeSerie object
        """
        return cls(*cls._timestamps_and_values_from_dict(d['values']))

    def to_dict(self):
        return {
            'values': dict((timestamp.value, float(v))
                           for timestamp, v
                           in six.iteritems(self.ts.dropna())),
        }

    @staticmethod
    def _serialize_time_period(value):
        if value:
            return six.text_type(value.n) + value.rule_code

    @staticmethod
    def _round_timestamp(ts, freq):
        return pandas.Timestamp(
            (ts.value // freq.delta.value) * freq.delta.value)


class BoundTimeSerie(TimeSerie):
    def __init__(self, timestamps=None, values=None,
                 block_size=None, back_window=0):
        """A time serie that is limited in size.

        Used to represent the full-resolution buffer of incoming raw
        datapoints associated with a metric.

        The maximum size of this time serie is expressed in a number of block
        size, called the back window.
        When the timeserie is truncated, a whole block is removed.

        You cannot set a value using a timestamp that is prior to the last
        timestamp minus this number of blocks. By default, a back window of 0
        does not allow you to go back in time prior to the current block being
        used.

        """
        super(BoundTimeSerie, self).__init__(timestamps, values)
        if isinstance(block_size, (float, six.integer_types)):
            self.block_size = pandas.tseries.offsets.Nano(block_size * 10e8)
        else:
            self.block_size = pandas.tseries.frequencies.to_offset(block_size)
        self.back_window = back_window
        self._truncate()

    def __eq__(self, other):
        return (isinstance(other, BoundTimeSerie)
                and super(BoundTimeSerie, self).__eq__(other)
                and self.block_size == other.block_size
                and self.back_window == other.back_window)

    def set_values(self, values, before_truncate_callback=None,
                   ignore_too_old_timestamps=False):
        if self.block_size is not None and not self.ts.empty:
            values = sorted(values, key=operator.itemgetter(0))
            first_block_timestamp = self._first_block_timestamp()
            if ignore_too_old_timestamps:
                for index, (timestamp, value) in enumerate(values):
                    if timestamp >= first_block_timestamp:
                        values = values[index:]
                        break
                else:
                    values = []
            else:
                # Check that the smallest timestamp does not go too much back
                # in time.
                smallest_timestamp = values[0][0]
                if smallest_timestamp < first_block_timestamp:
                    raise NoDeloreanAvailable(first_block_timestamp,
                                              smallest_timestamp)
        super(BoundTimeSerie, self).set_values(values)
        if before_truncate_callback:
            before_truncate_callback(self)
        self._truncate()

    @classmethod
    def from_dict(cls, d):
        """Build a time series from a dict.

        The dict format must be datetime as key and values as values.

        :param d: The dict.
        :returns: A TimeSerie object
        """
        timestamps, values = cls._timestamps_and_values_from_dict(d['values'])
        return cls(timestamps, values,
                   block_size=d.get('block_size'),
                   back_window=d.get('back_window'))

    def to_dict(self):
        basic = super(BoundTimeSerie, self).to_dict()
        basic.update({
            'block_size': self._serialize_time_period(self.block_size),
            'back_window': self.back_window,
        })
        return basic

    def _first_block_timestamp(self):
        rounded = self._round_timestamp(self.ts.index[-1], self.block_size)
        return rounded - (self.block_size * self.back_window)

    def _truncate(self):
        """Truncate the timeserie."""
        if self.block_size is not None and not self.ts.empty:
            # Change that to remove the amount of block needed to have
            # the size <= max_size. A block is a number of "seconds" (a
            # timespan)
            self.ts = self.ts[self._first_block_timestamp():]


class AggregatedTimeSerie(TimeSerie):

    _AGG_METHOD_PCT_RE = re.compile(r"([1-9][0-9]?)pct")

    def __init__(self, timestamps=None, values=None,
                 max_size=None,
                 sampling=None, aggregation_method='mean'):
        """A time serie that is downsampled.

        Used to represent the downsampled timeserie for a single
        granularity/aggregation-function pair stored for a metric.

        """
        super(AggregatedTimeSerie, self).__init__(timestamps, values)

        m = self._AGG_METHOD_PCT_RE.match(aggregation_method)

        if m:
            self.q = float(m.group(1)) / 100
            self.aggregation_method_func_name = 'quantile'
        else:
            if not hasattr(pandas.core.groupby.SeriesGroupBy,
                           aggregation_method):
                raise UnknownAggregationMethod(aggregation_method)
            self.aggregation_method_func_name = aggregation_method

        self.sampling = pandas.tseries.frequencies.to_offset(sampling)
        self.max_size = max_size
        self.aggregation_method = aggregation_method

    def __eq__(self, other):
        return (isinstance(other, AggregatedTimeSerie)
                and super(AggregatedTimeSerie, self).__eq__(other)
                and self.max_size == other.max_size
                and self.sampling == other.sampling
                and self.aggregation_method == other.aggregation_method)

    @classmethod
    def from_dict(cls, d):
        """Build a time series from a dict.

        The dict format must be datetime as key and values as values.

        :param d: The dict.
        :returns: A TimeSerie object
        """
        timestamps, values = cls._timestamps_and_values_from_dict(d['values'])
        return cls(timestamps, values,
                   max_size=d.get('max_size'),
                   sampling=d.get('sampling'),
                   aggregation_method=d.get('aggregation_method', 'mean'))

    def to_dict(self):
        d = super(AggregatedTimeSerie, self).to_dict()
        d.update({
            'aggregation_method': self.aggregation_method,
            'max_size': self.max_size,
            'sampling': self._serialize_time_period(self.sampling),
        })
        return d

    def _truncate(self):
        """Truncate the timeserie."""
        if self.max_size is not None:
            # Remove empty points if any that could be added by aggregation
            self.ts = self.ts.dropna()[-self.max_size:]

    def _resample(self, after):
        if self.sampling:
            # Group by the sampling, and then apply the aggregation method on
            # the points after `after'
            groupedby = self.ts[after:].groupby(
                functools.partial(self._round_timestamp,
                                  freq=self.sampling))
            agg_func = getattr(groupedby, self.aggregation_method_func_name)
            if self.aggregation_method_func_name == 'quantile':
                aggregated = agg_func(self.q)
            else:
                aggregated = agg_func()
            # Now combine the result with the rest of the point – everything
            # that is before `after'
            self.ts = aggregated.combine_first(self.ts[:after][:-1])

    def update(self, ts):
        if ts.ts.empty:
            return
        index = ts.ts.index
        first_timestamp = index[0]
        last_timestamp = index[-1]
        # Build a new time serie excluding all data points in the range of the
        # timeserie passed as argument
        new_ts = self.ts[:first_timestamp].combine_first(
            self.ts[last_timestamp:])

        # Build a new timeserie where we replaced the timestamp range covered
        # by the timeserie passed as argument
        self.ts = ts.ts.combine_first(new_ts)

        # Resample starting from the first timestamp we received
        # TODO(jd) So this only works correctly because we expect that we are
        # not going to replace a range in the middle of our timeserie. So we re
        # resample EVERYTHING FROM first timestamp. We should rather resample
        # from first timestamp AND TO LAST TIMESTAMP!
        self._resample(first_timestamp)
        self._truncate()


class TimeSerieArchive(SerializableMixin):

    def __init__(self, agg_timeseries):
        """A raw data buffer and a collection of downsampled timeseries.

        Used to represent the set of AggregatedTimeSeries for the range of
        granularities supported for a metric (for a particular aggregation
        function).

        """
        self.agg_timeseries = sorted(agg_timeseries,
                                     key=operator.attrgetter("sampling"))

    @property
    def max_block_size(self):
        return max(agg.sampling for agg in self.agg_timeseries)

    @classmethod
    def from_definitions(cls, definitions, aggregation_method='mean'):
        """Create a new collection of archived time series.

        :param definition: A list of tuple (sampling, max_size)
        :param aggregation_method: Aggregation function to use.
        """
        # Limit the main timeserie to a timespan mapping
        return cls(
            [AggregatedTimeSerie(
                max_size=size,
                sampling=pandas.tseries.offsets.Nano(sampling * 10e8),
                aggregation_method=aggregation_method)
             for sampling, size in definitions]
        )

    def fetch(self, from_timestamp=None, to_timestamp=None,
              timeserie_filter=None):
        """Fetch aggregated time value.

        Returns a sorted list of tuples (timestamp, granularity, value).
        """
        result = []
        end_timestamp = to_timestamp
        for ts in reversed(self.agg_timeseries):
            if timeserie_filter and not timeserie_filter(ts):
                continue
            granularity = ts.sampling.nanos / 1000000000.0
            points = ts[from_timestamp:to_timestamp]
            try:
                # Do not include stop timestamp
                del points[end_timestamp]
            except KeyError:
                pass
            result.extend([(timestamp, granularity, value)
                           for timestamp, value
                           in six.iteritems(points)])
        return result

    def __eq__(self, other):
        return (isinstance(other, TimeSerieArchive)
                and self.agg_timeseries == other.agg_timeseries)

    def update(self, timeserie):
        for agg in self.agg_timeseries:
            agg.update(timeserie)

    def to_dict(self):
        return {
            "archives": [ts.to_dict() for ts in self.agg_timeseries],
        }

    @classmethod
    def from_dict(cls, d):
        return cls([AggregatedTimeSerie.from_dict(a) for a in d['archives']])

    @staticmethod
    def aggregated(timeseries, from_timestamp=None, to_timestamp=None,
                   aggregation='mean', needed_percent_of_overlap=100.0):

        index = ['timestamp', 'granularity']
        columns = ['timestamp', 'granularity', 'value']
        dataframes = []

        if not timeseries:
            return []

        granularities = [set(ts.sampling for ts in timeserie.agg_timeseries)
                         for timeserie in timeseries]
        granularities = granularities[0].intersection(*granularities[1:])
        if len(granularities) == 0:
            raise UnAggregableTimeseries('No granularity match')

        for timeserie in timeseries:
            timeserie_raw = timeserie.fetch(
                from_timestamp, to_timestamp,
                lambda ts: ts.sampling in granularities)

            if timeserie_raw:
                dataframe = pandas.DataFrame(timeserie_raw, columns=columns)
                dataframe = dataframe.set_index(index)
                dataframes.append(dataframe)

        if not dataframes:
            return []

        grouped = pandas.concat(dataframes).groupby(level=index)
        left_boundary_ts = None
        right_boundary_ts = None
        maybe_next_timestamp_is_left_boundary = False
        holes = 0
        for (timestamp, __), group in grouped:
            if group.count()['value'] != len(timeseries):
                maybe_next_timestamp_is_left_boundary = True
                holes += 1
            elif maybe_next_timestamp_is_left_boundary:
                left_boundary_ts = timestamp
                maybe_next_timestamp_is_left_boundary = False
            else:
                right_boundary_ts = timestamp

        if to_timestamp is not None or from_timestamp is not None:
            maximum = len(grouped)
            percent_of_overlap = (float(maximum - holes) * 100.0 /
                                  float(maximum))
            if percent_of_overlap < needed_percent_of_overlap:
                raise UnAggregableTimeseries(
                    'Less than %f%% of datapoints overlap in this '
                    'timespan (%.2f%%)' % (needed_percent_of_overlap,
                                           percent_of_overlap))
        elif (needed_percent_of_overlap > 0 and
                (right_boundary_ts == left_boundary_ts or
                 (right_boundary_ts is None
                  and maybe_next_timestamp_is_left_boundary))):
            LOG.debug("We didn't find points that overlap in those "
                      "timeseries. "
                      "right_boundary_ts=%(right_boundary_ts)s, "
                      "left_boundary_ts=%(left_boundary_ts)s, "
                      "groups=%(groups)s" % {
                          'right_boundary_ts': right_boundary_ts,
                          'left_boundary_ts': left_boundary_ts,
                          'groups': list(grouped)
                      })
            raise UnAggregableTimeseries('No overlap')

        # NOTE(sileht): this call the aggregation method on already
        # aggregated values, for some kind of aggregation this can
        # result can looks wierd, but this is the best we can do
        # because we don't have anymore the raw datapoints in those case.
        # FIXME(sileht): so should we bailout is case of stddev, percentile
        # and median?
        agg_timeserie = getattr(grouped, aggregation)()
        agg_timeserie = agg_timeserie.dropna().reset_index()

        if from_timestamp is None and left_boundary_ts:
            agg_timeserie = agg_timeserie[
                agg_timeserie['timestamp'] >= left_boundary_ts]
        if to_timestamp is None and right_boundary_ts:
            agg_timeserie = agg_timeserie[
                agg_timeserie['timestamp'] <= right_boundary_ts]

        points = (agg_timeserie.sort_values(by=['granularity', 'timestamp'],
                                            ascending=[0, 1]).itertuples())
        return [(timestamp, granularity, value)
                for __, timestamp, granularity, value in points]


import argparse
import datetime

from oslo_utils import timeutils
import prettytable


def _definition(value):
    result = value.split(",")
    if len(result) != 2:
        raise ValueError("Format is: seconds,points")
    return int(result[0]), int(result[1])


def create_archive_file():
    parser = argparse.ArgumentParser(
        description="Create a Carbonara file",
    )
    parser.add_argument("--aggregation-method",
                        type=six.text_type,
                        default="mean",
                        choices=AGGREGATION_METHODS,
                        help="aggregation method to use")
    parser.add_argument("--back-window",
                        type=int,
                        default=0,
                        help="back window to keep")
    parser.add_argument("definition",
                        type=_definition,
                        nargs='+',
                        help="archive definition as granularity,points")
    parser.add_argument("filename",
                        nargs=1,
                        type=argparse.FileType(mode="wb"),
                        help="File name to create")
    args = parser.parse_args()
    ts = TimeSerieArchive.from_definitions(args.definition,
                                           args.aggregation_method)
    args.filename[0].write(ts.serialize())


def dump_archive_file():
    parser = argparse.ArgumentParser(
        description="Dump a Carbonara file",
    )
    parser.add_argument("filename",
                        nargs=1,
                        type=argparse.FileType(mode="rb"),
                        help="File name to read")
    args = parser.parse_args()

    ts = TimeSerieArchive.unserialize_from_file(args.filename[0])

    print("Aggregation method: %s"
          % (ts.agg_timeseries[0].aggregation_method))

    print("Number of aggregated timeseries: %d" % len(ts.agg_timeseries))

    for idx, agg_ts in enumerate(ts.agg_timeseries):
        sampling = agg_ts.sampling.nanos / 1000000000
        timespan = datetime.timedelta(seconds=sampling * agg_ts.max_size)
        print("\nAggregated timeserie #%d: %ds × %d = %s"
              % (idx + 1, sampling, agg_ts.max_size, timespan))
        print("Number of measures: %d" % len(agg_ts))
        table = prettytable.PrettyTable(("Timestamp", "Value"))
        for k, v in agg_ts.ts.iteritems():
            table.add_row((k, v))
        print(table.get_string())


def _timestamp_value(value):
    result = value.split(",")
    if len(result) != 2:
        raise ValueError("Format is: timestamp,value")
    try:
        timestamp = float(result[0])
    except (ValueError, TypeError):
        timestamp = timeutils.normalize_time(
            timeutils.parse_isotime(result[0]))
    else:
        timestamp = datetime.datetime.utcfromtimestamp(timestamp)

    return timestamp, float(result[1])


def update_archive_file():
    parser = argparse.ArgumentParser(
        description="Insert values in a Carbonara file",
    )
    parser.add_argument("timestamp,value",
                        nargs='+',
                        type=_timestamp_value,
                        help="Timestamp and value to set")
    parser.add_argument("filename",
                        nargs=1,
                        type=argparse.FileType(mode="rb+"),
                        help="File name to update")
    args = parser.parse_args()

    ts = TimeSerieArchive.unserialize_from_file(args.filename[0])

    try:
        ts.update(TimeSerie.from_tuples(getattr(args, 'timestamp,value')))
    except Exception as e:
        print("E: %s: %s" % (e.__class__.__name__, e))
        return 1

    args.filename[0].seek(0)
    ts.serialize_to_file(args.filename[0])
