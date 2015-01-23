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
"""Time series data manipulation, better with pancetta."""
import operator

import msgpack
import pandas
import six


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


class TimeSerie(object):

    def __init__(self, timestamps=None, values=None):
        self.ts = pandas.Series(values, timestamps)
        self.ts = self.ts.sort_index()

    def __eq__(self, other):
        return (isinstance(other, TimeSerie)
                and self.ts.all() == other.ts.all())

    def __getitem__(self, key):
        return self.ts[key]

    def get(self, key):
        return self.ts[key]

    def set_values(self, values):
        for timestamp, value in values:
            self.ts[timestamp] = value
        self.ts = self.ts.sort_index()

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
            # NOTE(jd) Store up to the nanosecond
            'values': dict((timestamp.value, float(v))
                           for timestamp, v
                           in six.iteritems(self.ts[~self.ts.isnull()])),
        }

    def serialize(self):
        return msgpack.dumps(self.to_dict())

    @classmethod
    def unserialize(cls, data):
        return cls.from_dict(msgpack.loads(data, encoding='utf-8'))

    @staticmethod
    def _serialize_time_period(value):
        if value:
            return six.text_type(value.n) + value.rule_code


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
        self.block_size = pandas.tseries.frequencies.to_offset(block_size)
        self.back_window = back_window
        self._truncate()

    def __eq__(self, other):
        return (isinstance(other, BoundTimeSerie)
                and super(BoundTimeSerie, self).__eq__(other)
                and self.block_size == other.block_size
                and self.back_window == other.back_window)

    def set_values(self, values, before_truncate_callback=None):
        if self.block_size is not None and not self.ts.empty:
            # Check that the smallest timestamp does not go too much back in
            # time.
            # TODO(jd) convert keys to timestamp to be sure we can subtract?
            smallest_timestamp = min(map(operator.itemgetter(0), values))
            first_block_timestamp = self._first_block_timestamp()
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
        ts = self.ts.resample(self.block_size)
        return (ts.index[-1] - (self.block_size * self.back_window))

    def _truncate(self):
        """Truncate the timeserie."""
        if self.block_size is not None and not self.ts.empty:
            # Change that to remove the amount of block needed to have
            # the size <= max_size. A block is a number of "seconds" (a
            # timespan)
            self.ts = self.ts[self._first_block_timestamp():]


class AggregatedTimeSerie(TimeSerie):

    def __init__(self, timestamps=None, values=None,
                 max_size=None,
                 sampling=None, aggregation_method='mean'):
        """A time serie that is downsampled.

        Used to represent the downsampled timeserie for a single
        granularity/aggregation-function pair stored for a metric.

        """
        super(AggregatedTimeSerie, self).__init__(timestamps, values)
        self.aggregation_method = aggregation_method
        self.sampling = pandas.tseries.frequencies.to_offset(sampling)
        self.max_size = max_size

    def __eq__(self, other):
        return (isinstance(other, AggregatedTimeSerie)
                and super(AggregatedTimeSerie, self).__eq__(other)
                and self.max_size == other.max_size
                and self.sampling == other.sampling
                and self.aggregation_method == other.aggregation_method)

    def set_values(self, values):
        values = list(values)
        super(AggregatedTimeSerie, self).set_values(values)
        self._resample(min(list(values), key=operator.itemgetter(0))[0])
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
            self.ts = self.ts[~self.ts.isnull()]
            self.ts = self.ts[-self.max_size:]

    def _resample(self, after):
        if self.sampling:
            self.ts = self.ts[after:].resample(
                self.sampling,
                how=self.aggregation_method).combine_first(
                    self.ts[:after][:-1])

    def update(self, ts):
        # NOTE(jd) Is there a more efficient way to do that with Pandas? The
        # goal is to delete all the values that `ts' is providing again, so
        # that means deleting the aggregate we did for it too.
        index = sorted(ts.ts.index)
        for timestamp, value in sorted(self.ts.iteritems()):
            if timestamp >= index[0] and timestamp <= index[-1]:
                del self.ts[timestamp]

        self.ts = ts.ts.combine_first(self.ts)

        self._resample(min(ts.ts.index))
        self._truncate()


class TimeSerieArchive(object):

    def __init__(self, full_res_timeserie, agg_timeseries):
        """A raw data buffer and a collection of downsampled timeseries.

        Used to represent the set of AggregatedTimeSeries for the range of
        granularities supported for a metric (for a particular aggregation
        function).

        In addition, a single BoundTimeSerie acts as the buffer for full-
        resolution datapoints feeding into the eager aggregation.

        """
        self.full_res_timeserie = full_res_timeserie
        self.agg_timeseries = sorted(agg_timeseries,
                                     key=operator.attrgetter("sampling"))

    @classmethod
    def from_definitions(cls, definitions, aggregation_method='mean',
                         back_window=0):
        """Create a new collection of archived time series.

        :param definition: A list of tuple (sampling, max_size)
        :param aggregation_method: Aggregation function to use.
        :param back_window: Number of block to use as back window.
        """
        definitions = sorted(definitions, key=operator.itemgetter(0))

        # The block size is the coarse grained archive definition
        block_size = definitions[-1][0]

        # Limit the main timeserie to a timespan mapping
        return cls(
            BoundTimeSerie(
                block_size=pandas.tseries.offsets.Second(block_size),
                back_window=back_window),
            [AggregatedTimeSerie(
                max_size=size,
                sampling=pandas.tseries.offsets.Second(sampling),
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
        for ts in self.agg_timeseries:
            if timeserie_filter and not timeserie_filter(ts):
                continue
            if result:
                # Change to_timestamp not to override more precise points we
                # already have
                to_timestamp = result[0][0]
            granularity = ts.sampling.nanos / 1000000000.0
            points = ts[from_timestamp:to_timestamp]
            try:
                # Do not include stop timestamp
                del points[end_timestamp]
            except KeyError:
                pass
            points = [(timestamp, granularity, value)
                      for timestamp, value
                      in six.iteritems(points)]
            points.extend(result)
            result = points
        return result

    def __eq__(self, other):
        return (isinstance(other, TimeSerieArchive)
                and self.full_res_timeserie == other.full_res_timeserie
                and self.agg_timeseries == other.agg_timeseries)

    def _update_aggregated_timeseries(self, timeserie):
        for agg in self.agg_timeseries:
            agg.update(timeserie)

    def set_values(self, values):
        self.full_res_timeserie.set_values(
            values,
            before_truncate_callback=self._update_aggregated_timeseries)

    def to_dict(self):
        return {
            "timeserie": self.full_res_timeserie.to_dict(),
            "archives": [ts.to_dict() for ts in self.agg_timeseries],
        }

    @classmethod
    def from_dict(cls, d):
        return cls(BoundTimeSerie.from_dict(d['timeserie']),
                   [AggregatedTimeSerie.from_dict(a) for a in d['archives']])

    @classmethod
    def unserialize(cls, data):
        return cls.from_dict(msgpack.loads(data, encoding='utf-8'))

    def serialize(self):
        return msgpack.dumps(self.to_dict())

    @staticmethod
    def aggregated(timeseries, from_timestamp=None, to_timestamp=None,
                   aggregation='mean', needed_percent_of_overlap=100.0):

        index = ['timestamp', 'granularity']
        columns = ['timestamp', 'granularity', 'value']
        dataframes = []

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

        points_count = pandas.concat(dataframes).groupby(
            level='timestamp').count()
        maximum = len(points_count)
        minimum = len(points_count[points_count < len(timeseries)].dropna())
        percent_of_overlap = float(maximum - minimum) * 100.0 / float(maximum)
        if percent_of_overlap < needed_percent_of_overlap:
            raise UnAggregableTimeseries('Less than %f%% of datapoints '
                                         'overlap in this timespan (%.2f%%)' %
                                         (needed_percent_of_overlap,
                                          percent_of_overlap))

        grouped = pandas.concat(dataframes).groupby(level=index)
        # NOTE(sileht): this call the aggregation method on already
        # aggregated values, for some kind of aggregation this can
        # result can looks wierd, but this is the best we can do
        # because we don't have anymore the raw datapoints in those case.
        # FIXME(sileht): so should we bailout is case of stddev and median ?
        agg_timeserie = getattr(grouped, aggregation)()
        points = (agg_timeserie.dropna().reset_index()
                  .sort(['granularity', 'timestamp'], ascending=[0, 1])
                  .itertuples())
        points = [(timestamp, granularity, value)
                  for __, timestamp, granularity, value in points]
        return points
