# -*- encoding: utf-8 -*-
#
# Copyright © 2016 Red Hat, Inc.
# Copyright © 2014-2015 eNovance
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

import datetime
import functools
import logging
import math
import numbers
import operator
import random
import re
import struct
import time

import lz4
import numpy
import numpy.lib.recfunctions
import pandas
from scipy import ndimage
import six

# NOTE(sileht): pandas relies on time.strptime()
# and often triggers http://bugs.python.org/issue7980
# its dues to our heavy threads usage, this is the workaround
# to ensure the module is correctly loaded before we use really it.
time.strptime("2016-02-19", "%Y-%m-%d")

LOG = logging.getLogger(__name__)


class NoDeloreanAvailable(Exception):
    """Error raised when trying to insert a value that is too old."""

    def __init__(self, first_timestamp, bad_timestamp):
        self.first_timestamp = first_timestamp
        self.bad_timestamp = bad_timestamp
        super(NoDeloreanAvailable, self).__init__(
            "%s is before %s" % (bad_timestamp, first_timestamp))


class BeforeEpochError(Exception):
    """Error raised when a timestamp before Epoch is used."""

    def __init__(self, timestamp):
        self.timestamp = timestamp
        super(BeforeEpochError, self).__init__(
            "%s is before Epoch" % timestamp)


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


class InvalidData(ValueError):
    """Error raised when data are corrupted."""
    def __init__(self):
        super(InvalidData, self).__init__("Unable to unpack, invalid data")


def round_timestamp(ts, freq):
    return pandas.Timestamp(
        (pandas.Timestamp(ts).value // freq) * freq)


class GroupedTimeSeries(object):
    def __init__(self, ts, granularity):
        # NOTE(sileht): The whole class assumes ts is ordered and don't have
        # duplicate timestamps, it uses numpy.unique that sorted list, but
        # we always assume the orderd to be the same as the input.
        freq = granularity * 10e8
        self._ts = ts
        self.indexes = (numpy.array(ts.index, 'float') // freq) * freq
        self.tstamps, self.counts = numpy.unique(self.indexes,
                                                 return_counts=True)

    def mean(self):
        return self._scipy_aggregate(ndimage.mean)

    def sum(self):
        return self._scipy_aggregate(ndimage.sum)

    def min(self):
        return self._scipy_aggregate(ndimage.minimum)

    def max(self):
        return self._scipy_aggregate(ndimage.maximum)

    def median(self):
        return self._scipy_aggregate(ndimage.median)

    def std(self):
        # NOTE(sileht): ndimage.standard_deviation is really more performant
        # but it use ddof=0, to get the same result as pandas we have to use
        # ddof=1. If one day scipy allow to pass ddof, this should be changed.
        return self._scipy_aggregate(ndimage.labeled_comprehension,
                                     remove_unique=True,
                                     func=functools.partial(numpy.std, ddof=1),
                                     out_dtype='float64',
                                     default=None)

    def _count(self):
        timestamps = numpy.array(self.tstamps, 'datetime64[ns]')
        return (self.counts, timestamps)

    def count(self):
        return pandas.Series(*self._count())

    def last(self):
        counts, timestamps = self._count()
        cumcounts = numpy.cumsum(counts) - 1
        values = self._ts.values[cumcounts]
        return pandas.Series(values, pandas.to_datetime(timestamps))

    def first(self):
        counts, timestamps = self._count()
        counts = numpy.insert(counts[:-1], 0, 0)
        cumcounts = numpy.cumsum(counts)
        values = self._ts.values[cumcounts]
        return pandas.Series(values, pandas.to_datetime(timestamps))

    def quantile(self, q):
        return self._scipy_aggregate(ndimage.labeled_comprehension,
                                     func=functools.partial(
                                         numpy.percentile,
                                         q=q,
                                     ),
                                     out_dtype='float64',
                                     default=None)

    def _scipy_aggregate(self, method, remove_unique=False, *args, **kwargs):
        if remove_unique:
            locs = numpy.argwhere(self.counts > 1).T[0]

        values = method(self._ts.values, self.indexes, self.tstamps,
                        *args, **kwargs)
        timestamps = numpy.array(self.tstamps, 'datetime64[ns]')

        if remove_unique:
            timestamps = timestamps[locs]
            values = values[locs]
        return pandas.Series(values, pandas.to_datetime(timestamps))


class TimeSerie(object):
    """A representation of series of a timestamp with a value.

    Duplicate timestamps are not allowed and will be filtered to use the
    last in the group when the TimeSerie is created or extended.
    """

    def __init__(self, ts=None):
        if ts is None:
            ts = pandas.Series()
        self.ts = ts

    @staticmethod
    def clean_ts(ts):
        if ts.index.has_duplicates:
            ts = ts[~ts.index.duplicated(keep='last')]
        if not ts.index.is_monotonic:
            ts = ts.sort_index()
        return ts

    @classmethod
    def from_data(cls, timestamps=None, values=None, clean=False):
        ts = pandas.Series(values, timestamps)
        if clean:
            # For format v2
            ts = cls.clean_ts(ts)
        return cls(ts)

    @classmethod
    def from_tuples(cls, timestamps_values):
        return cls.from_data(*zip(*timestamps_values))

    def __eq__(self, other):
        return (isinstance(other, TimeSerie)
                and self.ts.all() == other.ts.all())

    def __getitem__(self, key):
        return self.ts[key]

    def set_values(self, values):
        t = pandas.Series(*reversed(list(zip(*values))))
        self.ts = self.clean_ts(t).combine_first(self.ts)

    def __len__(self):
        return len(self.ts)

    @staticmethod
    def _timestamps_and_values_from_dict(values):
        timestamps = numpy.array(list(values.keys()), dtype='datetime64[ns]')
        timestamps = pandas.to_datetime(timestamps)
        v = list(values.values())
        if v:
            return timestamps, v
        return (), ()

    @staticmethod
    def _to_offset(value):
        if isinstance(value, numbers.Real):
            return pandas.tseries.offsets.Nano(value * 10e8)
        return pandas.tseries.frequencies.to_offset(value)

    @property
    def first(self):
        try:
            return self.ts.index[0]
        except IndexError:
            return

    @property
    def last(self):
        try:
            return self.ts.index[-1]
        except IndexError:
            return

    def group_serie(self, granularity, start=0):
        # NOTE(jd) Our whole serialization system is based on Epoch, and we
        # store unsigned integer, so we can't store anything before Epoch.
        # Sorry!
        if not self.ts.empty and self.ts.index[0].value < 0:
            raise BeforeEpochError(self.ts.index[0])

        return GroupedTimeSeries(self.ts[start:], granularity)


class BoundTimeSerie(TimeSerie):
    def __init__(self, ts=None, block_size=None, back_window=0):
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
        super(BoundTimeSerie, self).__init__(ts)
        self.block_size = self._to_offset(block_size)
        self.back_window = back_window
        self._truncate()

    @classmethod
    def from_data(cls, timestamps=None, values=None,
                  block_size=None, back_window=0):
        return cls(pandas.Series(values, timestamps),
                   block_size=block_size, back_window=back_window)

    def __eq__(self, other):
        return (isinstance(other, BoundTimeSerie)
                and super(BoundTimeSerie, self).__eq__(other)
                and self.block_size == other.block_size
                and self.back_window == other.back_window)

    def set_values(self, values, before_truncate_callback=None,
                   ignore_too_old_timestamps=False):
        # NOTE: values must be sorted when passed in.
        if self.block_size is not None and not self.ts.empty:
            first_block_timestamp = self.first_block_timestamp()
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

    _SERIALIZATION_TIMESTAMP_VALUE_LEN = struct.calcsize("<Qd")
    _SERIALIZATION_TIMESTAMP_LEN = struct.calcsize("<Q")

    @classmethod
    def unserialize(cls, data, block_size, back_window):
        uncompressed = lz4.loads(data)
        nb_points = (
            len(uncompressed) // cls._SERIALIZATION_TIMESTAMP_VALUE_LEN
        )
        timestamps_raw = uncompressed[
            :nb_points*cls._SERIALIZATION_TIMESTAMP_LEN]
        timestamps = numpy.frombuffer(timestamps_raw, dtype='<Q')
        timestamps = numpy.cumsum(timestamps)
        timestamps = numpy.array(timestamps, dtype='datetime64[ns]')

        values_raw = uncompressed[nb_points*cls._SERIALIZATION_TIMESTAMP_LEN:]
        values = numpy.frombuffer(values_raw, dtype='<d')

        return cls.from_data(
            pandas.to_datetime(timestamps),
            values,
            block_size=block_size,
            back_window=back_window)

    def serialize(self):
        # NOTE(jd) Use a double delta encoding for timestamps
        timestamps = numpy.insert(numpy.diff(self.ts.index),
                                  0, self.first.value)
        timestamps = numpy.array(timestamps, dtype='<Q')
        values = numpy.array(self.ts.values, dtype='<d')
        payload = (timestamps.tobytes() + values.tobytes())
        return lz4.dumps(payload)

    @classmethod
    def benchmark(cls):
        """Run a speed benchmark!"""
        points = SplitKey.POINTS_PER_SPLIT
        serialize_times = 50

        now = datetime.datetime(2015, 4, 3, 23, 11)

        print(cls.__name__)
        print("=" * len(cls.__name__))

        for title, values in [
                ("Simple continuous range", six.moves.range(points)),
                ("All 0", [float(0)] * points),
                ("All 1", [float(1)] * points),
                ("0 and 1", [0, 1] * (points // 2)),
                ("1 and 0 random",
                 [random.randint(0, 1)
                  for x in six.moves.range(points)]),
                ("Small number random pos/neg",
                 [random.randint(-100000, 10000)
                  for x in six.moves.range(points)]),
                ("Small number random pos",
                 [random.randint(0, 20000) for x in six.moves.range(points)]),
                ("Small number random neg",
                 [random.randint(-20000, 0) for x in six.moves.range(points)]),
                ("Sin(x)", map(math.sin, six.moves.range(points))),
                ("random ", [random.random()
                             for x in six.moves.range(points)]),
        ]:
            print(title)
            pts = pandas.Series(values,
                                [now + datetime.timedelta(
                                    seconds=i * random.randint(1, 10),
                                    microseconds=random.randint(1, 999999))
                                 for i in six.moves.range(points)])
            pts = pts.sort_index()
            ts = cls(ts=pts)
            t0 = time.time()
            for i in six.moves.range(serialize_times):
                s = ts.serialize()
            t1 = time.time()
            print("  Serialization speed: %.2f MB/s"
                  % (((points * 2 * 8)
                      / ((t1 - t0) / serialize_times)) / (1024.0 * 1024.0)))
            print("   Bytes per point: %.2f" % (len(s) / float(points)))

            t0 = time.time()
            for i in six.moves.range(serialize_times):
                cls.unserialize(s, 1, 1)
            t1 = time.time()
            print("  Unserialization speed: %.2f MB/s"
                  % (((points * 2 * 8)
                      / ((t1 - t0) / serialize_times)) / (1024.0 * 1024.0)))

    def first_block_timestamp(self):
        """Return the timestamp of the first block."""
        rounded = round_timestamp(self.ts.index[-1],
                                  self.block_size.delta.value)

        return rounded - (self.block_size * self.back_window)

    def _truncate(self):
        """Truncate the timeserie."""
        if self.block_size is not None and not self.ts.empty:
            # Change that to remove the amount of block needed to have
            # the size <= max_size. A block is a number of "seconds" (a
            # timespan)
            self.ts = self.ts[self.first_block_timestamp():]


@functools.total_ordering
class SplitKey(object):
    """A class representing a split key.

    A split key is basically a timestamp that can be used to split
    `AggregatedTimeSerie` objects in multiple parts. Each part will contain
    `SplitKey.POINTS_PER_SPLIT` points. The split key for a given granularity
    are regularly spaced.
    """

    POINTS_PER_SPLIT = 3600

    def __init__(self, value, sampling):
        if isinstance(value, SplitKey):
            self.key = value.key
        elif isinstance(value, pandas.Timestamp):
            self.key = value.value / 10e8
        else:
            self.key = float(value)

        self._carbonara_sampling = float(sampling)

    @classmethod
    def from_timestamp_and_sampling(cls, timestamp, sampling):
        return cls(
            round_timestamp(
                timestamp, freq=sampling * cls.POINTS_PER_SPLIT * 10e8),
            sampling)

    def __next__(self):
        """Get the split key of the next split.

        :return: A `SplitKey` object.
        """
        return self.__class__(
            self.key + self._carbonara_sampling * self.POINTS_PER_SPLIT,
            self._carbonara_sampling)

    next = __next__

    def __iter__(self):
        return self

    def __hash__(self):
        return hash(self.key)

    def _compare(self, op, other):
        if isinstance(other, SplitKey):
            return op(self.key, other.key)
        if isinstance(other, pandas.Timestamp):
            return op(self.key * 10e8, other.value)
        return op(self.key, other)

    def __lt__(self, other):
        return self._compare(operator.lt, other)

    def __eq__(self, other):
        return self._compare(operator.eq, other)

    def __ne__(self, other):
        # neither total_ordering nor py2 sets ne as the opposite of eq
        return self._compare(operator.ne, other)

    def __str__(self):
        return str(float(self))

    def __float__(self):
        return self.key

    def as_datetime(self):
        return pandas.Timestamp(self.key, unit='s')

    def __repr__(self):
        return "<%s: %s / %fs>" % (self.__class__.__name__,
                                   repr(self.key),
                                   self._carbonara_sampling)


class AggregatedTimeSerie(TimeSerie):

    _AGG_METHOD_PCT_RE = re.compile(r"([1-9][0-9]?)pct")

    PADDED_SERIAL_LEN = struct.calcsize("<?d")
    COMPRESSED_SERIAL_LEN = struct.calcsize("<Hd")
    COMPRESSED_TIMESPAMP_LEN = struct.calcsize("<H")

    def __init__(self, sampling, aggregation_method, ts=None, max_size=None):
        """A time serie that is downsampled.

        Used to represent the downsampled timeserie for a single
        granularity/aggregation-function pair stored for a metric.

        """
        super(AggregatedTimeSerie, self).__init__(ts)
        self.sampling = self._to_offset(sampling).nanos / 10e8
        self.max_size = max_size
        self.aggregation_method = aggregation_method
        self._truncate(quick=True)

    def resample(self, sampling):
        return AggregatedTimeSerie.from_grouped_serie(
            self.group_serie(sampling), sampling, self.aggregation_method)

    @classmethod
    def from_data(cls, sampling, aggregation_method, timestamps=None,
                  values=None, max_size=None):
        return cls(sampling=sampling,
                   aggregation_method=aggregation_method,
                   ts=pandas.Series(values, timestamps),
                   max_size=max_size)

    @staticmethod
    def _get_agg_method(aggregation_method):
        q = None
        m = AggregatedTimeSerie._AGG_METHOD_PCT_RE.match(aggregation_method)
        if m:
            q = float(m.group(1))
            aggregation_method_func_name = 'quantile'
        else:
            if not hasattr(GroupedTimeSeries, aggregation_method):
                raise UnknownAggregationMethod(aggregation_method)
            aggregation_method_func_name = aggregation_method
        return aggregation_method_func_name, q

    def split(self):
        # NOTE(sileht): We previously use groupby with
        # SplitKey.from_timestamp_and_sampling, but
        # this is slow because pandas can do that on any kind DataFrame
        # but we have ordered timestamps, so don't need
        # to iter the whole series.
        freq = self.sampling * SplitKey.POINTS_PER_SPLIT
        ix = numpy.array(self.ts.index, 'float64') / 10e8
        keys, counts = numpy.unique((ix // freq) * freq, return_counts=True)
        start = 0
        for key, count in six.moves.zip(keys, counts):
            end = start + count
            if key == -0.0:
                key = abs(key)
            yield (SplitKey(key, self.sampling),
                   AggregatedTimeSerie(self.sampling, self.aggregation_method,
                                       self.ts[start:end]))
            start = end

    @classmethod
    def from_timeseries(cls, timeseries, sampling, aggregation_method,
                        max_size=None):
        ts = pandas.Series()
        for t in timeseries:
            ts = ts.combine_first(t.ts)
        return cls(sampling=sampling,
                   aggregation_method=aggregation_method,
                   ts=ts, max_size=max_size)

    @classmethod
    def from_grouped_serie(cls, grouped_serie, sampling, aggregation_method,
                           max_size=None):
        agg_name, q = cls._get_agg_method(aggregation_method)
        return cls(sampling, aggregation_method,
                   ts=cls._resample_grouped(grouped_serie, agg_name,
                                            q),
                   max_size=max_size)

    def __eq__(self, other):
        return (isinstance(other, AggregatedTimeSerie)
                and super(AggregatedTimeSerie, self).__eq__(other)
                and self.max_size == other.max_size
                and self.sampling == other.sampling
                and self.aggregation_method == other.aggregation_method)

    def __repr__(self):
        return "<%s 0x%x sampling=%fs max_size=%s agg_method=%s>" % (
            self.__class__.__name__,
            id(self),
            self.sampling,
            self.max_size,
            self.aggregation_method,
        )

    @staticmethod
    def is_compressed(serialized_data):
        """Check whatever the data was serialized with compression."""
        return six.indexbytes(serialized_data, 0) == ord("c")

    @classmethod
    def unserialize(cls, data, start, agg_method, sampling):
        x, y = [], []

        start = float(start)
        if data:
            if cls.is_compressed(data):
                # Compressed format
                uncompressed = lz4.loads(memoryview(data)[1:].tobytes())
                nb_points = len(uncompressed) // cls.COMPRESSED_SERIAL_LEN

                timestamps_raw = uncompressed[
                    :nb_points*cls.COMPRESSED_TIMESPAMP_LEN]
                try:
                    y = numpy.frombuffer(timestamps_raw, dtype='<H')
                except ValueError:
                    raise InvalidData()
                y = numpy.cumsum(y * sampling) + start

                values_raw = uncompressed[
                    nb_points*cls.COMPRESSED_TIMESPAMP_LEN:]
                x = numpy.frombuffer(values_raw, dtype='<d')

            else:
                # Padded format
                try:
                    everything = numpy.frombuffer(data, dtype=[('b', '<?'),
                                                               ('v', '<d')])
                except ValueError:
                    raise InvalidData()
                index = numpy.nonzero(everything['b'])[0]
                y = index * sampling + start
                x = everything['v'][index]

            y = numpy.array(y, dtype='float64') * 10e8
            y = numpy.array(y, dtype='datetime64[ns]')
            y = pandas.to_datetime(y)
        return cls.from_data(sampling, agg_method, y, x)

    def get_split_key(self, timestamp=None):
        """Return the split key for a particular timestamp.

        :param timestamp: If None, the first timestamp of the timeserie
                          is used.
        :return: A SplitKey object.
        """
        if timestamp is None:
            timestamp = self.first
        return SplitKey.from_timestamp_and_sampling(
            timestamp, self.sampling)

    def serialize(self, start, compressed=True):
        """Serialize an aggregated timeserie.

        The serialization starts with a byte that indicate the serialization
        format: 'c' for compressed format, '\x00' or '\x01' for uncompressed
        format. Both format can be unserialized using the `unserialize` method.

        The offset returned indicates at which offset the data should be
        written from. In the case of compressed data, this is always 0.

        :param start: Timestamp to start serialization at.
        :param compressed: Serialize in a compressed format.
        :return: a tuple of (offset, data)

        """
        if not self.ts.index.is_monotonic:
            self.ts = self.ts.sort_index()
        offset_div = self.sampling * 10e8
        if isinstance(start, SplitKey):
            start = start.as_datetime().value
        else:
            start = pandas.Timestamp(start).value
        # calculate how many seconds from start the series runs until and
        # initialize list to store alternating delimiter, float entries
        if compressed:
            # NOTE(jd) Use a double delta encoding for timestamps
            timestamps = numpy.insert(
                numpy.diff(self.ts.index) // offset_div,
                0, int((self.first.value - start) // offset_div))
            timestamps = numpy.array(timestamps, dtype='<H')
            values = numpy.array(self.ts.values, dtype='<d')
            payload = (timestamps.tobytes() + values.tobytes())
            return None, b"c" + lz4.dumps(payload)
        # NOTE(gordc): this binary serializes series based on the split
        # time. the format is 1B True/False flag which denotes whether
        # subsequent 8B is a real float or zero padding. every 9B
        # represents one second from start time. this is intended to be run
        # on data already split. ie. False,0,True,0 serialization means
        # start datapoint is padding, and 1s after start time, the
        # aggregate value is 0. calculate how many seconds from start the
        # series runs until and initialize list to store alternating
        # delimiter, float entries
        first = self.first.value  # NOTE(jd) needed because faster
        e_offset = int((self.last.value - first) // offset_div) + 1

        locs = (numpy.cumsum(numpy.diff(self.ts.index)) // offset_div)
        locs = numpy.insert(locs, 0, 0)
        locs = numpy.array(locs, dtype='int')

        # Fill everything with zero
        serial_dtype = [('b', '<?'), ('v', '<d')]
        serial = numpy.zeros((e_offset,), dtype=serial_dtype)

        # Create a structured array with two dimensions
        values = numpy.array(self.ts.values, dtype='<d')
        ones = numpy.ones_like(values, dtype='<?')
        values = numpy.core.records.fromarrays((ones, values),
                                               dtype=serial_dtype)

        serial[locs] = values

        payload = serial.tobytes()
        offset = int((first - start) // offset_div) * self.PADDED_SERIAL_LEN
        return offset, payload

    def _truncate(self, quick=False):
        """Truncate the timeserie."""
        if self.max_size is not None:
            # Remove empty points if any that could be added by aggregation
            self.ts = (self.ts[-self.max_size:] if quick
                       else self.ts.dropna()[-self.max_size:])

    @staticmethod
    def _resample_grouped(grouped_serie, agg_name, q=None):
        agg_func = getattr(grouped_serie, agg_name)
        return agg_func(q) if agg_name == 'quantile' else agg_func()

    def fetch(self, from_timestamp=None, to_timestamp=None):
        """Fetch aggregated time value.

        Returns a sorted list of tuples (timestamp, granularity, value).
        """
        # Round timestamp to our granularity so we're sure that if e.g. 17:02
        # is requested and we have points for 17:00 and 17:05 in a 5min
        # granularity, we do return the 17:00 point and not nothing
        if from_timestamp is None:
            from_ = None
        else:
            from_ = round_timestamp(from_timestamp, self.sampling * 10e8)
        points = self[from_:to_timestamp]
        try:
            # Do not include stop timestamp
            del points[to_timestamp]
        except KeyError:
            pass
        return [(timestamp, self.sampling, value)
                for timestamp, value
                in six.iteritems(points)]

    def merge(self, ts):
        """Merge a timeserie into this one.

        This is equivalent to `update` but is faster as they are is no
        resampling. Be careful on what you merge.
        """
        self.ts = self.ts.combine_first(ts.ts)

    @classmethod
    def benchmark(cls):
        """Run a speed benchmark!"""
        points = SplitKey.POINTS_PER_SPLIT
        sampling = 5
        resample = 35

        now = datetime.datetime(2015, 4, 3, 23, 11)

        print(cls.__name__)
        print("=" * len(cls.__name__))

        for title, values in [
                ("Simple continuous range", six.moves.range(points)),
                ("All 0", [float(0)] * points),
                ("All 1", [float(1)] * points),
                ("0 and 1", [0, 1] * (points // 2)),
                ("1 and 0 random",
                 [random.randint(0, 1)
                  for x in six.moves.range(points)]),
                ("Small number random pos/neg",
                 [random.randint(-100000, 10000)
                  for x in six.moves.range(points)]),
                ("Small number random pos",
                 [random.randint(0, 20000) for x in six.moves.range(points)]),
                ("Small number random neg",
                 [random.randint(-20000, 0) for x in six.moves.range(points)]),
                ("Sin(x)", map(math.sin, six.moves.range(points))),
                ("random ", [random.random()
                             for x in six.moves.range(points)]),
        ]:
            print(title)
            serialize_times = 50
            pts = pandas.Series(values,
                                [now + datetime.timedelta(seconds=i*sampling)
                                 for i in six.moves.range(points)])
            pts = pts.sort_index()
            ts = cls(ts=pts, sampling=sampling, aggregation_method='mean')
            t0 = time.time()
            key = ts.get_split_key()
            for i in six.moves.range(serialize_times):
                e, s = ts.serialize(key, compressed=False)
            t1 = time.time()
            print("  Uncompressed serialization speed: %.2f MB/s"
                  % (((points * 2 * 8)
                      / ((t1 - t0) / serialize_times)) / (1024.0 * 1024.0)))
            print("   Bytes per point: %.2f" % (len(s) / float(points)))

            t0 = time.time()
            for i in six.moves.range(serialize_times):
                cls.unserialize(s, key, 'mean', sampling)
            t1 = time.time()
            print("  Unserialization speed: %.2f MB/s"
                  % (((points * 2 * 8)
                      / ((t1 - t0) / serialize_times)) / (1024.0 * 1024.0)))

            t0 = time.time()
            for i in six.moves.range(serialize_times):
                o, s = ts.serialize(key, compressed=True)
            t1 = time.time()
            print("  Compressed serialization speed: %.2f MB/s"
                  % (((points * 2 * 8)
                      / ((t1 - t0) / serialize_times)) / (1024.0 * 1024.0)))
            print("   Bytes per point: %.2f" % (len(s) / float(points)))

            t0 = time.time()
            for i in six.moves.range(serialize_times):
                cls.unserialize(s, key, 'mean', sampling)
            t1 = time.time()
            print("  Uncompression speed: %.2f MB/s"
                  % (((points * 2 * 8)
                      / ((t1 - t0) / serialize_times)) / (1024.0 * 1024.0)))

            t0 = time.time()
            for i in six.moves.range(serialize_times):
                list(ts.split())
            t1 = time.time()
            print("  split() speed: %.8f s" % ((t1 - t0) / serialize_times))

            # NOTE(sileht): propose a new series with half overload timestamps
            pts = ts.ts.copy(deep=True)
            tsbis = cls(ts=pts, sampling=sampling, aggregation_method='mean')
            tsbis.ts.reindex(tsbis.ts.index -
                             datetime.timedelta(seconds=sampling * points / 2))

            t0 = time.time()
            for i in six.moves.range(serialize_times):
                ts.merge(tsbis)
            t1 = time.time()
            print("  merge() speed: %.8f s" % ((t1 - t0) / serialize_times))

            for agg in ['mean', 'sum', 'max', 'min', 'std', 'median', 'first',
                        'last', 'count', '5pct', '90pct']:
                serialize_times = 3 if agg.endswith('pct') else 10
                ts = cls(ts=pts, sampling=sampling, aggregation_method=agg)
                t0 = time.time()
                for i in six.moves.range(serialize_times):
                    ts.resample(resample)
                t1 = time.time()
                print("  resample(%s) speed: %.8f s" % (agg, (t1 - t0) /
                                                        serialize_times))

    @staticmethod
    def aggregated(timeseries, aggregation, from_timestamp=None,
                   to_timestamp=None, needed_percent_of_overlap=100.0,
                   fill=None):

        index = ['timestamp', 'granularity']
        columns = ['timestamp', 'granularity', 'value']
        dataframes = []

        if not timeseries:
            return []

        for timeserie in timeseries:
            timeserie_raw = timeserie.fetch(from_timestamp, to_timestamp)

            if timeserie_raw:
                dataframe = pandas.DataFrame(timeserie_raw, columns=columns)
                dataframe = dataframe.set_index(index)
                dataframes.append(dataframe)

        if not dataframes:
            return []

        number_of_distinct_datasource = len(timeseries) / len(
            set(ts.sampling for ts in timeseries)
        )

        left_boundary_ts = None
        right_boundary_ts = None
        if fill is not None:
            fill_df = pandas.concat(dataframes, axis=1)
            if fill != 'null':
                fill_df = fill_df.fillna(fill)
            single_df = pandas.concat([series for __, series in
                                       fill_df.iteritems()]).to_frame()
            grouped = single_df.groupby(level=index)
        else:
            grouped = pandas.concat(dataframes).groupby(level=index)
            maybe_next_timestamp_is_left_boundary = False

            left_holes = 0
            right_holes = 0
            holes = 0
            for (timestamp, __), group in grouped:
                if group.count()['value'] != number_of_distinct_datasource:
                    maybe_next_timestamp_is_left_boundary = True
                    if left_boundary_ts is not None:
                        right_holes += 1
                    else:
                        left_holes += 1
                elif maybe_next_timestamp_is_left_boundary:
                    left_boundary_ts = timestamp
                    maybe_next_timestamp_is_left_boundary = False
                else:
                    right_boundary_ts = timestamp
                    holes += right_holes
                    right_holes = 0

            if to_timestamp is not None:
                holes += left_holes
            if from_timestamp is not None:
                holes += right_holes

            if to_timestamp is not None or from_timestamp is not None:
                maximum = len(grouped)
                percent_of_overlap = (float(maximum - holes) * 100.0 /
                                      float(maximum))
                if percent_of_overlap < needed_percent_of_overlap:
                    raise UnAggregableTimeseries(
                        'Less than %f%% of datapoints overlap in this '
                        'timespan (%.2f%%)' % (needed_percent_of_overlap,
                                               percent_of_overlap))
            if (needed_percent_of_overlap > 0 and
                    (right_boundary_ts == left_boundary_ts or
                     (right_boundary_ts is None
                      and maybe_next_timestamp_is_left_boundary))):
                LOG.debug("We didn't find points that overlap in those "
                          "timeseries. "
                          "right_boundary_ts=%(right_boundary_ts)s, "
                          "left_boundary_ts=%(left_boundary_ts)s, "
                          "groups=%(groups)s", {
                              'right_boundary_ts': right_boundary_ts,
                              'left_boundary_ts': left_boundary_ts,
                              'groups': list(grouped)
                          })
                raise UnAggregableTimeseries('No overlap')

        # NOTE(sileht): this call the aggregation method on already
        # aggregated values, for some kind of aggregation this can
        # result can looks weird, but this is the best we can do
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


if __name__ == '__main__':
    import sys
    args = sys.argv[1:]
    if not args or "--boundtimeserie" in args:
        BoundTimeSerie.benchmark()
    if not args or "--aggregatedtimeserie" in args:
        AggregatedTimeSerie.benchmark()
