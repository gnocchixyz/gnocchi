# -*- encoding: utf-8 -*-
#
# Copyright © 2016-2018 Red Hat, Inc.
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

import collections
import functools
import math
import operator
import random
import re
import struct
import time

import lz4.block
import numpy
import six

from gnocchi import calendar


UNIX_UNIVERSAL_START64 = numpy.datetime64("1970", 'ns')
ONE_SECOND = numpy.timedelta64(1, 's')


class BeforeEpochError(Exception):
    """Error raised when a timestamp before Epoch is used."""

    def __init__(self, timestamp):
        self.timestamp = timestamp
        super(BeforeEpochError, self).__init__(
            "%s is before Epoch" % timestamp)


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


def datetime64_to_epoch(dt):
    return (dt - UNIX_UNIVERSAL_START64) / ONE_SECOND


def round_timestamp(ts, freq):
    return UNIX_UNIVERSAL_START64 + numpy.floor(
        (ts - UNIX_UNIVERSAL_START64) / freq) * freq


TIMESERIES_ARRAY_DTYPE = [('timestamps', '<datetime64[ns]'),
                          ('values', '<d')]


def make_timeseries(timestamps, values):
    """Return a Numpy array representing a timeseries.

    This array specifies correctly the data types, which is important for
    Numpy to operate fastly.
    """
    l = len(timestamps)
    if l != len(values):
        raise ValueError("Timestamps and values must have the same length")
    arr = numpy.zeros(l, dtype=TIMESERIES_ARRAY_DTYPE)
    arr['timestamps'] = timestamps
    arr['values'] = values
    return arr


def combine_timeseries(ts1, ts2):
    """Combine a timeseries into this one.

    The timeseries does not need to be sorted.

    If a timestamp is present in both `ts1` and `ts2`, then value from `ts1`
    is used.

    :param ts: The timeseries to combine.
    :return: A new timeseries.
    """
    ts = numpy.concatenate((ts1, ts2))
    _, index = numpy.unique(ts['timestamps'], return_index=True)
    return ts[index]


class GroupedTimeSeries(object):
    def __init__(self, ts, granularity, start=None):
        # NOTE(sileht): The whole class assumes ts is ordered and don't have
        # duplicate timestamps, it uses numpy.unique that sorted list, but
        # we always assume the orderd to be the same as the input.
        self.granularity = granularity
        self.can_derive = isinstance(granularity, numpy.timedelta64)
        self.start = start
        if start is None:
            self._ts = ts
            self._ts_for_derive = ts
        else:
            self._ts = ts[numpy.searchsorted(ts['timestamps'], start):]
            if self.can_derive:
                start_derive = start - granularity
                self._ts_for_derive = ts[
                    numpy.searchsorted(ts['timestamps'], start_derive):
                ]
        if self.can_derive:
            self.indexes = round_timestamp(self._ts['timestamps'], granularity)
        elif calendar.GROUPINGS.get(granularity):
            self.indexes = calendar.GROUPINGS.get(granularity)(
                self._ts['timestamps'])
        self.tstamps, self.counts = numpy.unique(self.indexes,
                                                 return_counts=True)

    def mean(self):
        series = self.sum()
        series['values'] /= self.counts
        return series

    def sum(self):
        return make_timeseries(self.tstamps, numpy.bincount(
            numpy.repeat(numpy.arange(self.counts.size), self.counts),
            weights=self._ts['values']))

    def min(self):
        ordered = self._ts['values'].argsort()
        uniq_inv = numpy.repeat(numpy.arange(self.counts.size), self.counts)
        values = numpy.zeros(self.tstamps.size)
        values[uniq_inv[ordered][::-1]] = self._ts['values'][ordered][::-1]
        return make_timeseries(self.tstamps, values)

    def max(self):
        ordered = self._ts['values'].argsort()
        uniq_inv = numpy.repeat(numpy.arange(self.counts.size), self.counts)
        values = numpy.zeros(self.tstamps.size)
        values[uniq_inv[ordered]] = self._ts['values'][ordered]
        return make_timeseries(self.tstamps, values)

    def median(self):
        ordered = numpy.lexsort((self._ts['values'], self.indexes))
        # TODO(gordc): can use np.divmod when centos supports numpy 1.13
        mid_diff = numpy.floor_divide(self.counts, 2)
        odd = numpy.mod(self.counts, 2)
        mid_floor = (numpy.cumsum(self.counts) - 1) - mid_diff
        mid_ceil = mid_floor + (odd + 1) % 2
        return make_timeseries(
            self.tstamps, (self._ts['values'][ordered][mid_floor] +
                           self._ts['values'][ordered][mid_ceil]) / 2.0)

    def std(self):
        mean_ts = self.mean()
        diff_sq = numpy.square(self._ts['values'] -
                               numpy.repeat(mean_ts['values'], self.counts))
        bin_sum = numpy.bincount(
            numpy.repeat(numpy.arange(self.counts.size), self.counts),
            weights=diff_sq)
        return make_timeseries(self.tstamps[self.counts > 1],
                               numpy.sqrt(bin_sum[self.counts > 1] /
                                          (self.counts[self.counts > 1] - 1)))

    def count(self):
        return make_timeseries(self.tstamps, self.counts)

    def last(self):
        cumcounts = numpy.cumsum(self.counts) - 1
        values = self._ts['values'][cumcounts]
        return make_timeseries(self.tstamps, values)

    def first(self):
        cumcounts = numpy.cumsum(self.counts) - self.counts
        values = self._ts['values'][cumcounts]
        return make_timeseries(self.tstamps, values)

    def quantile(self, q):
        ordered = numpy.lexsort((self._ts['values'], self.indexes))
        min_pos = numpy.cumsum(self.counts) - self.counts
        real_pos = min_pos + (self.counts - 1) * (q / 100)
        floor_pos = numpy.floor(real_pos).astype(numpy.int, copy=False)
        ceil_pos = numpy.ceil(real_pos).astype(numpy.int, copy=False)
        values = (
            self._ts['values'][ordered][floor_pos] * (ceil_pos - real_pos) +
            self._ts['values'][ordered][ceil_pos] * (real_pos - floor_pos))
        # NOTE(gordc): above code doesn't compute proper value if pct lands on
        # exact index, it sets it to 0. we need to set it properly here
        exact_pos = numpy.equal(floor_pos, ceil_pos)
        values[exact_pos] = self._ts['values'][ordered][floor_pos][exact_pos]
        return make_timeseries(self.tstamps, values)

    def derived(self):
        if not self.can_derive:
            raise TypeError('Cannot derive aggregates on calendar '
                            'granularities.')
        timestamps = self._ts_for_derive['timestamps'][1:]
        values = numpy.diff(self._ts_for_derive['values'])
        # FIXME(sileht): create some alternative __init__ to avoid creating
        # useless Numpy object, recounting, timestamps convertion, ...
        return GroupedTimeSeries(make_timeseries(timestamps, values),
                                 self.granularity, self.start)


class TimeSerie(object):
    """A representation of series of a timestamp with a value.

    Duplicate timestamps are not allowed and will be filtered to use the
    last in the group when the TimeSerie is created or extended.
    """

    def __init__(self, ts=None):
        if ts is None:
            ts = make_timeseries([], [])
        self.ts = ts

    def __iter__(self):
        return six.moves.zip(self.ts['timestamps'], self.ts['values'])

    @classmethod
    def from_data(cls, timestamps=None, values=None):
        return cls(make_timeseries(timestamps, values))

    def __eq__(self, other):
        return (isinstance(other, TimeSerie) and
                numpy.array_equal(self.ts,  other.ts))

    def __getitem__(self, key):
        if isinstance(key, numpy.datetime64):
            idx = numpy.searchsorted(self.timestamps, key)
            if self.timestamps[idx] == key:
                return self[idx]
            raise KeyError(key)
        if isinstance(key, slice):
            if isinstance(key.start, numpy.datetime64):
                start = numpy.searchsorted(self.timestamps, key.start)
            else:
                start = key.start
            if isinstance(key.stop, numpy.datetime64):
                stop = numpy.searchsorted(self.timestamps, key.stop)
            else:
                stop = key.stop
            key = slice(start, stop, key.step)
        return self.ts[key]

    def _merge(self, ts):
        """Merge a Numpy timeseries into this one."""
        self.ts = combine_timeseries(ts, self.ts)

    def merge(self, ts):
        """Merge a TimeSerie into this one."""
        return self._merge(ts.ts)

    def set_values(self, values):
        """Set values into this timeseries.

        :param values: A list of tuple (timestamp, value).
        """
        return self._merge(values)

    def __len__(self):
        return len(self.ts)

    @property
    def timestamps(self):
        return self.ts['timestamps']

    @property
    def values(self):
        return self.ts['values']

    @property
    def first(self):
        try:
            return self.timestamps[0]
        except IndexError:
            return

    @property
    def last(self):
        try:
            return self.timestamps[-1]
        except IndexError:
            return

    def group_serie(self, granularity, start=None):
        # NOTE(jd) Our whole serialization system is based on Epoch, and we
        # store unsigned integer, so we can't store anything before Epoch.
        # Sorry!
        if len(self.ts) != 0 and self.first < UNIX_UNIVERSAL_START64:
            raise BeforeEpochError(self.first)

        return GroupedTimeSeries(self.ts, granularity, start)

    @staticmethod
    def _compress(payload):
        # FIXME(jd) lz4 > 0.9.2 returns bytearray instead of bytes. But Cradox
        # does not accept bytearray but only bytes, so make sure that we have a
        # byte type returned.
        return memoryview(lz4.block.compress(payload)).tobytes()


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
        self.block_size = block_size
        self.back_window = back_window

    @classmethod
    def from_data(cls, timestamps=None, values=None,
                  block_size=None, back_window=0):
        return cls(make_timeseries(timestamps, values),
                   block_size=block_size, back_window=back_window)

    def __eq__(self, other):
        return (isinstance(other, BoundTimeSerie)
                and super(BoundTimeSerie, self).__eq__(other)
                and self.block_size == other.block_size
                and self.back_window == other.back_window)

    def set_values(self, values, before_truncate_callback=None):
        """Set the timestamps and values in this timeseries.

        :param values: A sorted timeseries array.
        :param before_truncate_callback: A callback function to call before
                                         truncating the BoundTimeSerie to its
                                         maximum size.
        :return: None of the return value of before_truncate_callback
        """
        if self.block_size is not None and len(self.ts) != 0:
            index = numpy.searchsorted(values['timestamps'],
                                       self.first_block_timestamp())
            values = values[index:]
        super(BoundTimeSerie, self).set_values(values)
        if before_truncate_callback:
            return_value = before_truncate_callback(self)
        else:
            return_value = None
        self._truncate()
        return return_value

    _SERIALIZATION_TIMESTAMP_VALUE_LEN = struct.calcsize("<Qd")
    _SERIALIZATION_TIMESTAMP_LEN = struct.calcsize("<Q")

    @classmethod
    def unserialize(cls, data, block_size, back_window):
        uncompressed = lz4.block.decompress(data)
        nb_points = (
            len(uncompressed) // cls._SERIALIZATION_TIMESTAMP_VALUE_LEN
        )

        try:
            timestamps = numpy.frombuffer(uncompressed, dtype='<Q',
                                          count=nb_points)
            values = numpy.frombuffer(
                uncompressed, dtype='<d',
                offset=nb_points * cls._SERIALIZATION_TIMESTAMP_LEN)
        except ValueError:
            raise InvalidData

        return cls.from_data(
            numpy.cumsum(timestamps),
            values,
            block_size=block_size,
            back_window=back_window)

    def serialize(self):
        # NOTE(jd) Use a double delta encoding for timestamps
        timestamps = numpy.empty(self.timestamps.size, dtype='<Q')
        timestamps[0] = self.first
        timestamps[1:] = numpy.diff(self.timestamps)
        return self._compress(timestamps.tobytes() + self.values.tobytes())

    @classmethod
    def benchmark(cls):
        """Run a speed benchmark!"""
        points = SplitKey.POINTS_PER_SPLIT
        serialize_times = 50

        now = numpy.datetime64("2015-04-03 23:11")
        timestamps = numpy.sort(numpy.array(
            [now + numpy.timedelta64(random.randint(1000000, 10000000), 'us')
             for i in six.moves.range(points)]))

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
                ("Sin(x)", list(map(math.sin, six.moves.range(points)))),
                ("random ", [random.random()
                             for x in six.moves.range(points)]),
        ]:
            print(title)
            ts = cls.from_data(timestamps, values)
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
                cls.unserialize(s, ONE_SECOND, 1)
            t1 = time.time()
            print("  Unserialization speed: %.2f MB/s"
                  % (((points * 2 * 8)
                      / ((t1 - t0) / serialize_times)) / (1024.0 * 1024.0)))

    def first_block_timestamp(self):
        """Return the timestamp of the first block."""
        rounded = round_timestamp(self.timestamps[-1], self.block_size)
        return rounded - (self.block_size * self.back_window)

    def _truncate(self):
        """Truncate the timeserie."""
        if self.block_size is not None and len(self.ts) != 0:
            # Change that to remove the amount of block needed to have
            # the size <= max_size. A block is a number of "seconds" (a
            # timespan)
            self.ts = self[self.first_block_timestamp():]


@functools.total_ordering
class SplitKey(object):
    """A class representing a split key.

    A split key is basically a timestamp that can be used to split
    `AggregatedTimeSerie` objects in multiple parts. Each part will contain
    `SplitKey.POINTS_PER_SPLIT` points. The split key for a given granularity
    are regularly spaced.
    """

    __slots__ = (
        'key',
        'sampling',
    )

    POINTS_PER_SPLIT = 3600

    def __init__(self, value, sampling):
        if isinstance(value, SplitKey):
            self.key = value.key
        else:
            self.key = value

        self.sampling = sampling

    @classmethod
    def from_timestamp_and_sampling(cls, timestamp, sampling):
        return cls(
            round_timestamp(
                timestamp,
                freq=sampling * cls.POINTS_PER_SPLIT),
            sampling)

    def __next__(self):
        """Get the split key of the next split.

        :return: A `SplitKey` object.
        """
        return self.__class__(
            self.key + self.sampling * self.POINTS_PER_SPLIT,
            self.sampling)

    next = __next__

    def __iter__(self):
        return self

    def __hash__(self):
        return hash(str(self.key.astype('datetime64[ns]')) +
                    str(self.sampling.astype('timedelta64[ns]')))

    def _compare(self, op, other):
        if isinstance(other, SplitKey):
            if self.sampling != other.sampling:
                if op == operator.eq:
                    return False
                if op == operator.ne:
                    return True
                raise TypeError(
                    "Cannot compare %s with different sampling" %
                    self.__class__.__name__)
            return op(self.key, other.key)
        if isinstance(other, numpy.datetime64):
            return op(self.key, other)
        raise TypeError("Cannot compare %r with %r" % (self, other))

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
        return datetime64_to_epoch(self.key)

    def __repr__(self):
        return "<%s: %s / %s>" % (self.__class__.__name__,
                                  self.key,
                                  self.sampling)


Aggregation = collections.namedtuple(
    "Aggregation",
    ["method", "granularity", "timespan"],
)


class AggregatedTimeSerie(TimeSerie):

    _AGG_METHOD_PCT_RE = re.compile(r"([1-9][0-9]?)pct")

    PADDED_SERIAL_LEN = struct.calcsize("<?d")
    COMPRESSED_SERIAL_LEN = struct.calcsize("<Hd")
    COMPRESSED_TIMESPAMP_LEN = struct.calcsize("<H")

    def __init__(self, aggregation, ts=None):
        """A time serie that is downsampled.

        Used to represent the downsampled timeserie for a single
        granularity/aggregation-function pair stored for a metric.

        """
        super(AggregatedTimeSerie, self).__init__(ts)
        self.aggregation = aggregation

    def resample(self, sampling):
        return AggregatedTimeSerie.from_grouped_serie(
            self.group_serie(sampling),
            Aggregation(self.aggregation.method, sampling,
                        self.aggregation.timespan))

    @classmethod
    def from_data(cls, aggregation, timestamps, values):
        return cls(aggregation=aggregation,
                   ts=make_timeseries(timestamps, values))

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

    def truncate(self, oldest_point=None):
        """Truncate the time series up to oldest_point excluded.

        :param oldest_point: Oldest point to keep from, this excluded.
                             Default is the aggregation timespan.
        :type oldest_point: numpy.datetime64 or numpy.timedelta64
        :return: The oldest point that could have been kept.
        """
        last = self.last
        if last is None:
            return
        if oldest_point is None:
            oldest_point = self.aggregation.timespan
            if oldest_point is None:
                return
        if isinstance(oldest_point, numpy.timedelta64):
            oldest_point = last - oldest_point
        index = numpy.searchsorted(self.ts['timestamps'], oldest_point,
                                   side='right')
        self.ts = self.ts[index:]
        return oldest_point

    def split(self):
        # NOTE(sileht): We previously use groupby with
        # SplitKey.from_timestamp_and_sampling, but
        # this is slow because pandas can do that on any kind DataFrame
        # but we have ordered timestamps, so don't need
        # to iter the whole series.
        freq = self.aggregation.granularity * SplitKey.POINTS_PER_SPLIT
        keys, counts = numpy.unique(
            round_timestamp(self.timestamps, freq),
            return_counts=True)
        start = 0
        for key, count in six.moves.zip(keys, counts):
            end = start + count
            yield (SplitKey(key, self.aggregation.granularity),
                   AggregatedTimeSerie(self.aggregation, self[start:end]))
            start = end

    @classmethod
    def from_timeseries(cls, timeseries, aggregation):
        # NOTE(gordc): Indices must be unique across all timeseries. Also,
        # timeseries should be a list that is ordered within list and series.
        if timeseries:
            ts = numpy.concatenate([ts.ts for ts in timeseries])
        else:
            ts = None
        return cls(aggregation=aggregation, ts=ts)

    @classmethod
    def from_grouped_serie(cls, grouped_serie, aggregation):
        if aggregation.method.startswith("rate:"):
            grouped_serie = grouped_serie.derived()
            aggregation_method_name = aggregation.method[5:]
        else:
            aggregation_method_name = aggregation.method
        agg_name, q = cls._get_agg_method(aggregation_method_name)
        return cls(aggregation,
                   ts=cls._resample_grouped(grouped_serie, agg_name, q))

    def __eq__(self, other):
        return (isinstance(other, AggregatedTimeSerie)
                and super(AggregatedTimeSerie, self).__eq__(other)
                and self.aggregation == other.aggregation)

    def __repr__(self):
        return "<%s 0x%x granularity=%s agg_method=%s>" % (
            self.__class__.__name__,
            id(self),
            self.aggregation.granularity,
            self.aggregation.method,
        )

    @staticmethod
    def is_compressed(serialized_data):
        """Check whatever the data was serialized with compression."""
        return six.indexbytes(serialized_data, 0) == ord("c")

    @classmethod
    def unserialize(cls, data, key, aggregation):
        """Unserialize an aggregated timeserie.

        :param data: Raw data buffer.
        :param key: A :class:`SplitKey` key.
        :param aggregation: The Aggregation object of this timeseries.
        """
        x, y = [], []

        if data:
            if cls.is_compressed(data):
                # Compressed format
                uncompressed = lz4.block.decompress(
                    memoryview(data)[1:].tobytes())
                nb_points = len(uncompressed) // cls.COMPRESSED_SERIAL_LEN

                try:
                    y = numpy.frombuffer(uncompressed, dtype='<H',
                                         count=nb_points)
                    x = numpy.frombuffer(
                        uncompressed, dtype='<d',
                        offset=nb_points*cls.COMPRESSED_TIMESPAMP_LEN)
                except ValueError:
                    raise InvalidData()
                y = numpy.cumsum(y * key.sampling) + key.key
            else:
                # Padded format
                try:
                    everything = numpy.frombuffer(data, dtype=[('b', '<?'),
                                                               ('v', '<d')])
                except ValueError:
                    raise InvalidData()
                index = numpy.nonzero(everything['b'])[0]
                y = index * key.sampling + key.key
                x = everything['v'][index]

        return cls.from_data(aggregation, y, x)

    def get_split_key(self, timestamp=None):
        """Return the split key for a particular timestamp.

        :param timestamp: If None, the first timestamp of the timeseries
                          is used.
        :return: A SplitKey object or None if the timeseries is empty.
        """
        if timestamp is None:
            timestamp = self.first
            if timestamp is None:
                return
        return SplitKey.from_timestamp_and_sampling(
            timestamp, self.aggregation.granularity)

    def serialize(self, start, compressed=True):
        """Serialize an aggregated timeserie.

        The serialization starts with a byte that indicate the serialization
        format: 'c' for compressed format, '\x00' or '\x01' for uncompressed
        format. Both format can be unserialized using the `unserialize` method.

        The offset returned indicates at which offset the data should be
        written from. In the case of compressed data, this is always 0.

        :param start: SplitKey to start serialization at.
        :param compressed: Serialize in a compressed format.
        :return: a tuple of (offset, data)

        """
        offset_div = self.aggregation.granularity
        # calculate how many seconds from start the series runs until and
        # initialize list to store alternating delimiter, float entries
        if compressed:
            # NOTE(jd) Use a double delta encoding for timestamps
            timestamps = numpy.empty(self.timestamps.size, dtype='<H')
            timestamps[0] = (self.first - start.key) / offset_div
            timestamps[1:] = numpy.diff(self.timestamps) / offset_div
            payload = (timestamps.tobytes() + self.values.tobytes())
            return None, b"c" + self._compress(payload)
        # NOTE(gordc): this binary serializes series based on the split
        # time. the format is 1B True/False flag which denotes whether
        # subsequent 8B is a real float or zero padding. every 9B
        # represents one second from start time. this is intended to be run
        # on data already split. ie. False,0,True,0 serialization means
        # start datapoint is padding, and 1s after start time, the
        # aggregate value is 0. calculate how many seconds from start the
        # series runs until and initialize list to store alternating
        # delimiter, float entries
        first = self.first  # NOTE(jd) needed because faster
        e_offset = int((self.last - first) / offset_div) + 1

        locs = numpy.zeros(self.timestamps.size, dtype=numpy.int)
        locs[1:] = numpy.cumsum(numpy.diff(self.timestamps)) / offset_div

        # Fill everything with zero and set
        serial = numpy.zeros((e_offset,), dtype=[('b', '<?'), ('v', '<d')])
        serial['b'][locs] = numpy.ones_like(self.values, dtype='<?')
        serial['v'][locs] = self.values

        offset = int((first - start.key) / offset_div) * self.PADDED_SERIAL_LEN
        return offset, serial.tobytes()

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
            from_ = round_timestamp(from_timestamp,
                                    self.aggregation.granularity)
        return self.__class__(self.aggregation, ts=self[from_:to_timestamp])

    @classmethod
    def benchmark(cls):
        """Run a speed benchmark!"""
        points = SplitKey.POINTS_PER_SPLIT
        sampling = numpy.timedelta64(5, 's')
        resample = numpy.timedelta64(35, 's')

        now = numpy.datetime64("2015-04-03 23:11")
        timestamps = numpy.sort(numpy.array(
            [now + i * sampling
             for i in six.moves.range(points)]))

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
                ("Sin(x)", list(map(math.sin, six.moves.range(points)))),
                ("random ", [random.random()
                             for x in six.moves.range(points)]),
        ]:
            print(title)
            serialize_times = 50
            aggregation = Aggregation("mean", sampling, None)
            ts = cls.from_data(aggregation, timestamps, values)
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
                cls.unserialize(s, key, 'mean')
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
                cls.unserialize(s, key, 'mean')
            t1 = time.time()
            print("  Uncompression speed: %.2f MB/s"
                  % (((points * 2 * 8)
                      / ((t1 - t0) / serialize_times)) / (1024.0 * 1024.0)))

            def per_sec(t1, t0):
                return 1 / ((t1 - t0) / serialize_times)

            t0 = time.time()
            for i in six.moves.range(serialize_times):
                list(ts.split())
            t1 = time.time()
            print("  split() speed: %.2f Hz" % per_sec(t1, t0))

            # NOTE(sileht): propose a new series with half overload timestamps
            pts = ts.ts.copy()
            tsbis = cls(ts=pts, aggregation=aggregation)
            tsbis.ts['timestamps'] = (
                tsbis.timestamps - numpy.timedelta64(
                    sampling * points / 2, 's')
            )

            t0 = time.time()
            for i in six.moves.range(serialize_times):
                ts.merge(tsbis)
            t1 = time.time()
            print("  merge() speed %.2f Hz" % per_sec(t1, t0))

            for agg in ['mean', 'sum', 'max', 'min', 'std', 'median', 'first',
                        'last', 'count', '5pct', '90pct']:
                serialize_times = 3 if agg.endswith('pct') else 10
                ts = cls(ts=pts, aggregation=aggregation)
                t0 = time.time()
                for i in six.moves.range(serialize_times):
                    ts.resample(resample)
                t1 = time.time()
                print("  resample(%s) speed: %.2f Hz"
                      % (agg, per_sec(t1, t0)))


if __name__ == '__main__':
    import sys
    args = sys.argv[1:]
    if not args or "--boundtimeserie" in args:
        BoundTimeSerie.benchmark()
    if not args or "--aggregatedtimeserie" in args:
        AggregatedTimeSerie.benchmark()
