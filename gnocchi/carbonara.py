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


class TimeSerie(object):

    def __init__(self, timestamps, values,
                 max_size=None,
                 sampling=None, aggregation_method='mean'):
        self.aggregation_method = aggregation_method
        self.sampling = pandas.tseries.frequencies.to_offset(sampling)
        self.max_size = max_size
        self.ts = pandas.Series(values, timestamps)
        self._resample()
        self._truncate()

    def __eq__(self, other):
        return (self.ts.all() == other.ts.all()
                and self.max_size == other.max_size
                and self.sampling == other.sampling
                and self.aggregation_method == other.aggregation_method)

    def __getitem__(self, key):
        return self.ts[key]

    def __setitem__(self, key, value):
        self.ts[key] = value

    def __len__(self):
        return len(self.ts)

    @classmethod
    def from_dict(cls, d):
        """Build a time series from a dict.
        The dict format must be datetime as key and values as values.

        :param d: The dict.
        :returns: A TimeSerie object
        """
        values_and_timestamps = tuple(
            zip(*dict(
                (pandas.Timestamp(k), v)
                for k, v in six.iteritems(d['values'])).items()))
        if values_and_timestamps:
            values, timestamps = values_and_timestamps
        else:
            values, timestamps = (), ()
        return cls(values, timestamps,
                   max_size=d.get('max_size'),
                   sampling=d.get('sampling'),
                   aggregation_method=d.get('aggregation_method', 'mean'))

    def to_dict(self):
        return {
            'aggregation_method': self.aggregation_method,
            'max_size': self.max_size,
            'sampling': (six.text_type(self.sampling.n)
                         + self.sampling.rule_code),
            'values': dict((six.text_type(k), float(v))
                           for k, v
                           in six.iteritems(self.ts[~self.ts.isnull()])),
        }

    def _truncate(self):
        if self.max_size is not None:
            self.ts = self.ts[~self.ts.isnull()][-self.max_size:]

    def _resample(self):
        if self.sampling:
            self.ts = self.ts.resample(self.sampling,
                                       how=self.aggregation_method)

    def update(self, ts):
        self.ts = ts.ts.combine_first(self.ts)
        self._resample()
        self._truncate()

    def serialize(self):
        return msgpack.dumps(self.to_dict())

    @classmethod
    def unserialize(cls, data):
        return cls.from_dict(msgpack.loads(data, encoding='utf-8'))


class TimeSerieCollection(object):

    def __init__(self, timeseries):
        if timeseries:
            agg = timeseries[0].aggregation_method
        for ts in timeseries[1:]:
            if ts.aggregation_method != agg:
                raise ValueError(
                    "All time series must use the same aggregation method")
        self.timeseries = sorted(timeseries,
                                 key=operator.attrgetter('sampling'))

    def fetch(self, from_timestamp=None, to_timestamp=None):
        result = pandas.Series()
        fts = pandas.Timestamp(from_timestamp,
                               unit='s') if from_timestamp else None
        tts = pandas.Timestamp(to_timestamp,
                               unit='s') if to_timestamp else None
        for ts in self.timeseries:
            result = result.combine_first(ts[fts:tts])
        return dict(result)

    def __eq__(self, other):
        return self.timeseries == other.timeseries

    def serialize(self):
        return msgpack.dumps([ts.to_dict() for ts in self.timeseries])

    def __setitem__(self, timestamp, value):
        timestamp = pandas.Timestamp(timestamp, unit='s')
        for ts in self.timeseries:
            ts[timestamp] = value

    def __getitem__(self, key):
        if isinstance(key, slice):
            if key.step:
                raise ValueError("Unable to use step on getitem %s",
                                 self.__class__.__name__)
            return self.fetch(key.start, key.stop)
        return self.fetch(key)

    @classmethod
    def unserialize(cls, data):
        return cls([TimeSerie.from_dict(ts)
                    for ts in msgpack.loads(data, encoding='utf-8')])
